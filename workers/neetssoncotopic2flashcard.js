"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

const TABLE = "neet_ss_oncology_pyt_source";
const OUTPUT_COL = "jsonb_output";
const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

const EXPECTED_CARD_COUNT = 20;
const MIN_ANSWER_WORDS = 3;
const MAX_ANSWER_WORDS = 6;

const BANNED_ANSWER_VERBS = new Set([
  "consider",
  "evaluate",
  "investigate",
  "assess",
  "verify",
  "monitor",
  "reassess",
  "ensure",
  "check",
  "rule",
  "arrange",
  "manage"
]);

function integerEnv(name, fallback, min, max) {
  const value = Number.parseInt(
    process.env[name] || String(fallback),
    10
  );

  if (!Number.isInteger(value) || value < min || value > max) {
    throw new Error(
      `${name} must be an integer between ${min} and ${max}`
    );
  }

  return value;
}

const MODEL =
  process.env.NEETSS_ONCOLOGY_FLASHCARD_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "NEETSS_ONCOLOGY_FLASHCARD_LIMIT",
  50,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "NEETSS_ONCOLOGY_FLASHCARD_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "NEETSS_ONCOLOGY_FLASHCARD_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "NEETSS_ONCOLOGY_FLASHCARD_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "NEETSS_ONCOLOGY_FLASHCARD_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = integerEnv(
  "NEETSS_ONCOLOGY_FLASHCARD_MAX_OUTPUT_TOKENS",
  16000,
  2000,
  50000
);

const WORKER_ID =
  process.env.NEETSS_ONCOLOGY_FLASHCARD_WORKER_ID ||
  `neetss-oncology-flashcard-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

/*
PASTE YOUR COMPLETE PROMPT INSIDE THE COMMENT BELOW.

Markdown, JSON examples, quotation marks and backticks are safe here.

IMPORTANT:
The pasted prompt must not contain the comment-closing characters:
* followed immediately by /
*/
const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
PASTE YOUR COMPLETE NEET SS MEDICAL ONCOLOGY FLASHCARD PROMPT HERE
  */ };

  const source = promptContainer.toString();
  const start = source.indexOf("/*") + 2;
  const end = source.lastIndexOf("*/");

  return source.slice(start, end).trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE NEET SS MEDICAL ONCOLOGY FLASHCARD PROMPT HERE"
  )
) {
  throw new Error(
    "Paste the complete inline SYSTEM_PROMPT before starting the worker"
  );
}

const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

function errorText(error) {
  return String(
    error?.message ||
      error?.error?.message ||
      error ||
      "Unknown error"
  );
}

function isCreditError(error) {
  return /no credits remaining|insufficient_quota|billing|credit balance|billing_hard_limit/i.test(
    errorText(error)
  );
}

function isRetryable(error) {
  if (isCreditError(error)) {
    return false;
  }

  const status = Number(
    error?.status ||
      error?.statusCode ||
      error?.response?.status
  );

  return (
    status === 408 ||
    status === 409 ||
    status === 429 ||
    status >= 500 ||
    /timeout|temporar|unavailable|rate limit|ECONNRESET|ETIMEDOUT|socket hang up/i.test(
      errorText(error)
    )
  );
}

function requiredText(value, label, minimumLength = 1) {
  const text = String(value ?? "").trim();

  if (text.length < minimumLength) {
    throw new Error(`${label} must be a non-empty string`);
  }

  return text;
}

function normalizeText(value) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function serializeSubtopic(value) {
  if (value === null || value === undefined) {
    throw new Error("Subtopic/source is missing");
  }

  if (typeof value === "string") {
    const text = value.trim();

    if (!text) {
      throw new Error("Subtopic/source is empty");
    }

    try {
      return JSON.stringify(JSON.parse(text), null, 2);
    } catch {
      return text;
    }
  }

  return JSON.stringify(value, null, 2);
}

function buildInput(row) {
  return [
    `TOPIC: ${requiredText(row.topic, "Topic")}`,
    "",
    "SOURCE / SUBTOPICS:",
    serializeSubtopic(row.subtopic),
    "",
    `Create exactly ${EXPECTED_CARD_COUNT} clinical-decision flashcards.`,
    "Use the supplied topic and subtopics as the content framework.",
    "Each answer must contain exactly 3–6 space-separated words.",
    "Return only the required valid JSON object.",
    "Do not use Markdown code fences.",
    "Do not add commentary before or after the JSON."
  ].join("\n");
}

function extractText(response) {
  if (
    typeof response?.output_text === "string" &&
    response.output_text.trim()
  ) {
    return response.output_text.trim();
  }

  const pieces = [];

  for (const item of response?.output || []) {
    for (const content of item?.content || []) {
      if (
        content?.type === "output_text" &&
        typeof content.text === "string"
      ) {
        pieces.push(content.text);
      }
    }
  }

  const text = pieces.join("\n").trim();

  if (!text) {
    throw new Error("OpenAI returned empty output");
  }

  return text;
}

function removeCodeFence(value) {
  let text = String(value ?? "").trim();

  const match = text.match(
    /^```(?:json)?\s*([\s\S]*?)\s*```$/i
  );

  if (match) {
    text = match[1].trim();
  }

  return text;
}

function parseJsonOutput(rawText) {
  const cleaned = removeCodeFence(rawText);

  try {
    return JSON.parse(cleaned);
  } catch {
    const firstBrace = cleaned.indexOf("{");
    const lastBrace = cleaned.lastIndexOf("}");

    if (
      firstBrace === -1 ||
      lastBrace === -1 ||
      lastBrace <= firstBrace
    ) {
      throw new Error(
        "Output does not contain a JSON object"
      );
    }

    const candidate = cleaned.slice(
      firstBrace,
      lastBrace + 1
    );

    try {
      return JSON.parse(candidate);
    } catch (error) {
      throw new Error(
        `Model returned invalid JSON: ${error.message}`
      );
    }
  }
}

function validateExactKeys(object, expectedKeys, label) {
  if (
    !object ||
    typeof object !== "object" ||
    Array.isArray(object)
  ) {
    throw new Error(`${label} must be an object`);
  }

  const actual = Object.keys(object).sort();
  const expected = [...expectedKeys].sort();

  if (
    JSON.stringify(actual) !==
    JSON.stringify(expected)
  ) {
    throw new Error(
      `${label} must contain exactly: ${expectedKeys.join(", ")}`
    );
  }
}

function countWords(value) {
  return String(value)
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .length;
}

function getFirstWord(value) {
  return String(value)
    .trim()
    .split(/\s+/)[0]
    .toLowerCase()
    .replace(/[^a-z-]/g, "");
}

function validateFlashcards(value, expectedTopic) {
  validateExactKeys(
    value,
    ["topic", "cards"],
    "Flashcard output"
  );

  const outputTopic = requiredText(
    value.topic,
    "Output topic"
  );

  if (
    normalizeText(outputTopic) !==
    normalizeText(expectedTopic)
  ) {
    throw new Error(
      `Output topic mismatch. Expected "${expectedTopic}", received "${outputTopic}"`
    );
  }

  if (!Array.isArray(value.cards)) {
    throw new Error('"cards" must be an array');
  }

  if (value.cards.length !== EXPECTED_CARD_COUNT) {
    throw new Error(
      `Expected exactly ${EXPECTED_CARD_COUNT} cards, received ${value.cards.length}`
    );
  }

  const questions = new Set();
  const answers = new Set();

  value.cards.forEach((card, index) => {
    const cardNumber = index + 1;

    validateExactKeys(
      card,
      ["question", "answer"],
      `Card ${cardNumber}`
    );

    const question = requiredText(
      card.question,
      `Card ${cardNumber} question`,
      30
    );

    const answer = requiredText(
      card.answer,
      `Card ${cardNumber} answer`
    );

    if (!question.endsWith("?")) {
      throw new Error(
        `Card ${cardNumber} question must end with a question mark`
      );
    }

    const answerWords = countWords(answer);

    if (
      answerWords < MIN_ANSWER_WORDS ||
      answerWords > MAX_ANSWER_WORDS
    ) {
      throw new Error(
        `Card ${cardNumber} answer contains ${answerWords} words; required range is ${MIN_ANSWER_WORDS}–${MAX_ANSWER_WORDS}`
      );
    }

    const firstWord = getFirstWord(answer);

    if (BANNED_ANSWER_VERBS.has(firstWord)) {
      throw new Error(
        `Card ${cardNumber} begins with banned answer verb "${firstWord}"`
      );
    }

    const normalizedQuestion = normalizeText(question);
    const normalizedAnswer = normalizeText(answer);

    if (questions.has(normalizedQuestion)) {
      throw new Error(
        `Card ${cardNumber} duplicates another question`
      );
    }

    if (answers.has(normalizedAnswer)) {
      throw new Error(
        `Card ${cardNumber} duplicates another answer`
      );
    }

    questions.add(normalizedQuestion);
    answers.add(normalizedAnswer);

    card.question = question;
    card.answer = answer;
  });

  value.topic = expectedTopic;

  return value;
}

async function generateFlashcards(row) {
  let lastError;

  for (
    let attempt = 0;
    attempt <= API_RETRIES;
    attempt += 1
  ) {
    try {
      const response = await openai.responses.create({
        model: MODEL,
        instructions: SYSTEM_PROMPT,
        input: buildInput(row),
        max_output_tokens: MAX_OUTPUT_TOKENS
      });

      const rawText = extractText(response);
      const parsed = parseJsonOutput(rawText);

      const validated = validateFlashcards(
        parsed,
        requiredText(row.topic, "Topic")
      );

      return {
        json: validated,
        cards: validated.cards.length,
        characters: rawText.length
      };
    } catch (error) {
      lastError = error;

      if (isCreditError(error)) {
        throw error;
      }

      const validationFailure =
        /empty output|JSON|Flashcard|cards|Card|question|answer|topic|word|banned|duplicate|Subtopic|exactly/i.test(
          errorText(error)
        );

      if (
        attempt === API_RETRIES ||
        (!isRetryable(error) && !validationFailure)
      ) {
        break;
      }

      const delay =
        1000 * 2 ** attempt +
        Math.floor(Math.random() * 400);

      console.warn(
        `Retry ${attempt + 1}/${API_RETRIES} after ${delay} ms: ${errorText(error)}`
      );

      await sleep(delay);
    }
  }

  throw (
    lastError ||
    new Error(
      "NEET SS Medical Oncology flashcard generation failed"
    )
  );
}

async function verifySchema() {
  const { error } = await supabase
    .from(TABLE)
    .select(
      [
        "id",
        "topic",
        "subtopic",
        OUTPUT_COL,
        LOCK_COL,
        LOCK_AT_COL
      ].join(",")
    )
    .limit(1);

  if (error) {
    throw new Error(
      `Schema check failed: ${error.message}`
    );
  }
}

async function releaseExpiredLocks() {
  const cutoff = new Date(
    Date.now() - LOCK_TTL_MIN * 60 * 1000
  ).toISOString();

  const { error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null
    })
    .eq(LOCK_COL, true)
    .not("topic", "is", null)
    .not("subtopic", "is", null)
    .is(OUTPUT_COL, null)
    .lt(LOCK_AT_COL, cutoff);

  if (error) {
    throw new Error(
      `Failed to release expired locks: ${error.message}`
    );
  }
}

async function lockRow(candidate) {
  const lockedAt = new Date().toISOString();

  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: true,
      [LOCK_AT_COL]: lockedAt
    })
    .eq("id", candidate.id)
    .eq(LOCK_COL, false)
    .not("topic", "is", null)
    .not("subtopic", "is", null)
    .is(OUTPUT_COL, null)
    .select(
      [
        "id",
        "subject",
        "serial_number",
        "topic",
        "subtopic",
        "number_of_times_asked",
        LOCK_AT_COL
      ].join(",")
    )
    .maybeSingle();

  if (error) {
    throw new Error(
      `Failed to lock row ${candidate.id}: ${error.message}`
    );
  }

  return data || null;
}

async function claimRows(limit) {
  await releaseExpiredLocks();

  const { data, error } = await supabase
    .from(TABLE)
    .select("id,serial_number")
    .eq(LOCK_COL, false)
    .not("topic", "is", null)
    .not("subtopic", "is", null)
    .is(OUTPUT_COL, null)
    .order("serial_number", {
      ascending: true
    })
    .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending oncology rows: ${error.message}`
    );
  }

  if (!data?.length) {
    return [];
  }

  const results = await Promise.allSettled(
    data.map(lockRow)
  );

  const rows = [];

  for (const result of results) {
    if (
      result.status === "fulfilled" &&
      result.value
    ) {
      rows.push(result.value);
    }

    if (result.status === "rejected") {
      console.error(
        "Row-lock error:",
        errorText(result.reason)
      );
    }
  }

  return rows;
}

async function saveSuccess(row, flashcardJson) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: flashcardJson,
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null
    })
    .eq("id", row.id)
    .eq(LOCK_COL, true)
    .eq(LOCK_AT_COL, row[LOCK_AT_COL])
    .is(OUTPUT_COL, null)
    .select("id");

  if (error) {
    throw new Error(
      `Failed to save oncology flashcards: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or jsonb_output already exists"
    );
  }
}

async function releaseLock(row) {
  const { error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null
    })
    .eq("id", row.id)
    .eq(LOCK_COL, true)
    .eq(LOCK_AT_COL, row[LOCK_AT_COL])
    .is(OUTPUT_COL, null);

  if (error) {
    console.error(
      `Failed to release lock ${row.id}: ${error.message}`
    );
  }
}

async function processRow(row) {
  console.log(
    `Generating oncology flashcards | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result = await generateFlashcards(row);

    await saveSuccess(row, result.json);

    console.log(
      `Completed | ${row.subject} | ${row.serial_number} | ${row.topic} | cards=${result.cards} | characters=${result.characters}`
    );

    return {
      creditExhausted: false
    };
  } catch (error) {
    await releaseLock(row);

    if (isCreditError(error)) {
      console.error(
        "OpenAI credits exhausted. Worker will stop safely."
      );

      return {
        creditExhausted: true
      };
    }

    console.error(
      `Failed | ${row.subject} | ${row.serial_number} | ${row.topic}: ${errorText(error)}`
    );

    return {
      creditExhausted: false
    };
  }
}

async function processBatch(rows) {
  let next = 0;
  let creditExhausted = false;

  async function runner() {
    while (
      next < rows.length &&
      !creditExhausted
    ) {
      const index = next;
      next += 1;

      const result = await processRow(rows[index]);

      if (result.creditExhausted) {
        creditExhausted = true;
      }
    }
  }

  await Promise.all(
    Array.from(
      {
        length: Math.min(
          CONCURRENCY,
          rows.length
        )
      },
      runner
    )
  );

  if (creditExhausted) {
    await Promise.allSettled(
      rows.slice(next).map(releaseLock)
    );
  }

  return {
    creditExhausted
  };
}

async function main() {
  await verifySchema();

  console.log(
    `NEET SS MEDICAL ONCOLOGY TOPIC -> FLASHCARDS WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `Model=${MODEL} | Table=${TABLE} | Input=topic+subtopic | Output=${OUTPUT_COL} | Cards=${EXPECTED_CARD_COUNT} | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY} | MaxOutputTokens=${MAX_OUTPUT_TOKENS}`
  );

  while (true) {
    try {
      const rows = await claimRows(PICKUP_LIMIT);

      if (!rows.length) {
        await sleep(LOOP_SLEEP_MS);
        continue;
      }

      console.log(
        `Claimed ${rows.length} pending oncology row(s)`
      );

      const result = await processBatch(rows);

      if (result.creditExhausted) {
        process.exit(1);
      }
    } catch (error) {
      if (isCreditError(error)) {
        process.exit(1);
      }

      console.error(
        "Worker loop error:",
        errorText(error)
      );

      await sleep(
        Math.max(LOOP_SLEEP_MS, 2000)
      );
    }
  }
}

main().catch((error) => {
  console.error(
    "Fatal oncology flashcard worker error:",
    error
  );

  process.exit(1);
});
