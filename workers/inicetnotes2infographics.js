"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

const TABLE = "inicet_pyt_source";
const INPUT_COL = "notes_json";
const OUTPUT_COL = "infographics";
const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

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
  process.env.INICET_INFOGRAPHICS_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "INICET_INFOGRAPHICS_LIMIT",
  50,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "INICET_INFOGRAPHICS_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "INICET_INFOGRAPHICS_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "INICET_INFOGRAPHICS_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "INICET_INFOGRAPHICS_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = integerEnv(
  "INICET_INFOGRAPHICS_MAX_OUTPUT_TOKENS",
  16000,
  1000,
  50000
);

const WORKER_ID =
  process.env.INICET_INFOGRAPHICS_WORKER_ID ||
  `inicet-infographics-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

/*
Paste the complete prompt between the comment markers below.

Markdown backticks and JSON examples are safe inside this section.

Important:
The prompt must not contain the closing comment characters:
* followed immediately by /
*/
const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
PASTE YOUR COMPLETE INICET INFOGRAPHICS SYSTEM PROMPT HERE
  */ };

  const source = promptContainer.toString();
  const start = source.indexOf("/*") + 2;
  const end = source.lastIndexOf("*/");

  return source.slice(start, end).trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE INICET INFOGRAPHICS SYSTEM PROMPT HERE"
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

function requiredText(value, label) {
  const text = String(value ?? "").trim();

  if (!text) {
    throw new Error(`${label} must be a non-empty string`);
  }

  return text;
}

function serializeNotes(value) {
  if (value === null || value === undefined) {
    throw new Error("notes_json is missing");
  }

  if (typeof value === "string") {
    const text = value.trim();

    if (!text) {
      throw new Error("notes_json is empty");
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
    `SUBJECT: ${requiredText(row.subject, "Subject")}`,
    `SERIAL NUMBER: ${row.serial_number}`,
    `NUMBER OF TIMES ASKED: ${row.number_of_times_asked}`,
    "",
    "SOURCE INICET NOTES JSON:",
    serializeNotes(row[INPUT_COL]),
    "",
    "Create the finished INICET infographic revision content using only this topic and its supplied notes.",
    "Create exactly 20 unique numbered clinical buzzword-chain questions.",
    "Number the entries clearly from 1 through 20.",
    "Each entry must contain a clinical buzzword chain, a question, a direct answer, and a concise high-yield memory lock or explanation.",
    "Return only the finished React Native-friendly Markdown.",
    "Do not include explanations before or after the finished content.",
    "Do not wrap the output in a Markdown code fence."
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

function findNumberedItems(markdown) {
  const numbers = new Set();

  const patterns = [
    /^\s*(?:#{1,6}\s*)?(\d{1,2})[.)]\s+/gm,
    /^\s*(?:#{1,6}\s*)?\*\*(\d{1,2})[.)]\s+/gm,
    /^\s*(?:#{1,6}\s*)?Q(?:UESTION)?\s*(\d{1,2})[.):-]?\s*/gim
  ];

  for (const pattern of patterns) {
    let match;

    while ((match = pattern.exec(markdown)) !== null) {
      const number = Number.parseInt(match[1], 10);

      if (number >= 1 && number <= 20) {
        numbers.add(number);
      }
    }
  }

  return numbers;
}

function countAnswerLines(markdown) {
  const matches = markdown.match(
    /^\s*(?:[-*]\s*)?(?:\*\*)?(?:A|ANSWER)(?:\*\*)?\s*(?:→|:|-|—)/gim
  );

  return matches?.length || 0;
}

function validateInfographics(raw) {
  const markdown = requiredText(
    raw,
    "Generated infographics"
  );

  if (
    markdown.startsWith("```") ||
    markdown.endsWith("```")
  ) {
    throw new Error(
      "Output is wrapped in a Markdown code fence"
    );
  }

  if (!/^#\s+\S+/m.test(markdown)) {
    throw new Error(
      "Output lacks a main Markdown heading"
    );
  }

  if (
    /<\s*\/?\s*(div|span|table|br|style|script)\b/i.test(
      markdown
    )
  ) {
    throw new Error(
      "Output contains prohibited HTML"
    );
  }

  if (
    /```(?:mermaid)?[\s\S]*?(?:flowchart|graph|sequenceDiagram)/i.test(
      markdown
    )
  ) {
    throw new Error(
      "Output contains prohibited Mermaid"
    );
  }

  const numberedItems = findNumberedItems(markdown);
  const missingNumbers = [];

  for (let number = 1; number <= 20; number += 1) {
    if (!numberedItems.has(number)) {
      missingNumbers.push(number);
    }
  }

  if (missingNumbers.length > 0) {
    throw new Error(
      `Output does not contain all 20 numbered entries. Missing: ${missingNumbers.join(", ")}`
    );
  }

  const answerCount = countAnswerLines(markdown);

  if (answerCount < 20) {
    throw new Error(
      `Output contains only ${answerCount} identifiable answers; expected at least 20`
    );
  }

  if (markdown.length < 2000) {
    throw new Error(
      "Output is unexpectedly short"
    );
  }

  return {
    markdown,
    characters: markdown.length,
    lines: markdown.split(/\r?\n/).length,
    numberedItems: numberedItems.size,
    answers: answerCount
  };
}

async function generateInfographics(row) {
  let lastError;

  for (
    let attempt = 0;
    attempt <= API_RETRIES;
    attempt += 1
  ) {
    try {
      const response =
        await openai.responses.create({
          model: MODEL,
          instructions: SYSTEM_PROMPT,
          input: buildInput(row),
          max_output_tokens: MAX_OUTPUT_TOKENS
        });

      return validateInfographics(
        extractText(response)
      );
    } catch (error) {
      lastError = error;

      if (isCreditError(error)) {
        throw error;
      }

      const validationFailure =
        /empty output|non-empty string|code fence|Markdown heading|numbered entries|identifiable answers|prohibited|unexpectedly short|notes_json/i.test(
          errorText(error)
        );

      if (
        attempt === API_RETRIES ||
        (!isRetryable(error) &&
          !validationFailure)
      ) {
        break;
      }

      const delay =
        1000 * (2 ** attempt) +
        Math.floor(Math.random() * 400);

      console.warn(
        `Retry ${attempt + 1}/${API_RETRIES} after ${delay} ms: ${errorText(error)}`
      );

      await sleep(delay);
    }
  }

  throw (
    lastError ||
    new Error("Infographics generation failed")
  );
}

async function releaseExpiredLocks() {
  const cutoff = new Date(
    Date.now() -
      LOCK_TTL_MIN * 60 * 1000
  ).toISOString();

  const { error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null
    })
    .eq(LOCK_COL, true)
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .lt(LOCK_AT_COL, cutoff);

  if (error) {
    throw new Error(
      `Failed to release expired infographic locks: ${error.message}`
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
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .select(
      [
        "id",
        "subject",
        "serial_number",
        "topic",
        "number_of_times_asked",
        INPUT_COL,
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
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .order("serial_number", {
      ascending: true
    })
    .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending infographic rows: ${error.message}`
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

async function saveSuccess(row, markdown) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: markdown,
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null
    })
    .eq("id", row.id)
    .eq(LOCK_COL, true)
    .eq(
      LOCK_AT_COL,
      row[LOCK_AT_COL]
    )
    .is(OUTPUT_COL, null)
    .select("id");

  if (error) {
    throw new Error(
      `Failed to save infographics: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or infographics already exist"
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
    .eq(
      LOCK_AT_COL,
      row[LOCK_AT_COL]
    )
    .is(OUTPUT_COL, null);

  if (error) {
    console.error(
      `Failed to release lock ${row.id}: ${error.message}`
    );
  }
}

async function processRow(row) {
  console.log(
    `Generating INICET infographics | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateInfographics(row);

    await saveSuccess(
      row,
      result.markdown
    );

    console.log(
      `Completed | ${row.subject} | ${row.serial_number} | ${row.topic} | entries=${result.numberedItems} | answers=${result.answers} | lines=${result.lines} | characters=${result.characters}`
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

      const result =
        await processRow(rows[index]);

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
      rows
        .slice(next)
        .map(releaseLock)
    );
  }

  return {
    creditExhausted
  };
}

async function main() {
  console.log(
    `INICET NOTES -> INFOGRAPHICS WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `Model=${MODEL} | Table=${TABLE} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY} | MaxOutputTokens=${MAX_OUTPUT_TOKENS}`
  );

  while (true) {
    try {
      const rows =
        await claimRows(PICKUP_LIMIT);

      if (!rows.length) {
        await sleep(LOOP_SLEEP_MS);
        continue;
      }

      console.log(
        `Claimed ${rows.length} pending infographic row(s)`
      );

      const result =
        await processBatch(rows);

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
        Math.max(
          LOOP_SLEEP_MS,
          2000
        )
      );
    }
  }
}

main().catch((error) => {
  console.error(
    "Fatal INICET infographics worker error:",
    error
  );

  process.exit(1);
});
