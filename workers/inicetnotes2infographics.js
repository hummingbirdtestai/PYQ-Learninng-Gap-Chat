"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ============================================================
// DATABASE CONFIGURATION
// ============================================================

const TABLE = "inicet_pyt_source";
const INPUT_COL = "mcq_json";
const OUTPUT_COL = "infographics";
const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

// ============================================================
// ENVIRONMENT HELPERS
// ============================================================

function integerEnv(name, fallback, min, max) {
  const value = Number.parseInt(
    process.env[name] || String(fallback),
    10
  );

  if (
    !Number.isInteger(value) ||
    value < min ||
    value > max
  ) {
    throw new Error(
      `${name} must be an integer between ${min} and ${max}`
    );
  }

  return value;
}

// ============================================================
// WORKER VARIABLES
// ============================================================

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

// ============================================================
// PASTE YOUR PROMPT BELOW
// ============================================================

/*
Paste your complete prompt between the comment markers.

Do not paste the prompt outside the comment.

The prompt must not contain the closing comment characters:
* immediately followed by /
*/

const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
PASTE YOUR COMPLETE INICET INFOGRAPHICS PROMPT HERE
  */ };

  const source = promptContainer.toString();
  const start = source.indexOf("/*") + 2;
  const end = source.lastIndexOf("*/");

  return source.slice(start, end).trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE INICET INFOGRAPHICS PROMPT HERE"
  )
) {
  throw new Error(
    "Paste your complete prompt inside SYSTEM_PROMPT before starting the worker"
  );
}

// This contract is appended automatically.
// You do not need to add these technical formatting rules to your prompt.

const OUTPUT_CONTRACT = `
OUTPUT FORMAT:

Begin with:

# {{TOPIC}}

Create exactly 20 entries numbered from 1 to 20.

Use this exact structure for every entry:

### 1. Short clinical concept title

**BUZZWORDS:** clue + clue + clue + clue

**Q:** Clinical question

**A:** Direct answer

**LOCK:** Concise high-yield explanation, discriminator, mechanism, or examiner trap

Continue sequentially through:

### 20. Short clinical concept title

MANDATORY RULES:

- Create exactly 20 entries.
- Number the entries from 1 through 20.
- Every entry must contain BUZZWORDS, Q, A, and LOCK.
- Use the supplied MCQ JSON as the factual source.
- Do not copy the MCQs verbatim.
- Convert them into rapid-revision clinical buzzword chains.
- Return only finished Markdown.
- Do not return JSON.
- Do not use HTML.
- Do not use Mermaid.
- Do not use Markdown tables.
- Do not wrap the output in a Markdown code fence.
- Do not add commentary before or after the finished content.
`;

// ============================================================
// GENERAL HELPERS
// ============================================================

const sleep = (milliseconds) =>
  new Promise((resolve) =>
    setTimeout(resolve, milliseconds)
  );

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

function isValidationError(error) {
  return /empty output|non-empty string|code fence|Markdown heading|numbered entries|exactly 20|identifiable answers|BUZZWORDS|LOCK|prohibited|unexpectedly short|mcq_json|JSON output/i.test(
    errorText(error)
  );
}

function requiredText(value, label) {
  const text = String(value ?? "").trim();

  if (!text) {
    throw new Error(
      `${label} must be a non-empty string`
    );
  }

  return text;
}

// ============================================================
// SERIALIZE MCQ JSON
// ============================================================

function serializeSource(value) {
  if (
    value === null ||
    value === undefined
  ) {
    throw new Error(
      `${INPUT_COL} is missing`
    );
  }

  if (typeof value === "string") {
    const text = value.trim();

    if (!text) {
      throw new Error(
        `${INPUT_COL} is empty`
      );
    }

    try {
      return JSON.stringify(
        JSON.parse(text),
        null,
        2
      );
    } catch {
      return text;
    }
  }

  return JSON.stringify(
    value,
    null,
    2
  );
}

// ============================================================
// MODEL INPUT
// ============================================================

function buildInput(row) {
  return [
    `Topic: ${requiredText(row.topic, "Topic")}`,
    `Subject: ${requiredText(row.subject, "Subject")}`,
    "",
    "SOURCE MCQ JSON:",
    serializeSource(row[INPUT_COL])
  ].join("\n");
}

// ============================================================
// EXTRACT OPENAI RESPONSE
// ============================================================

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

  const text = pieces
    .join("\n")
    .trim();

  if (!text) {
    throw new Error(
      "OpenAI returned empty output"
    );
  }

  return text;
}

// ============================================================
// NORMALIZE MODEL OUTPUT
// ============================================================

function removeOuterCodeFence(text) {
  let output = text.trim();

  output = output.replace(
    /^```(?:markdown|md|text|json)?[ \t]*\r?\n/i,
    ""
  );

  output = output.replace(
    /\r?\n```[ \t]*$/i,
    ""
  );

  return output.trim();
}

function normalizeInfographics(raw, topic) {
  let markdown = requiredText(
    raw,
    "Generated infographics"
  );

  markdown = removeOuterCodeFence(
    markdown
  );

  // Remove common unwanted introductory text.
  markdown = markdown.replace(
    /^(?:Here (?:is|are)|Below (?:is|are))[^:\n]*:\s*/i,
    ""
  );

  const safeTopic = requiredText(
    topic,
    "Topic"
  );

  // Add a main title if the model omitted it.
  if (!/^#\s+\S+/m.test(markdown)) {
    markdown =
      `# ${safeTopic}\n\n${markdown}`;
  }

  return markdown.trim();
}

// ============================================================
// OUTPUT VALIDATION
// ============================================================

function findNumberedItems(markdown) {
  const numbers = new Set();

  const patterns = [
    // ### 1. Title
    /^\s*#{1,6}\s*\*{0,2}(\d{1,2})[.):\-]\s*/gm,

    // 1. Title
    /^\s*(\d{1,2})[.)]\s+/gm,

    // **1. Title**
    /^\s*\*{1,2}(\d{1,2})[.)]\s+/gm,

    // Question 1 / Concept 1 / Entry 1 / Case 1 / Q1
    /^\s*(?:#{1,6}\s*)?\*{0,2}(?:concept|question|entry|case|pattern|q)\s*[-:#.]?\s*(\d{1,2})\b/gim,

    // JSON-style numbering
    /"(?:number|concept_number|question_number|id)"\s*:\s*(\d{1,2})\b/gim
  ];

  for (const pattern of patterns) {
    let match;

    while (
      (match = pattern.exec(markdown)) !== null
    ) {
      const number = Number.parseInt(
        match[1],
        10
      );

      if (
        number >= 1 &&
        number <= 20
      ) {
        numbers.add(number);
      }
    }
  }

  return numbers;
}

function countLabel(markdown, label) {
  const expression = new RegExp(
    `^\\s*(?:[-*]\\s*)?(?:#{1,6}\\s*)?\\*{0,2}${label}\\*{0,2}\\s*(?:→|:|-|—)`,
    "gim"
  );

  return (
    markdown.match(expression)?.length ||
    0
  );
}

function validateInfographics(raw) {
  const markdown = requiredText(
    raw,
    "Generated infographics"
  );

  if (!/^#\s+\S+/m.test(markdown)) {
    throw new Error(
      "Output lacks a main Markdown heading"
    );
  }

  if (
    /^```/m.test(markdown) &&
    /```(?:\s*)$/m.test(markdown)
  ) {
    throw new Error(
      "Output still contains a Markdown code fence"
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
    ) ||
    /^\s*(?:flowchart|graph|sequenceDiagram)\s+(?:TD|TB|LR|RL)\b/im.test(
      markdown
    )
  ) {
    throw new Error(
      "Output contains prohibited Mermaid"
    );
  }

  const numberedItems =
    findNumberedItems(markdown);

  const missingNumbers = [];

  for (
    let number = 1;
    number <= 20;
    number += 1
  ) {
    if (!numberedItems.has(number)) {
      missingNumbers.push(number);
    }
  }

  if (missingNumbers.length > 0) {
    throw new Error(
      `Output does not contain all 20 numbered entries. Missing: ${missingNumbers.join(", ")}`
    );
  }

  const answerCount =
    countLabel(markdown, "(?:A|ANSWER)");

  if (answerCount < 20) {
    throw new Error(
      `Output contains only ${answerCount} identifiable answers; expected 20`
    );
  }

  const questionCount =
    countLabel(markdown, "(?:Q|QUESTION)");

  if (questionCount < 20) {
    throw new Error(
      `Output contains only ${questionCount} identifiable questions; expected 20`
    );
  }

  const buzzwordCount =
    countLabel(
      markdown,
      "(?:BUZZWORDS?|CLINICAL\\s+CLUES?)"
    );

  if (buzzwordCount < 20) {
    throw new Error(
      `Output contains only ${buzzwordCount} identifiable BUZZWORDS sections; expected 20`
    );
  }

  const lockCount =
    countLabel(
      markdown,
      "(?:LOCK|MEMORY\\s+LOCK|EXAMINER\\s+TRAP)"
    );

  if (lockCount < 20) {
    throw new Error(
      `Output contains only ${lockCount} identifiable LOCK sections; expected 20`
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
    lines:
      markdown.split(/\r?\n/).length,
    numberedItems: numberedItems.size,
    answers: answerCount,
    questions: questionCount,
    buzzwords: buzzwordCount,
    locks: lockCount
  };
}

// ============================================================
// GENERATE INFOGRAPHICS
// ============================================================

async function generateInfographics(row) {
  let lastError;

  for (
    let attempt = 0;
    attempt <= API_RETRIES;
    attempt += 1
  ) {
    try {
      const instructions = [
        SYSTEM_PROMPT,
        OUTPUT_CONTRACT.replace(
          "{{TOPIC}}",
          requiredText(
            row.topic,
            "Topic"
          )
        )
      ].join("\n\n");

      const response =
        await openai.responses.create({
          model: MODEL,
          instructions,
          input: buildInput(row),
          max_output_tokens:
            MAX_OUTPUT_TOKENS
        });

      const rawOutput =
        extractText(response);

      const normalizedOutput =
        normalizeInfographics(
          rawOutput,
          row.topic
        );

      return validateInfographics(
        normalizedOutput
      );
    } catch (error) {
      lastError = error;

      if (isCreditError(error)) {
        throw error;
      }

      if (
        attempt === API_RETRIES ||
        (
          !isRetryable(error) &&
          !isValidationError(error)
        )
      ) {
        break;
      }

      const delay =
        1000 * (2 ** attempt) +
        Math.floor(
          Math.random() * 400
        );

      console.warn(
        `Retry ${attempt + 1}/${API_RETRIES} after ${delay} ms: ${errorText(error)}`
      );

      await sleep(delay);
    }
  }

  throw (
    lastError ||
    new Error(
      "Infographics generation failed"
    )
  );
}

// ============================================================
// RELEASE EXPIRED LOCKS
// ============================================================

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

// ============================================================
// LOCK ONE ROW
// ============================================================

async function lockRow(candidate) {
  const lockedAt =
    new Date().toISOString();

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

// ============================================================
// CLAIM PENDING ROWS
// ============================================================

async function claimRows(limit) {
  await releaseExpiredLocks();

  const { data, error } = await supabase
    .from(TABLE)
    .select(
      "id,serial_number"
    )
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

  const results =
    await Promise.allSettled(
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

    if (
      result.status === "rejected"
    ) {
      console.error(
        "Row-lock error:",
        errorText(result.reason)
      );
    }
  }

  return rows;
}

// ============================================================
// SAVE SUCCESSFUL OUTPUT
// ============================================================

async function saveSuccess(
  row,
  markdown
) {
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

// ============================================================
// RELEASE ONE ROW LOCK
// ============================================================

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

// ============================================================
// PROCESS ONE ROW
// ============================================================

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
      `Completed | ${row.subject} | ${row.serial_number} | ${row.topic} | entries=${result.numberedItems} | questions=${result.questions} | answers=${result.answers} | buzzwords=${result.buzzwords} | locks=${result.locks} | characters=${result.characters}`
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

// ============================================================
// PROCESS ROWS WITH CONTROLLED CONCURRENCY
// ============================================================

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
        await processRow(
          rows[index]
        );

      if (
        result.creditExhausted
      ) {
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

// ============================================================
// MAIN WORKER LOOP
// ============================================================

async function main() {
  console.log(
    `INICET MCQ -> INFOGRAPHICS WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `Model=${MODEL} | Table=${TABLE} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY} | MaxOutputTokens=${MAX_OUTPUT_TOKENS}`
  );

  while (true) {
    try {
      const rows =
        await claimRows(
          PICKUP_LIMIT
        );

      if (!rows.length) {
        await sleep(
          LOOP_SLEEP_MS
        );

        continue;
      }

      console.log(
        `Claimed ${rows.length} pending infographic row(s)`
      );

      const result =
        await processBatch(rows);

      if (
        result.creditExhausted
      ) {
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

// ============================================================
// START WORKER
// ============================================================

main().catch((error) => {
  console.error(
    "Fatal INICET infographics worker error:",
    error
  );

  process.exit(1);
});
