require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// DATABASE SETTINGS
// ─────────────────────────────────────────────

const TABLE = "fmge_master_content";

const COURSE_ID =
  "b0c5c874-c0e4-4f38-8269-9c2df57c64b7";

const INPUT_COL = "question";
const OUTPUT_COL = "explanation_new";

const LOCK_COL = "explanation_lock";
const LOCK_AT_COL = "explanation_locked_at";

// ─────────────────────────────────────────────
// ENVIRONMENT SETTINGS
// ─────────────────────────────────────────────

function parseIntegerEnv(name, fallback, min, max) {
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
      `${name} must be between ${min} and ${max}`
    );
  }

  return value;
}

const MODEL =
  process.env.INICET_EXPLANATION_MODEL ||
  "gpt-5-mini";

const PICKUP_LIMIT = parseIntegerEnv(
  "INICET_EXPLANATION_LIMIT",
  20,
  1,
  100
);

const BATCH_SIZE = parseIntegerEnv(
  "INICET_EXPLANATION_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "INICET_EXPLANATION_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "INICET_EXPLANATION_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = parseIntegerEnv(
  "INICET_EXPLANATION_API_RETRIES",
  2,
  0,
  5
);

const WORKER_ID =
  process.env.WORKER_ID ||
  `inicet-explanation-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// Paste your complete prompt exactly between
// the backticks below.
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = String.raw`
PASTE YOUR COMPLETE PROMPT HERE
`.trim();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT ===
    "PASTE YOUR COMPLETE PROMPT HERE"
) {
  throw new Error(
    "Paste the complete prompt inside SYSTEM_PROMPT"
  );
}

// ─────────────────────────────────────────────
// STRUCTURED OUTPUT SCHEMA
// ─────────────────────────────────────────────

const EXPLANATION_JSON_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["topic", "subtopics"],
  properties: {
    topic: {
      type: "string",
      minLength: 1
    },

    subtopics: {
      type: "array",
      minItems: 1,

      items: {
        type: "object",
        additionalProperties: false,
        required: ["subtopic", "notes"],

        properties: {
          subtopic: {
            type: "string",
            minLength: 1
          },

          notes: {
            type: "array",
            minItems: 1,

            items: {
              type: "string",
              minLength: 1
            }
          }
        }
      }
    }
  }
};

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────

const sleep = (milliseconds) =>
  new Promise((resolve) =>
    setTimeout(resolve, milliseconds)
  );

function getErrorText(error) {
  return String(
    error?.message ||
    error?.error?.message ||
    error ||
    "Unknown error"
  );
}

function isCreditExhaustionError(error) {
  return /no credits remaining|insufficient_quota|billing_hard_limit|credit balance/i.test(
    getErrorText(error)
  );
}

function isRetryableError(error) {
  if (isCreditExhaustionError(error)) {
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
    /timeout|temporar|unavailable|rate limit|ECONNRESET|ETIMEDOUT|socket hang up|invalid JSON|empty output/i.test(
      getErrorText(error)
    )
  );
}

function extractResponseText(response) {
  if (
    typeof response?.output_text === "string" &&
    response.output_text.trim()
  ) {
    return response.output_text.trim();
  }

  const collected = [];

  for (const outputItem of response?.output || []) {
    for (const contentItem of outputItem?.content || []) {
      if (
        contentItem?.type === "output_text" &&
        typeof contentItem.text === "string"
      ) {
        collected.push(contentItem.text);
      }
    }
  }

  const text = collected.join("\n").trim();

  if (!text) {
    throw new Error("OpenAI returned empty output");
  }

  return text;
}

function cleanJsonText(rawOutput) {
  return String(rawOutput)
    .trim()
    .replace(/^\s*```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();
}

// ─────────────────────────────────────────────
// BUILD INPUT
// ─────────────────────────────────────────────

function buildUserInput(row) {
  return [
    `SUBJECT: ${row.subject}`,
    `EXACT TOPIC/PYT: ${row.topic}`,
    `CARD SERIAL NUMBER: ${row.serial_number}`,
    row.year_or_recent_question
      ? `EXAM REFERENCE: ${row.year_or_recent_question}`
      : null,
    "",
    "SOURCE QUESTION:",
    row.question
  ]
    .filter((value) => value !== null)
    .join("\n");
}

// ─────────────────────────────────────────────
// VALIDATE GENERATED OUTPUT
// ─────────────────────────────────────────────

function validateGeneratedOutput(rawOutput) {
  const cleaned = cleanJsonText(rawOutput);

  let parsed;

  try {
    parsed = JSON.parse(cleaned);
  } catch (error) {
    throw new Error(
      `Model returned invalid JSON: ${error.message}`
    );
  }

  if (
    !parsed ||
    typeof parsed !== "object" ||
    Array.isArray(parsed)
  ) {
    throw new Error(
      "Output must be one JSON object"
    );
  }

  if (
    typeof parsed.topic !== "string" ||
    !parsed.topic.trim()
  ) {
    throw new Error(
      "Output contains no valid topic"
    );
  }

  if (
    !Array.isArray(parsed.subtopics) ||
    parsed.subtopics.length === 0
  ) {
    throw new Error(
      "Output contains no subtopics"
    );
  }

  const normalizedSubtopics =
    parsed.subtopics.map((group, groupIndex) => {
      if (
        !group ||
        typeof group !== "object" ||
        Array.isArray(group)
      ) {
        throw new Error(
          `Subtopic ${groupIndex + 1} is invalid`
        );
      }

      const subtopic = String(
        group.subtopic || ""
      ).trim();

      if (!subtopic) {
        throw new Error(
          `Subtopic ${groupIndex + 1} has no name`
        );
      }

      if (
        !Array.isArray(group.notes) ||
        group.notes.length === 0
      ) {
        throw new Error(
          `No notes found in subtopic: ${subtopic}`
        );
      }

      const notes = group.notes.map(
        (note, noteIndex) => {
          if (typeof note !== "string") {
            throw new Error(
              `Note ${noteIndex + 1} in "${subtopic}" is not text`
            );
          }

          const normalizedNote = note.trim();

          if (!normalizedNote) {
            throw new Error(
              `Empty note found in "${subtopic}"`
            );
          }

          return normalizedNote;
        }
      );

      return {
        subtopic,
        notes
      };
    });

  const totalNotes = normalizedSubtopics.reduce(
    (total, group) =>
      total + group.notes.length,
    0
  );

  if (totalNotes === 0) {
    throw new Error(
      "Generated output contains no notes"
    );
  }

  return {
    output: {
      topic: parsed.topic.trim(),
      subtopics: normalizedSubtopics
    },

    totalNotes,
    totalSubtopics: normalizedSubtopics.length
  };
}

// ─────────────────────────────────────────────
// GENERATE EXPLANATION
// No max_output_tokens is specified.
// ─────────────────────────────────────────────

async function generateExplanation(row) {
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

          input: buildUserInput(row),

          text: {
            format: {
              type: "json_schema",
              name: "rapid_revision_explanation",
              strict: true,
              schema: EXPLANATION_JSON_SCHEMA
            }
          }
        });

      const rawOutput =
        extractResponseText(response);

      return validateGeneratedOutput(
        rawOutput
      );
    } catch (error) {
      lastError = error;

      if (isCreditExhaustionError(error)) {
        throw error;
      }

      const retriesFinished =
        attempt === API_RETRIES;

      if (
        retriesFinished ||
        !isRetryableError(error)
      ) {
        break;
      }

      const delay =
        1000 * 2 ** attempt +
        Math.floor(Math.random() * 300);

      console.warn(
        `⚠️ API retry ${attempt + 1}/${API_RETRIES} after ${delay} ms`
      );

      await sleep(delay);
    }
  }

  throw lastError;
}

// ─────────────────────────────────────────────
// RELEASE EXPIRED LOCKS
// Restricted to the specified INI-CET course.
// ─────────────────────────────────────────────

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
    .eq("course_id", COURSE_ID)
    .eq(LOCK_COL, true)
    .is(OUTPUT_COL, null)
    .lt(LOCK_AT_COL, cutoff);

  if (error) {
    throw new Error(
      `Failed to release expired locks: ${error.message}`
    );
  }
}

// ─────────────────────────────────────────────
// LOCK ONE ROW
// Lock timestamp acts as lock ownership token.
// ─────────────────────────────────────────────

async function lockOneRow(row) {
  const lockedAt =
    new Date().toISOString();

  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: true,
      [LOCK_AT_COL]: lockedAt
    })
    .eq("id", row.id)
    .eq("course_id", COURSE_ID)
    .eq("active", true)
    .eq(LOCK_COL, false)
    .is(OUTPUT_COL, null)
    .select(
      [
        "id",
        "subject",
        "serial_number",
        "topic",
        "year_or_recent_question",
        INPUT_COL,
        LOCK_AT_COL
      ].join(",")
    )
    .maybeSingle();

  if (error) {
    throw new Error(
      `Failed to lock row ${row.id}: ${error.message}`
    );
  }

  return data || null;
}

// ─────────────────────────────────────────────
// CLAIM ROWS
// ─────────────────────────────────────────────

async function claimRows(limit) {
  await releaseExpiredLocks();

  const { data: availableRows, error } =
    await supabase
      .from(TABLE)
      .select(
        [
          "id",
          "subject",
          "serial_number",
          "topic",
          "year_or_recent_question",
          INPUT_COL
        ].join(",")
      )
      .eq("course_id", COURSE_ID)
      .eq("active", true)
      .not(INPUT_COL, "is", null)
      .neq(INPUT_COL, "")
      .is(OUTPUT_COL, null)
      .eq(LOCK_COL, false)
      .order("subject", {
        ascending: true
      })
      .order("serial_number", {
        ascending: true
      })
      .limit(limit);

  if (error) {
    throw new Error(
      `Failed to select pending rows: ${error.message}`
    );
  }

  if (!availableRows?.length) {
    return [];
  }

  const lockResults =
    await Promise.allSettled(
      availableRows.map((row) =>
        lockOneRow(row)
      )
    );

  const claimedRows = [];

  for (const result of lockResults) {
    if (
      result.status === "fulfilled" &&
      result.value
    ) {
      claimedRows.push(result.value);
    } else if (
      result.status === "rejected"
    ) {
      console.error(
        "❌ Row-lock error:",
        getErrorText(result.reason)
      );
    }
  }

  return claimedRows;
}

// ─────────────────────────────────────────────
// SAVE SUCCESS
// ─────────────────────────────────────────────

async function saveSuccess(
  row,
  generatedOutput
) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: generatedOutput,
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null,
      updated_at: new Date().toISOString()
    })
    .eq("id", row.id)
    .eq("course_id", COURSE_ID)
    .eq(LOCK_COL, true)
    .eq(
      LOCK_AT_COL,
      row[LOCK_AT_COL]
    )
    .is(OUTPUT_COL, null)
    .select("id");

  if (error) {
    throw new Error(
      `Failed to save explanation: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or output already exists"
    );
  }
}

// ─────────────────────────────────────────────
// RELEASE ONE OWNED LOCK
// ─────────────────────────────────────────────

async function releaseRowLock(row) {
  const { error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null
    })
    .eq("id", row.id)
    .eq("course_id", COURSE_ID)
    .eq(LOCK_COL, true)
    .eq(
      LOCK_AT_COL,
      row[LOCK_AT_COL]
    )
    .is(OUTPUT_COL, null);

  if (error) {
    console.error(
      `❌ Failed to release lock ${row.id}:`,
      error.message
    );
  }
}

async function releaseClaimedRows(rows) {
  await Promise.allSettled(
    rows.map((row) =>
      releaseRowLock(row)
    )
  );
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────

async function processRow(row) {
  console.log(
    `🧠 Generating | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateExplanation(row);

    await saveSuccess(
      row,
      result.output
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | Subtopics=${result.totalSubtopics} | Notes=${result.totalNotes}`
    );

    return {
      success: true,
      creditExhausted: false
    };
  } catch (error) {
    await releaseRowLock(row);

    if (isCreditExhaustionError(error)) {
      console.error(
        "🛑 API credits exhausted. Worker will stop safely."
      );

      return {
        success: false,
        creditExhausted: true,
        error
      };
    }

    console.error(
      `❌ Failed | ${row.subject} | ${row.serial_number} | ${row.topic}: ${getErrorText(error)}`
    );

    return {
      success: false,
      creditExhausted: false,
      error
    };
  }
}

// ─────────────────────────────────────────────
// CONTROL CONCURRENCY
// ─────────────────────────────────────────────

async function processWithConcurrency(rows) {
  let nextIndex = 0;
  let creditExhausted = false;

  async function runner() {
    while (
      nextIndex < rows.length &&
      !creditExhausted
    ) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      const result =
        await processRow(
          rows[currentIndex]
        );

      if (result.creditExhausted) {
        creditExhausted = true;
      }
    }
  }

  const runnerCount = Math.min(
    BATCH_SIZE,
    rows.length
  );

  await Promise.all(
    Array.from(
      { length: runnerCount },
      () => runner()
    )
  );

  if (creditExhausted) {
    await releaseClaimedRows(
      rows.slice(nextIndex)
    );
  }

  return {
    creditExhausted
  };
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────

async function main() {
  console.log(
    `🚀 INICET EXPLANATION WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE}`
  );

  console.log(
    `🎯 Course=${COURSE_ID}`
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
        `📥 Claimed ${rows.length} card(s)`
      );

      const result =
        await processWithConcurrency(
          rows
        );

      if (result.creditExhausted) {
        console.error(
          "🛑 Worker stopped because the API account has no available credits."
        );

        process.exit(1);
      }
    } catch (error) {
      if (isCreditExhaustionError(error)) {
        console.error(
          "🛑 Worker stopped: API credits exhausted."
        );

        process.exit(1);
      }

      console.error(
        "❌ Worker loop error:",
        getErrorText(error)
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
    "❌ Fatal worker error:",
    error
  );

  process.exit(1);
});
