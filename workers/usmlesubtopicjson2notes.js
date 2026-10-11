"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ============================================================
// DATABASE CONFIGURATION
// ============================================================

const TABLE = "usmle_step1_topic_grouped";
const INPUT_COL = "subtopics_json";
const OUTPUT_COL = "notes_json";
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
  process.env.USMLE_TOPIC_NOTES_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "USMLE_TOPIC_NOTES_LIMIT",
  50,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "USMLE_TOPIC_NOTES_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "USMLE_TOPIC_NOTES_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "USMLE_TOPIC_NOTES_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "USMLE_TOPIC_NOTES_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = integerEnv(
  "USMLE_TOPIC_NOTES_MAX_OUTPUT_TOKENS",
  16000,
  1000,
  50000
);

const WORKER_ID =
  process.env.USMLE_TOPIC_NOTES_WORKER_ID ||
  `usmle-topic-notes-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ============================================================
// PASTE YOUR COMPLETE PROMPT BELOW
// ============================================================

/*
Paste the complete prompt between the comment markers.

Important:
The prompt must not contain the closing comment characters:
* immediately followed by /
*/

const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
This is a HYT of USMLE Based on First Aid for USMLE Step 1  , THERE IS FULL LIST OF cLINICAL vignette based and High Yield facts for USMLE Step 1  , one need to have Notes as list of 5-8 Buzz words for Rapid revision  Give that complete list as JSON with Mark down of highlighting 1-2 Words in Bold and Unicode for sUPER SCRIPTS , sUBSCRIPTS , SYMBOLS , MATH , GREEK LETTERS TO RENDER in RNW front end  give as Sub topics deivided and under each Subtopic the LIST OF cLINICAL vignette based and High Yield facts for NEETPG Prep  . Strict rule : 1) For every PYT  deliberately extracT  not just direct facts, but also diagnostic clues, differentiators, next-best-step, contraindications, adverse effects, mechanisms, exceptions and examiner traps  similiar to NBME , USMLE DEEP CONTENT for 3 level MCQ analysis of Amboss , Americal Board Level and depth 

2) For the specific purpose of a “Rapid Revision Notes” tab, the high yield facts should not be  too long overall. The individual notes should not be  full explanatory sentences rather they should be  5–8-word visual recall triggers. 

The challenge  is compression .  Target roughly 5–10 words per note, with one testable idea per line.

1 note = 1 examinable fact → 5–10 words → 1–2 bold buzzwords

THE FOLLOWING IS EXAMPLE 

Current:
"Temporal-lobe epilepsy may cause wandering, but usually has impaired
awareness/stereotyped episodes rather than organized fugue"

Better:
"TLE wandering → *stereotyped + impaired awareness*"

Current:
"Diagnosis requires exclusion of substance use, seizures, head injury,
delirium and neurocognitive disorders"

Better:
"Fugue DDx → *substances, seizures, head injury, delirium*"

Current:
"Grandiosity continuously since early adulthood → narcissistic PD;
grandiosity with ↓ sleep + pressured speech → mania"

Better:
"Chronic grandiosity → *NPD; episodic + ↓ sleep → **mania*"

 GIVE OUTPUT IN THIS JSON STRUCTURE 



  {    "topic": "string",    "subtopics": [      {        "subtopic": "string",        "notes": [          "string",          "string"        ]      }    ]  }
  */ };

  const source = promptContainer.toString();
  const start = source.indexOf("/*") + 2;
  const end = source.lastIndexOf("*/");

  return source.slice(start, end).trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE USMLE NOTES PROMPT HERE"
  )
) {
  throw new Error(
    "Paste your complete prompt inside SYSTEM_PROMPT before starting the worker"
  );
}

// Technical JSON rules are added automatically.
// They do not need to be added to your main prompt.

const OUTPUT_CONTRACT = `
MANDATORY OUTPUT CONTRACT:

Return only one valid JSON object.

Use exactly this top-level structure:

{
  "topic": "string",
  "subtopics": [
    {
      "subtopic": "string",
      "notes": [
        "string",
        "string"
      ]
    }
  ]
}

Rules:

- The topic value must match the supplied topic.
- subtopics must be a JSON array.
- Every subtopic must contain a non-empty subtopic string.
- Every subtopic must contain a notes array.
- Every note must be a non-empty JSON string.
- Preserve every important examinable fact from the supplied source.
- Do not return Markdown outside the JSON object.
- Markdown bold using **word** is permitted inside note strings.
- Unicode symbols are permitted inside note strings.
- Do not wrap the JSON in a Markdown code fence.
- Do not include commentary before or after the JSON.
- Do not include trailing commas.
- Do not use JavaScript object syntax.
- Return strict, parseable JSON only.
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
  return /empty output|valid JSON|JSON object|topic|subtopics|subtopic|notes array|note must|source JSON|unexpected output/i.test(
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
// SERIALIZE SOURCE JSON
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
      throw new Error(
        `${INPUT_COL} does not contain valid JSON`
      );
    }
  }

  return JSON.stringify(
    value,
    null,
    2
  );
}

// ============================================================
// BUILD MODEL INPUT
// ============================================================

function buildInput(row) {
  return [
    `TOPIC: ${requiredText(row.topic, "Topic")}`,
    `SUBJECT: ${requiredText(row.subject, "Subject")}`,
    `SUBJECT-WISE TOPIC NUMBER: ${
      row.topic_subjectwise_serial_number ?? "Not assigned"
    }`,
    `SOURCE ROW COUNT: ${row.source_row_count}`,
    "",
    "SOURCE SUBTOPICS JSON:",
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
// CLEAN AND PARSE MODEL JSON
// ============================================================

function removeOuterCodeFence(value) {
  let text = requiredText(
    value,
    "Generated output"
  ).trim();

  text = text.replace(
    /^```(?:json|javascript|js)?[ \t]*\r?\n/i,
    ""
  );

  text = text.replace(
    /\r?\n```[ \t]*$/i,
    ""
  );

  return text.trim();
}

function extractJsonObject(value) {
  const text = removeOuterCodeFence(value);

  try {
    return JSON.parse(text);
  } catch {
    const firstBrace = text.indexOf("{");
    const lastBrace = text.lastIndexOf("}");

    if (
      firstBrace === -1 ||
      lastBrace === -1 ||
      lastBrace <= firstBrace
    ) {
      throw new Error(
        "Generated output does not contain a valid JSON object"
      );
    }

    const jsonCandidate = text.slice(
      firstBrace,
      lastBrace + 1
    );

    try {
      return JSON.parse(jsonCandidate);
    } catch (error) {
      throw new Error(
        `Generated output is not valid JSON: ${error.message}`
      );
    }
  }
}

// ============================================================
// NORMALIZE AND VALIDATE GENERATED NOTES
// ============================================================

function normalizeNotesOutput(value, row) {
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value)
  ) {
    throw new Error(
      "Generated output must be a JSON object"
    );
  }

  const sourceTopic = requiredText(
    row.topic,
    "Source topic"
  );

  const topic =
    typeof value.topic === "string" &&
    value.topic.trim()
      ? value.topic.trim()
      : sourceTopic;

  if (!Array.isArray(value.subtopics)) {
    throw new Error(
      "Generated output must contain a subtopics array"
    );
  }

  const subtopics = value.subtopics.map(
    (item, subtopicIndex) => {
      if (
        !item ||
        typeof item !== "object" ||
        Array.isArray(item)
      ) {
        throw new Error(
          `Subtopic ${subtopicIndex + 1} must be a JSON object`
        );
      }

      const subtopic = requiredText(
        item.subtopic,
        `Subtopic ${subtopicIndex + 1} name`
      );

      if (!Array.isArray(item.notes)) {
        throw new Error(
          `Subtopic "${subtopic}" must contain a notes array`
        );
      }

      const notes = item.notes.map(
        (note, noteIndex) => {
          if (typeof note !== "string") {
            throw new Error(
              `Note ${noteIndex + 1} in "${subtopic}" must be a string`
            );
          }

          const cleaned = note.trim();

          if (!cleaned) {
            throw new Error(
              `Note ${noteIndex + 1} in "${subtopic}" is empty`
            );
          }

          return cleaned;
        }
      );

      if (notes.length === 0) {
        throw new Error(
          `Subtopic "${subtopic}" contains no notes`
        );
      }

      return {
        subtopic,
        notes
      };
    }
  );

  if (subtopics.length === 0) {
    throw new Error(
      "Generated output contains no subtopics"
    );
  }

  return {
    topic,
    subtopics
  };
}

function validateNotesOutput(value) {
  let totalNotes = 0;

  for (const item of value.subtopics) {
    totalNotes += item.notes.length;
  }

  if (totalNotes === 0) {
    throw new Error(
      "Generated output contains no notes"
    );
  }

  return {
    json: value,
    subtopicCount: value.subtopics.length,
    noteCount: totalNotes,
    characters:
      JSON.stringify(value).length
  };
}

// ============================================================
// GENERATE NOTES
// ============================================================

async function generateNotes(row) {
  let lastError;

  for (
    let attempt = 0;
    attempt <= API_RETRIES;
    attempt += 1
  ) {
    try {
      const instructions = [
        SYSTEM_PROMPT,
        OUTPUT_CONTRACT
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

      const parsedOutput =
        extractJsonObject(rawOutput);

      const normalizedOutput =
        normalizeNotesOutput(
          parsedOutput,
          row
        );

      return validateNotesOutput(
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
      "USMLE topic-notes generation failed"
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
      `Failed to release expired locks: ${error.message}`
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
        "topic",
        "source_row_count",
        "topic_subjectwise_serial_number",
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
      "id,subject,topic,topic_subjectwise_serial_number"
    )
    .eq(LOCK_COL, false)
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .order("subject", {
      ascending: true
    })
    .order(
      "topic_subjectwise_serial_number",
      {
        ascending: true,
        nullsFirst: false
      }
    )
    .order("topic", {
      ascending: true
    })
    .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending rows: ${error.message}`
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
// SAVE SUCCESSFUL NOTES
// ============================================================

async function saveSuccess(row, notesJson) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: notesJson,
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
      `Failed to save ${OUTPUT_COL}: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or notes already exist"
    );
  }
}

// ============================================================
// RELEASE ONE LOCK
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
    `Generating USMLE notes | ${row.subject} | ${
      row.topic_subjectwise_serial_number ?? "-"
    } | ${row.topic}`
  );

  try {
    const result =
      await generateNotes(row);

    await saveSuccess(
      row,
      result.json
    );

    console.log(
      `Completed | ${row.subject} | ${
        row.topic_subjectwise_serial_number ?? "-"
      } | ${row.topic} | subtopics=${result.subtopicCount} | notes=${result.noteCount} | characters=${result.characters}`
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
      `Failed | ${row.subject} | ${
        row.topic_subjectwise_serial_number ?? "-"
      } | ${row.topic}: ${errorText(error)}`
    );

    return {
      creditExhausted: false
    };
  }
}

// ============================================================
// PROCESS BATCH WITH CONTROLLED CONCURRENCY
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

// ============================================================
// MAIN WORKER LOOP
// ============================================================

async function main() {
  console.log(
    `USMLE SUBTOPICS -> NOTES WORKER STARTED: ${WORKER_ID}`
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
        `Claimed ${rows.length} pending row(s)`
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

// ============================================================
// START WORKER
// ============================================================

main().catch((error) => {
  console.error(
    "Fatal USMLE topic-notes worker error:",
    error
  );

  process.exit(1);
});
