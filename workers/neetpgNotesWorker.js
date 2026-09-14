require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// TABLE AND COLUMNS
// ─────────────────────────────────────────────

const TABLE = "neetpg_pyt_source";

const INPUT_COL = "jsonb_output";
const OUTPUT_COL = "notes_json";
const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────

const MODEL =
  process.env.NEETPG_NOTES_MODEL ||
  "gpt-5.6-terra";

const LIMIT = parseIntegerEnv(
  "NEETPG_NOTES_LIMIT",
  10,
  1,
  50
);

const BATCH_SIZE = parseIntegerEnv(
  "NEETPG_NOTES_BATCH_SIZE",
  3,
  1,
  10
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "NEETPG_NOTES_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "NEETPG_NOTES_LOCK_TTL_MIN",
  30,
  5,
  240
);

const API_RETRIES = parseIntegerEnv(
  "NEETPG_NOTES_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = parseIntegerEnv(
  "NEETPG_NOTES_MAX_OUTPUT_TOKENS",
  12000,
  1000,
  30000
);

const WORKER_ID =
  process.env.WORKER_ID ||
  `neetpg-notes-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

let stopReason = null;

// ─────────────────────────────────────────────
// SYSTEM PROMPT — USED AS PROVIDED
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = String.raw`
This is a PYT , THERE IS FULL LIST OF cLINICAL vignette based and High Yield facts for NEETPG Prep , one need to have Notes as list of 5-8 Buzz words for Rapid revision  Give that complete list as JSON with Mark down of highlighting 1-2 Words in Bold and Unicode for sUPER SCRIPTS , sUBSCRIPTS , SYMBOLS , MATH , GREEK LETTERS TO RENDER in RNW front end  give as Sub topics deivided and under each Subtopic the LIST OF cLINICAL vignette based and High Yield facts for NEETPG Prep  . Strict rule : 1) For every PYT  deliberately extracT  not just direct facts, but also diagnostic clues, differentiators, next-best-step, contraindications, adverse effects, mechanisms, exceptions and examiner traps  similiar to NBME , USMLE DEEP CONTENT for 2 level MCQ analysis 

2) For the specific purpose of a “Rapid Revision Notes” tab, the high yield facts should not be  too long overall. The individual notes should not be  full explanatory sentences rather they should be  5–8-word visual recall triggers. 

The challenge  is compression .  Target roughly 5–10 words per note, with one testable idea per line.

1 note = 1 examinable fact → 5–10 words → 1–2 bold buzzwords

THE FOLLOWING IS EXAMPLE 

Current:
"Temporal-lobe epilepsy may cause wandering, but usually has impaired
awareness/stereotyped episodes rather than organized fugue"

Better:
"TLE wandering → **stereotyped + impaired awareness**"

Current:
"Diagnosis requires exclusion of substance use, seizures, head injury,
delirium and neurocognitive disorders"

Better:
"Fugue DDx → **substances, seizures, head injury, delirium**"

Current:
"Grandiosity continuously since early adulthood → narcissistic PD;
grandiosity with ↓ sleep + pressured speech → mania"

Better:
"Chronic grandiosity → **NPD**; episodic + ↓ sleep → **mania**"

 GIVE OUTPUT IN THIS JSON STRUCTURE 



  {    "topic": "string",    "subtopics": [      {        "subtopic": "string",        "notes": [          "string",          "string"        ]      }    ]  }
`.trim();

// ─────────────────────────────────────────────
// GENERAL HELPERS
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
      `${name} must be an integer from ${min} to ${max}`
    );
  }

  return value;
}

const sleep = (milliseconds) =>
  new Promise((resolve) =>
    setTimeout(resolve, milliseconds)
  );

function errorMessage(error) {
  return String(
    error?.message ||
    error?.error?.message ||
    error
  );
}

function isCreditExhausted(error) {
  return /no credits remaining|insufficient_quota|billing hard limit|billing/i.test(
    errorMessage(error)
  );
}

function isRetryable(error) {
  if (isCreditExhausted(error)) {
    return false;
  }

  const status = Number(error?.status);

  return (
    status === 408 ||
    status === 409 ||
    status === 429 ||
    status >= 500 ||
    /timeout|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(
      errorMessage(error)
    )
  );
}

// ─────────────────────────────────────────────
// BUILD MODEL INPUT
// ─────────────────────────────────────────────

function buildUserInput(row) {
  return [
    `SUBJECT: ${row.subject}`,
    `EXACT TOPIC/PYT: ${row.topic}`,
    `TOPIC SERIAL NUMBER: ${row.serial_number}`,
    `NUMBER OF TIMES ASKED: ${row.number_of_times_asked}`,
    "",
    "FLASHCARD Q→A SOURCE JSON:",
    JSON.stringify(row.jsonb_output, null, 2),
    "",
    "Return only valid JSON.",
    `The JSON topic must be exactly: ${JSON.stringify(
      row.topic
    )}`
  ].join("\n");
}

// ─────────────────────────────────────────────
// OPENAI CALL
// ─────────────────────────────────────────────

async function generateNotes(row) {
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

          input: [
            {
              role: "system",
              content: SYSTEM_PROMPT
            },
            {
              role: "user",
              content: buildUserInput(row)
            }
          ],

          max_output_tokens:
            MAX_OUTPUT_TOKENS
        });

      const rawOutput =
        response.output_text?.trim();

      if (!rawOutput) {
        throw new Error(
          "OpenAI returned empty content"
        );
      }

      return validateAndNormalizeOutput(
        rawOutput,
        row.topic
      );
    } catch (error) {
      lastError = error;

      if (isCreditExhausted(error)) {
        stopReason =
          "OpenAI API credits are exhausted";

        throw error;
      }

      if (
        !isRetryable(error) ||
        attempt === API_RETRIES
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
// EXTRACT JSON
// ─────────────────────────────────────────────

function extractJson(rawOutput) {
  let cleaned = String(rawOutput)
    .replace(/^\s*```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();

  try {
    return JSON.parse(cleaned);
  } catch {
    // Continue to object extraction.
  }

  const firstBrace = cleaned.indexOf("{");

  if (firstBrace === -1) {
    throw new Error(
      "No JSON object found in model output"
    );
  }

  let depth = 0;
  let inString = false;
  let escaped = false;
  let closingBrace = -1;

  for (
    let index = firstBrace;
    index < cleaned.length;
    index += 1
  ) {
    const character = cleaned[index];

    if (escaped) {
      escaped = false;
      continue;
    }

    if (
      character === "\\" &&
      inString
    ) {
      escaped = true;
      continue;
    }

    if (character === '"') {
      inString = !inString;
      continue;
    }

    if (!inString) {
      if (character === "{") {
        depth += 1;
      }

      if (character === "}") {
        depth -= 1;

        if (depth === 0) {
          closingBrace = index;
          break;
        }
      }
    }
  }

  if (closingBrace === -1) {
    throw new Error(
      "Generated JSON object is incomplete"
    );
  }

  const jsonText = cleaned.slice(
    firstBrace,
    closingBrace + 1
  );

  try {
    return JSON.parse(jsonText);
  } catch (error) {
    throw new Error(
      `Model returned invalid JSON: ${error.message}`
    );
  }
}

// ─────────────────────────────────────────────
// VALIDATE NOTES JSON
// ─────────────────────────────────────────────

function validateAndNormalizeOutput(
  rawOutput,
  exactTopic
) {
  const parsed = extractJson(rawOutput);

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
    !Array.isArray(parsed.subtopics) ||
    parsed.subtopics.length === 0
  ) {
    throw new Error(
      "Generated JSON has no subtopics"
    );
  }

  const normalizedSubtopics = [];
  let totalNotes = 0;

  for (
    let groupIndex = 0;
    groupIndex < parsed.subtopics.length;
    groupIndex += 1
  ) {
    const group =
      parsed.subtopics[groupIndex];

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

    const normalizedNotes = [];

    for (
      let noteIndex = 0;
      noteIndex < group.notes.length;
      noteIndex += 1
    ) {
      if (
        typeof group.notes[noteIndex] !==
        "string"
      ) {
        throw new Error(
          `Invalid note in subtopic: ${subtopic}`
        );
      }

      const note =
        group.notes[noteIndex].trim();

      if (!note) {
        throw new Error(
          `Empty note in subtopic: ${subtopic}`
        );
      }

      if (note.length > 500) {
        throw new Error(
          `Note is excessively long in subtopic: ${subtopic}`
        );
      }

      normalizedNotes.push(note);
      totalNotes += 1;
    }

    normalizedSubtopics.push({
      subtopic,
      notes: normalizedNotes
    });
  }

  if (totalNotes === 0) {
    throw new Error(
      "Generated output contains no notes"
    );
  }

  /*
   * The database topic is authoritative.
   * This prevents the model from renaming the PYT.
   */
  return {
    topic: exactTopic,
    subtopics: normalizedSubtopics
  };
}

// ─────────────────────────────────────────────
// RELEASE EXPIRED LOCKS
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
    .eq(LOCK_COL, true)
    .lt(LOCK_AT_COL, cutoff)
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null);

  if (error) {
    throw new Error(
      `Failed to release expired locks: ${error.message}`
    );
  }
}

// ─────────────────────────────────────────────
// CLAIM AVAILABLE ROWS
// ─────────────────────────────────────────────

async function claimRows(limit) {
  await releaseExpiredLocks();

  const {
    data: availableRows,
    error: selectError
  } = await supabase
    .from(TABLE)
    .select(
      [
        "id",
        "subject",
        "serial_number",
        "topic",
        "number_of_times_asked",
        INPUT_COL
      ].join(",")
    )
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .eq(LOCK_COL, false)
    .order("subject", {
      ascending: true
    })
    .order("serial_number", {
      ascending: true
    })
    .limit(limit);

  if (selectError) {
    throw new Error(
      `Failed to select rows: ${selectError.message}`
    );
  }

  if (!availableRows?.length) {
    return [];
  }

  const ids = availableRows.map(
    (row) => row.id
  );

  const lockedAt =
    new Date().toISOString();

  const {
    data: lockedRows,
    error: lockError
  } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: true,
      [LOCK_AT_COL]: lockedAt
    })
    .in("id", ids)
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
    );

  if (lockError) {
    throw new Error(
      `Failed to lock rows: ${lockError.message}`
    );
  }

  return lockedRows || [];
}

// ─────────────────────────────────────────────
// SAVE SUCCESS
// ─────────────────────────────────────────────

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
      row.generation_locked_at
    )
    .is(OUTPUT_COL, null)
    .select("id");

  if (error) {
    throw new Error(
      `Failed to save notes: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Notes were not saved because the lock changed"
    );
  }
}

// ─────────────────────────────────────────────
// RELEASE ONE FAILED ROW
// ─────────────────────────────────────────────

async function releaseRow(row) {
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
      row.generation_locked_at
    )
    .is(OUTPUT_COL, null);

  if (error) {
    console.error(
      `❌ Failed to unlock ${row.id}:`,
      error.message
    );
  }
}

// ─────────────────────────────────────────────
// RELEASE MULTIPLE UNPROCESSED ROWS
// ─────────────────────────────────────────────

async function releaseRows(rows) {
  if (!rows?.length) {
    return;
  }

  await Promise.allSettled(
    rows.map((row) =>
      releaseRow(row)
    )
  );
}

// ─────────────────────────────────────────────
// PROCESS ONE TOPIC
// ─────────────────────────────────────────────

async function processRow(row) {
  if (stopReason) {
    await releaseRow(row);
    return;
  }

  console.log(
    `🧠 Generating | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const notesJson =
      await generateNotes(row);

    await saveSuccess(
      row,
      notesJson
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | ${row.topic}`
    );
  } catch (error) {
    console.error(
      `❌ Failed | ${row.subject} | ${row.serial_number} | ${row.topic}:`,
      errorMessage(error)
    );

    await releaseRow(row);
  }
}

// ─────────────────────────────────────────────
// PROCESS ROWS IN BATCHES
// ─────────────────────────────────────────────

async function processRowsInBatches(rows) {
  for (
    let index = 0;
    index < rows.length;
    index += BATCH_SIZE
  ) {
    if (stopReason) {
      await releaseRows(
        rows.slice(index)
      );

      throw new Error(stopReason);
    }

    const batch = rows.slice(
      index,
      index + BATCH_SIZE
    );

    await Promise.allSettled(
      batch.map((row) =>
        processRow(row)
      )
    );

    if (stopReason) {
      await releaseRows(
        rows.slice(
          index + BATCH_SIZE
        )
      );

      throw new Error(stopReason);
    }
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────

async function main() {
  console.log(
    `🚀 NEETPG NOTES WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Pickup=${LIMIT} | Concurrent=${BATCH_SIZE}`
  );

  while (true) {
    try {
      const rows =
        await claimRows(LIMIT);

      if (!rows.length) {
        await sleep(
          LOOP_SLEEP_MS
        );

        continue;
      }

      console.log(
        `📥 Claimed ${rows.length} topic(s)`
      );

      await processRowsInBatches(rows);
    } catch (error) {
      if (stopReason) {
        throw error;
      }

      console.error(
        "❌ Worker loop error:",
        errorMessage(error)
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
    `🛑 Worker stopped: ${errorMessage(error)}`
  );

  process.exit(1);
});
