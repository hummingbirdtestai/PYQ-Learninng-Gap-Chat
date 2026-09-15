require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// TABLE AND COLUMNS
// ─────────────────────────────────────────────

const TABLE = "fmge_master_content";

const INPUT_COL = "explanation";
const OUTPUT_COL = "explanation_new";
const LOCK_COL = "explanation_lock";
const LOCK_AT_COL = "explanation_locked_at";

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────

const MODEL =
  process.env.FMGE_EXPLANATION_NEW_MODEL ||
  "gpt-5.6-terra";

const LIMIT = parseIntegerEnv(
  "FMGE_EXPLANATION_NEW_LIMIT",
  10,
  1,
  50
);

const BATCH_SIZE = parseIntegerEnv(
  "FMGE_EXPLANATION_NEW_BATCH_SIZE",
  3,
  1,
  10
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "FMGE_EXPLANATION_NEW_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "FMGE_EXPLANATION_NEW_LOCK_TTL_MIN",
  30,
  5,
  240
);

const API_RETRIES = parseIntegerEnv(
  "FMGE_EXPLANATION_NEW_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = parseIntegerEnv(
  "FMGE_EXPLANATION_NEW_MAX_OUTPUT_TOKENS",
  12000,
  1000,
  30000
);

const WORKER_ID =
  process.env.WORKER_ID ||
  `fmge-explanation-new-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

let stopReason = null;

// ─────────────────────────────────────────────
// SYSTEM PROMPT — USED AS PROVIDED
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = String.raw`
This is a PYT , THERE IS FULL LIST OF cLINICAL vignette based and High Yield facts for NEETPG Prep , one need to have Notes as list of 5-8 Buzz words for Rapid revision Give that complete list as JSON with Mark down of highlighting 1-2 Words in Bold and Unicode for sUPER SCRIPTS , sUBSCRIPTS , SYMBOLS , MATH , GREEK LETTERS TO RENDER in RNW front end give as Sub topics deivided and under each Subtopic the LIST OF cLINICAL vignette based and High Yield facts for NEETPG Prep . Strict rule : 1) For every PYT deliberately extracT not just direct facts, but also diagnostic clues, differentiators, next-best-step, contraindications, adverse effects, mechanisms, exceptions and examiner traps similiar to NBME , USMLE DEEP CONTENT for 2 level MCQ analysis

For the specific purpose of a “Rapid Revision Notes” tab, the high yield facts should not be too long overall. The individual notes should not be full explanatory sentences rather they should be 5–8-word visual recall triggers.

The challenge is compression . Target roughly 5–10 words per note, with one testable idea per line.

1 note = 1 examinable fact → 5–10 words → 1–2 bold buzzwords

THE FOLLOWING IS EXAMPLE

Current:

"Temporal-lobe epilepsy may cause wandering, but usually has impaired awareness/stereotyped episodes rather than organized fugue"

Better:

"TLE wandering → stereotyped + impaired awareness"

Current:

"Diagnosis requires exclusion of substance use, seizures, head injury, delirium and neurocognitive disorders"

Better:

"Fugue DDx → substances, seizures, head injury, delirium"

Current:

"Grandiosity continuously since early adulthood → narcissistic PD; grandiosity with ↓ sleep + pressured speech → mania"

Better:

"Chronic grandiosity → *NPD; episodic + ↓ sleep → *mania"

never fall short of true AMBOSS-level depth —it is the clinical decision linkage. AMBOSS-quality material would more deliberately connect the finding to recognizable disease contexts and mechanism-based discrimination. For example: postherpetic neuralgia + clothing hurts → allodynia; diabetic neuropathy + bedsheet hurts → tactile allodynia; migraine + scalp/hair brushing hurts → central sensitization; CRPS + gentle touch produces severe pain → allodynia. Those are the kinds of clues that convert a memorized definition into a 2-level vignette answer.

There should not be redundancy. Create exceptionally discriminating notes. Instead of repetitions aim for disease-linked clinical clues, mechanism traps, where they genuinely help differentiate an MCQ.

Adding more content would actually make the Rapid Revision tab worse. What is missing is a small number of more discriminating clinical relationships.

For example, the highest-quality notes would deliberately distinguish:

Early severe asthma: tachypnea → hyperventilation → PaCO₂ ↓

versus

Deteriorating asthma: respiratory fatigue → PaCO₂ normalizes/↑

Similarly:

Wheeze ↓ + dyspnea ↓ → **improvement**

versus

Wheeze ↓ + air entry ↓ → **impending failure**

Another excellent 2-level linkage would be:

Ventilated asthma + hypotension + high pressures → **dynamic hyperinflation**

followed by the differentiator:

Sudden hypotension + unilateral absent sounds → **pneumothorax**

For every PYT, aim for roughly this hierarchy:

Direct PYT fact → vignette clue → mechanism → differentiator → next-best-step → exception/trap → complication/management linkage.

But only include a category when it adds a genuinely new examinable decision.

Instead of 60–80 repetitive facts, prefer approximately 30–45 exceptionally discriminating notes per PYT, depending on topic breadth.

A good note should pass this test:

Can this 5–10-word line help the student answer an MCQ that requires one extra inference beyond simple recall?

GIVE OUTPUT IN THIS JSON STRUCTURE

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
`.trim();

// ─────────────────────────────────────────────
// HELPERS
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

const sleep = (milliseconds) =>
  new Promise((resolve) =>
    setTimeout(resolve, milliseconds)
  );

function getErrorMessage(error) {
  return String(
    error?.message ||
    error?.error?.message ||
    error
  );
}

function isCreditExhausted(error) {
  return /no credits remaining|insufficient_quota|billing hard limit/i.test(
    getErrorMessage(error)
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
      getErrorMessage(error)
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
    `QUESTION SERIAL NUMBER: ${row.serial_number}`,
    "",
    "QUESTION:",
    row.question,
    "",
    "ANSWER:",
    row.answer,
    "",
    "SOURCE EXPLANATION:",
    row.explanation,
    "",
    "Return only valid JSON.",
    `The JSON topic must be exactly ${JSON.stringify(
      row.topic
    )}.`
  ].join("\n");
}

// ─────────────────────────────────────────────
// GENERATE NOTES
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

      return validateOutput(
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
  const cleaned = String(rawOutput)
    .replace(/^\s*```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();

  try {
    return JSON.parse(cleaned);
  } catch {
    const firstBrace =
      cleaned.indexOf("{");

    const lastBrace =
      cleaned.lastIndexOf("}");

    if (
      firstBrace === -1 ||
      lastBrace === -1 ||
      lastBrace <= firstBrace
    ) {
      throw new Error(
        "No complete JSON object found"
      );
    }

    try {
      return JSON.parse(
        cleaned.slice(
          firstBrace,
          lastBrace + 1
        )
      );
    } catch (error) {
      throw new Error(
        `Invalid JSON: ${error.message}`
      );
    }
  }
}

// ─────────────────────────────────────────────
// VALIDATE OUTPUT
// ─────────────────────────────────────────────

function validateOutput(rawOutput, exactTopic) {
  const parsed =
    extractJson(rawOutput);

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
      "Output has no subtopics"
    );
  }

  let totalNotes = 0;

  const subtopics =
    parsed.subtopics.map(
      (group, groupIndex) => {
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
          (value) => {
            if (
              typeof value !== "string" ||
              !value.trim()
            ) {
              throw new Error(
                `Invalid note in ${subtopic}`
              );
            }

            const note = value.trim();

            if (note.length > 500) {
              throw new Error(
                `Excessively long note in ${subtopic}`
              );
            }

            totalNotes += 1;
            return note;
          }
        );

        return {
          subtopic,
          notes
        };
      }
    );

  if (totalNotes === 0) {
    throw new Error(
      "Output contains no notes"
    );
  }

  return {
    topic: exactTopic,
    subtopics
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
// CLAIM ROWS
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
        "question",
        "answer",
        INPUT_COL
      ].join(",")
    )
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .eq(LOCK_COL, false)
    .eq("active", true)
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
    .eq("active", true)
    .select(
      [
        "id",
        "subject",
        "serial_number",
        "topic",
        "question",
        "answer",
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

async function saveSuccess(row, output) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: output,
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null,
      updated_at:
        new Date().toISOString()
    })
    .eq("id", row.id)
    .eq(LOCK_COL, true)
    .eq(
      LOCK_AT_COL,
      row.explanation_locked_at
    )
    .is(OUTPUT_COL, null)
    .select("id");

  if (error) {
    throw new Error(
      `Failed to save output: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed"
    );
  }
}

// ─────────────────────────────────────────────
// RELEASE ONE ROW
// ─────────────────────────────────────────────

async function releaseRow(row) {
  const { error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null,
      updated_at:
        new Date().toISOString()
    })
    .eq("id", row.id)
    .eq(LOCK_COL, true)
    .eq(
      LOCK_AT_COL,
      row.explanation_locked_at
    )
    .is(OUTPUT_COL, null);

  if (error) {
    console.error(
      `❌ Failed to unlock ${row.id}:`,
      error.message
    );
  }
}

async function releaseRows(rows) {
  await Promise.allSettled(
    rows.map((row) =>
      releaseRow(row)
    )
  );
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
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
    const output =
      await generateNotes(row);

    await saveSuccess(row, output);

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | ${row.topic}`
    );
  } catch (error) {
    console.error(
      `❌ Failed | ${row.subject} | ${row.serial_number} | ${row.topic}:`,
      getErrorMessage(error)
    );

    await releaseRow(row);
  }
}

// ─────────────────────────────────────────────
// PROCESS BATCHES
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
    `🚀 FMGE EXPLANATION NEW WORKER STARTED: ${WORKER_ID}`
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
        `📥 Claimed ${rows.length} row(s)`
      );

      await processRowsInBatches(rows);
    } catch (error) {
      if (stopReason) {
        throw error;
      }

      console.error(
        "❌ Worker loop error:",
        getErrorMessage(error)
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
    `🛑 Worker stopped: ${getErrorMessage(error)}`
  );

  process.exit(1);
});
