require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// TABLE AND COURSE
// ─────────────────────────────────────────────

const TABLE = "fmge_master_content";

const COURSE_ID =
  "48d5bb2d-fb66-4b41-842f-35baf45d65ad";

const INPUT_COL = "question";
const OUTPUT_COL = "explanation_new";

const LOCK_COL = "explanation_lock";
const LOCK_AT_COL = "explanation_locked_at";

// ─────────────────────────────────────────────
// WORKER SETTINGS
// ─────────────────────────────────────────────

const MODEL =
  process.env.NEETPG_EXPLANATION_MODEL ||
  "gpt-5.6-terra";

const LIMIT = parseIntegerEnv(
  "NEETPG_EXPLANATION_LIMIT",
  20,
  1,
  100
);

const BATCH_SIZE = parseIntegerEnv(
  "NEETPG_EXPLANATION_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "NEETPG_EXPLANATION_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "NEETPG_EXPLANATION_LOCK_TTL_MIN",
  60,
  5,
  240
);

const API_RETRIES = parseIntegerEnv(
  "NEETPG_EXPLANATION_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = parseIntegerEnv(
  "NEETPG_EXPLANATION_MAX_OUTPUT_TOKENS",
  12000,
  1000,
  30000
);

const WORKER_ID =
  process.env.WORKER_ID ||
  `neetpg-explanation-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

let stopReason = null;

// ─────────────────────────────────────────────
// SYSTEM PROMPT — VERBATIM
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = String.raw`
This is a PYT , THERE IS FULL LIST OF cLINICAL vignette based and High Yield facts for NEETPG Prep , one need to have Notes as list of 5-8 Buzz words for Rapid revision Give that complete list as JSON with Mark down of highlighting 1-2 Words in Bold and Unicode for sUPER SCRIPTS , sUBSCRIPTS , SYMBOLS , MATH , GREEK LETTERS TO RENDER in RNW front end give as Sub topics deivided and under each Subtopic the LIST OF cLINICAL vignette based and High Yield facts for NEETPG Prep . Strict rule : 1) For every PYT deliberately extracT not just direct facts, but also diagnostic clues, differentiators, next-best-step, contraindications, adverse effects, mechanisms, exceptions and examiner traps similiar to NBME , USMLE DEEP CONTENT for 2 level MCQ analysis

For the specific purpose of a “Rapid Revision Notes” tab, the high yield facts should not be too long overall. The individual notes should not be full explanatory sentences rather they should be 5–8-word visual recall triggers.

The challenge is compression . Target roughly 5–10 words per note, with one testable idea per line.

1 note = 1 examinable fact → 5–10 words → 1–2 bold buzzwords

THE FOLLOWING IS EXAMPLE

Current:

"Temporal-lobe epilepsy may cause wandering, but usually has impaired

awareness/stereotyped episodes rather than organized fugue"

Better:

"TLE wandering → stereotyped + impaired awareness"

Current:

"Diagnosis requires exclusion of substance use, seizures, head injury,

delirium and neurocognitive disorders"

Better:

"Fugue DDx → substances, seizures, head injury, delirium"

Current:

"Grandiosity continuously since early adulthood → narcissistic PD;

grandiosity with ↓ sleep + pressured speech → mania"

Better:

"Chronic grandiosity → *NPD; episodic + ↓ sleep → *mania"

never fall short of true AMBOSS-level depth —it is the clinical decision linkage. AMBOSS-quality material would more deliberately connect the finding to recognizable disease contexts and mechanism-based discrimination. For example: postherpetic neuralgia + clothing hurts → allodynia; diabetic neuropathy + bedsheet hurts → tactile allodynia; migraine + scalp/hair brushing hurts → central sensitization; CRPS + gentle touch produces severe pain → allodynia. Those are the kinds of clues that convert a memorized definition into a 2-level vignette answer.

There should not be redundancy. Create exceptionally discriminating notes. Instead of repetitions aim for disease-linked clinical clues, mechanism traps, ,where they genuinely help differentiate an MCQ
Adding more content would actually make the Rapid Revision tab worse. What is missing is a small number of more discriminating clinical relationships.

For example, the highest-quality notes would deliberately distinguish:

Early severe asthma: tachypnea → hyperventilation → PaCO₂ ↓

versus

Deteriorating asthma: respiratory fatigue → PaCO₂ normalizes/↑

That is exactly the kind of physiological transition an examiner can hide inside a vignette.

Similarly:

Wheeze ↓ + dyspnea ↓ → **improvement**

versus

Wheeze ↓ + air entry ↓ → **impending failure**

That distinction is more valuable than having three separate notes saying silent chest is severe.

Another excellent 2-level linkage would be:

Ventilated asthma + hypotension + high pressures → **dynamic hyperinflation**

followed by the differentiator:

Sudden hypotension + unilateral absent sounds → **pneumothorax**

Now the student isn't merely recalling complications—they're discriminating between two causes of sudden deterioration.

The standard I would use for RevisionCart

For every PYT, your worker should aim for roughly this hierarchy:

Direct PYT fact → vignette clue → mechanism → differentiator → next-best-step → exception/trap → complication/management linkage.

But only include a category when it adds a genuinely new examinable decision.

So instead of 60–80 repetitive facts, I would prefer approximately 30–45 exceptionally discriminating notes per PYT, depending on topic breadth.

A good RevisionCart note should pass this test:

Can this 5–10-word line help the student answer an MCQ that requires one extra inference beyond simple recall?

For example:

Silent chest → **minimal airflow**, not clinical improvement

is good.

Normal PaCO₂ + severe distress → **respiratory fatigue**

is better.

Wheeze ↓ + air entry ↓ → **impending failure**

is excellent.

Ventilated asthma + hypotension → **dynamic hyperinflation**

is excellent.

Hypotension + unilateral absent sounds → **pneumothorax**

is excellent.

Those last three are where your notes start approaching AMBOSS/UWorld-style clinical discrimination while retaining First Aid-like compression.

So I would keep your current JSON architecture and note-length rule exactly as they are, but tighten the generation instruction to prioritize disease-linked clues and competing-diagnosis discrimination over repeated direct facts
original hierarchy is correct:



PYT fact → clinical clue → mechanism → discriminator → management decision → trap/exception → complication



But not every PYT needs every category.



For this particular PYT, I would rather have around 25–30 exceptional notes than the current ~50.



The worker should ask of every generated line:

“Does this note enable an additional MCQ decision that another note does not?”

If no → delete it.



That single rule would probably improve your generated notes more than adding additional medical content.



The next improvement should be stronger deduplication and more contrastive 2-step decision links, not more facts.
Every additional note must create a new MCQ decision. If another note already allows the same decision, delete it.

Make  notes more contrastive.

A useful target mix would be roughly 20% core recall, 30% vignette recognition, 20% mechanism-linked inference, 20% differentiators/examiner traps, and 10% next-step/management. It doesn't need to be mechanically enforced, but it captures the desired character.
Prefer decision density over fact density. Every note must either answer the PYT, identify a vignette presentation, explain a mechanism needed for inference, distinguish a plausible competing answer, change the next diagnostic/management decision, or expose an examiner trap. Delete isolated background facts that do none of these.

Two medically different facts are still redundant if they lead to the same MCQ decision. Keep the more clinically discriminating one.
DO not increase the amount of content , delete synonymous decision pathways and spend those tokens on exceptions, competing-test discrimination, and vignette-changing qualifiers.
First Aid compression + AMBOSS clinical connections + UWorld/NBME-style discrimination → in 5–10-word recall triggers.

That would be a genuinely strong format for NEET-PG last-mile revision.
GIVE OUTPUT IN THIS JSON STRUCTURE

{ "topic": "string", "subtopics": [ { "subtopic": "string", "notes": [ "string", "string" ] } ] }
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
// MODEL CALL
// Input is only the question column.
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

          input: row.question,

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
// JSON EXTRACTION
// ─────────────────────────────────────────────

function extractJson(rawOutput) {
  const cleaned = String(rawOutput)
    .replace(/^\s*```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();

  try {
    return JSON.parse(cleaned);
  } catch {
    // Continue with balanced-object extraction.
  }

  const firstBrace =
    cleaned.indexOf("{");

  if (firstBrace === -1) {
    throw new Error(
      "No JSON object found in model output"
    );
  }

  let depth = 0;
  let inString = false;
  let escaped = false;
  let finalBrace = -1;

  for (
    let index = firstBrace;
    index < cleaned.length;
    index += 1
  ) {
    const character =
      cleaned[index];

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
      } else if (
        character === "}"
      ) {
        depth -= 1;

        if (depth === 0) {
          finalBrace = index;
          break;
        }
      }
    }
  }

  if (finalBrace === -1) {
    throw new Error(
      "Generated JSON object is incomplete"
    );
  }

  const jsonText = cleaned.slice(
    firstBrace,
    finalBrace + 1
  );

  try {
    return JSON.parse(jsonText);
  } catch (error) {
    throw new Error(
      `Invalid JSON: ${error.message}`
    );
  }
}

// ─────────────────────────────────────────────
// JSON VALIDATION
// ─────────────────────────────────────────────

function validateAndNormalizeOutput(
  rawOutput,
  exactTopic
) {
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
          (value, noteIndex) => {
            if (
              typeof value !== "string"
            ) {
              throw new Error(
                `Invalid note ${noteIndex + 1} in ${subtopic}`
              );
            }

            const note =
              value.trim();

            if (!note) {
              throw new Error(
                `Empty note in ${subtopic}`
              );
            }

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

  /*
   * The database topic is authoritative.
   * The model cannot rename the topic.
   */
  return {
    topic: exactTopic,
    subtopics
  };
}

// ─────────────────────────────────────────────
// RELEASE EXPIRED LOCKS
// Only for the selected NEET-PG course.
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
    .lt(LOCK_AT_COL, cutoff)
    .is(OUTPUT_COL, null)
    .eq("active", true);

  if (error) {
    throw new Error(
      `Failed to release expired locks: ${error.message}`
    );
  }
}

// ─────────────────────────────────────────────
// CLAIM ROWS
// Every query is filtered by course_id.
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
        "course_id",
        "subject",
        "serial_number",
        "topic",
        "question"
      ].join(",")
    )
    .eq("course_id", COURSE_ID)
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
    .eq("course_id", COURSE_ID)
    .in("id", ids)
    .eq(LOCK_COL, false)
    .is(OUTPUT_COL, null)
    .eq("active", true)
    .select(
      [
        "id",
        "course_id",
        "subject",
        "serial_number",
        "topic",
        "question",
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

async function saveSuccess(
  row,
  generatedOutput
) {
  const updatedAt =
    new Date().toISOString();

  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: generatedOutput,
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null,
      updated_at: updatedAt
    })
    .eq("id", row.id)
    .eq("course_id", COURSE_ID)
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
    .eq("course_id", COURSE_ID)
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
    const generatedOutput =
      await generateExplanation(row);

    await saveSuccess(
      row,
      generatedOutput
    );

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
// PROCESS ROWS IN CONCURRENT BATCHES
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
    `🚀 NEETPG EXPLANATION WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `🎯 Course=${COURSE_ID}`
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
