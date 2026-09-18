require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// TABLE AND COLUMNS
// ─────────────────────────────────────────────

const TABLE = "inicet_pyt_source";

const INPUT_COL = "jsonb_output";
const OUTPUT_COL = "notes_json";

const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

// ─────────────────────────────────────────────
// SETTINGS
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
  process.env.INICET_NOTES_MODEL ||
  "gpt-5-mini";

const PICKUP_LIMIT = parseIntegerEnv(
  "INICET_NOTES_LIMIT",
  20,
  1,
  100
);

const BATCH_SIZE = parseIntegerEnv(
  "INICET_NOTES_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "INICET_NOTES_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "INICET_NOTES_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = parseIntegerEnv(
  "INICET_NOTES_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = parseIntegerEnv(
  "INICET_NOTES_MAX_OUTPUT_TOKENS",
  6000,
  1000,
  30000
);

const WORKER_ID =
  process.env.WORKER_ID ||
  `inicet-notes-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// Paste your complete prompt between the backticks.
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

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT ===
    "PASTE THE COMPLETE PROMPT YOU PROVIDED HERE"
) {
  throw new Error(
    "Paste the complete INICET Notes prompt into SYSTEM_PROMPT"
  );
}

// ─────────────────────────────────────────────
// JSON SCHEMA
// ─────────────────────────────────────────────

const NOTES_JSON_SCHEMA = {
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
  return /no credits remaining|insufficient_quota|billing_hard_limit|credit balance|billing/i.test(
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
    /timeout|temporar|unavailable|rate limit|ECONNRESET|ETIMEDOUT|socket hang up/i.test(
      getErrorText(error)
    )
  );
}

function cleanJsonText(rawOutput) {
  return String(rawOutput)
    .trim()
    .replace(/^\s*```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();
}

function extractResponseText(response) {
  if (
    typeof response?.output_text === "string" &&
    response.output_text.trim()
  ) {
    return response.output_text.trim();
  }

  const parts = [];

  for (const outputItem of response?.output || []) {
    for (const contentItem of outputItem?.content || []) {
      if (
        contentItem?.type === "output_text" &&
        typeof contentItem.text === "string"
      ) {
        parts.push(contentItem.text);
      }
    }
  }

  const output = parts.join("\n").trim();

  if (!output) {
    throw new Error(
      "OpenAI returned empty output"
    );
  }

  return output;
}

// ─────────────────────────────────────────────
// BUILD INPUT
// ─────────────────────────────────────────────

function buildUserInput(row) {
  return [
    `SUBJECT: ${row.subject}`,
    `TOPIC SERIAL NUMBER: ${row.serial_number}`,
    `EXACT TOPIC/PYT: ${row.topic}`,
    `NUMBER OF TIMES ASKED: ${row.number_of_times_asked}`,
    "",
    "SOURCE FLASHCARDS:",
    JSON.stringify(row.jsonb_output, null, 2)
  ].join("\n");
}

// ─────────────────────────────────────────────
// VALIDATE GENERATED NOTES
// ─────────────────────────────────────────────

function validateGeneratedOutput(rawOutput, row) {
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
    !Array.isArray(parsed.subtopics) ||
    parsed.subtopics.length === 0
  ) {
    throw new Error(
      "Generated JSON contains no subtopics"
    );
  }

  let noteCount = 0;

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
          `Subtopic ${groupIndex + 1} has no title`
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
          const normalizedNote =
            String(note || "").trim();

          if (!normalizedNote) {
            throw new Error(
              `Empty note in "${subtopic}" at position ${noteIndex + 1}`
            );
          }

          noteCount += 1;

          return normalizedNote;
        }
      );

      return {
        subtopic,
        notes
      };
    });

  if (noteCount === 0) {
    throw new Error(
      "Generated output contains no notes"
    );
  }

  /*
   * The database topic is authoritative.
   * This prevents the model from renaming the PYT.
   */
  return {
    output: {
      topic: row.topic,
      subtopics: normalizedSubtopics
    },
    subtopicCount: normalizedSubtopics.length,
    noteCount
  };
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

          instructions: SYSTEM_PROMPT,

          input: buildUserInput(row),

          max_output_tokens:
            MAX_OUTPUT_TOKENS,

          text: {
            format: {
              type: "json_schema",
              name: "inicet_rapid_revision_notes",
              strict: true,
              schema: NOTES_JSON_SCHEMA
            }
          }
        });

      const rawOutput =
        extractResponseText(response);

      return validateGeneratedOutput(
        rawOutput,
        row
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
// RELEASE EXPIRED NOTES LOCKS
// Only rows needing notes are affected.
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
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .lt(LOCK_AT_COL, cutoff);

  if (error) {
    throw new Error(
      `Failed to release expired notes locks: ${error.message}`
    );
  }
}

// ─────────────────────────────────────────────
// LOCK ONE ROW
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
          "number_of_times_asked",
          INPUT_COL
        ].join(",")
      )
      .not(INPUT_COL, "is", null)
      .is(OUTPUT_COL, null)
      .eq(LOCK_COL, false)
      .order("serial_number", {
        ascending: true
      })
      .limit(limit);

  if (error) {
    throw new Error(
      `Failed to select pending notes rows: ${error.message}`
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

async function saveSuccess(row, notesOutput) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: notesOutput,
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
      `Failed to save notes: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or notes already exist"
    );
  }
}

// ─────────────────────────────────────────────
// RELEASE OWNED LOCK
// ─────────────────────────────────────────────

async function releaseRowLock(row) {
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
// PROCESS ONE PYT
// ─────────────────────────────────────────────

async function processRow(row) {
  console.log(
    `🧠 Generating notes | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateNotes(row);

    await saveSuccess(
      row,
      result.output
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | Subtopics=${result.subtopicCount} | Notes=${result.noteCount}`
    );

    return {
      success: true,
      creditExhausted: false
    };
  } catch (error) {
    await releaseRowLock(row);

    if (isCreditExhaustionError(error)) {
      console.error(
        "🛑 OpenAI credits exhausted"
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
    `🚀 INICET NOTES WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE} | Max output=${MAX_OUTPUT_TOKENS}`
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
        `📥 Claimed ${rows.length} PYT(s)`
      );

      const result =
        await processWithConcurrency(
          rows
        );

      if (result.creditExhausted) {
        console.error(
          "🛑 Worker stopped because API credits are unavailable"
        );

        process.exit(1);
      }
    } catch (error) {
      if (isCreditExhaustionError(error)) {
        console.error(
          "🛑 Worker stopped: OpenAI credits exhausted"
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
    "❌ Fatal INICET Notes worker error:",
    error
  );

  process.exit(1);
});
