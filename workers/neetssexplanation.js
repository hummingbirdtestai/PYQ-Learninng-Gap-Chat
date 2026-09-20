require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// TABLE AND COLUMNS
// ─────────────────────────────────────────────

const TABLE = "neetss_medicine_pyt_source";

const INPUT_COL = "jsonb_output";
const OUTPUT_COL = "notes_json";

const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

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
  process.env.NEETSS_MEDICINE_NOTES_MODEL ||
  "gpt-5-mini";

const PICKUP_LIMIT = parseIntegerEnv(
  "NEETSS_MEDICINE_NOTES_LIMIT",
  20,
  1,
  100
);

const BATCH_SIZE = parseIntegerEnv(
  "NEETSS_MEDICINE_NOTES_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "NEETSS_MEDICINE_NOTES_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "NEETSS_MEDICINE_NOTES_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = parseIntegerEnv(
  "NEETSS_MEDICINE_NOTES_API_RETRIES",
  2,
  0,
  5
);

const WORKER_ID =
  process.env.WORKER_ID ||
  `neetss-medicine-notes-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = String.raw`
This is a PYT. There is a full list of clinical-vignette-based and high-yield facts for postgraduate medical entrance preparation.

Generate Rapid Revision Notes as a JSON list of compressed clinical and examination facts. Use Markdown to highlight 1–2 decisive words in bold. Use Unicode directly for superscripts, subscripts, mathematical symbols, arrows and Greek letters so they render correctly in a React Native Web frontend.

Divide the content into logical subtopics. Under each subtopic, provide clinical-vignette-based and high-yield facts.

For every PYT deliberately extract not only direct facts, but also:

diagnostic clues
differentiators
next-best steps
contraindications
adverse effects
mechanisms
important exceptions
examiner traps
complications
management linkages

The content should approach First Aid compression, AMBOSS clinical linkage and UWorld/NBME-style two-level reasoning.

For the Rapid Revision Notes tab, the facts must not be long explanatory sentences. Each note should be a visual recall trigger.

Target:

1 note = 1 independently examinable decision
approximately 5–10 words per note
1–2 bold buzzwords per note
approximately 25–30 exceptional notes per PYT when the topic supports them

Examples:

"TLE wandering → **stereotyped + impaired awareness**"

"Fugue DDx → **substances, seizures, head injury, delirium**"

"Chronic grandiosity → **NPD**; episodic + ↓ sleep → **mania**"

"Silent chest → **minimal airflow**, not improvement"

"Normal PaCO₂ + severe distress → **respiratory fatigue**"

"Wheeze ↓ + air entry ↓ → **impending failure**"

"Ventilated asthma + hypotension → **dynamic hyperinflation**"

"Hypotension + unilateral absent sounds → **pneumothorax**"

Never fall short of true AMBOSS-level clinical decision linkage. Connect findings to recognizable disease contexts and mechanism-based discrimination.

There must be no redundancy. Create exceptionally discriminating notes. Prefer disease-linked clues, mechanism traps and contrasts that genuinely change an MCQ decision.

Use this hierarchy when relevant:

PYT fact → clinical clue → mechanism → discriminator → management decision → trap or exception → complication

Not every PYT needs every category. Include a category only when it adds a genuinely new examinable decision.

Every generated note must pass this test:

“Does this note enable an additional MCQ decision that another note does not?”

If no, delete it.

Two medically different facts are still redundant if they lead to the same MCQ decision. Keep the more clinically discriminating one.

Prefer decision density over fact density. Every note must do at least one of the following:

answer the PYT
identify a vignette presentation
explain a mechanism required for inference
distinguish a plausible competing answer
change the next diagnostic or management decision
expose an examiner trap
identify an important contraindication or exception
link a condition to a decisive complication

Delete isolated background facts that do none of these.

Make notes contrastive. Prefer distinctions such as:

early versus late disease
stable versus unstable patient
screening versus diagnostic testing
initial versus confirmatory testing
first-line versus rescue treatment
expected finding versus dangerous transition
improvement versus impending failure
common diagnosis versus closest competitor

A useful overall mix is approximately:

20% core recall
30% vignette recognition
20% mechanism-linked inference
20% differentiators and examiner traps
10% next-step and management decisions

Do not enforce the percentages mechanically.

Do not increase content merely to reach a number. Delete synonymous decision pathways and use the available space for exceptions, competing-test discrimination and vignette-changing qualifiers.

Return only valid JSON.

Do not use Markdown code fences.

Do not include introductions, explanations, citations, conclusions or any text outside the JSON.

Use exactly this JSON structure:

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

if (!SYSTEM_PROMPT) {
  throw new Error(
    "SYSTEM_PROMPT cannot be empty"
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

  const output = collected.join("\n").trim();

  if (!output) {
    throw new Error(
      "OpenAI returned empty output"
    );
  }

  return output;
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
    `TOPIC SERIAL NUMBER: ${row.serial_number}`,
    `EXACT TOPIC/PYT: ${row.topic}`,
    `NUMBER OF TIMES ASKED: ${row.number_of_times_asked}`,
    "",
    "SOURCE PYQ AND FUTURE FLASHCARDS:",
    JSON.stringify(row[INPUT_COL], null, 2)
  ].join("\n");
}

// ─────────────────────────────────────────────
// VALIDATE OUTPUT
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

  const topic = String(
    parsed.topic || ""
  ).trim();

  if (!topic) {
    throw new Error(
      "Generated JSON contains no topic"
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

  let totalNotes = 0;

  const subtopics = parsed.subtopics.map(
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
        (note, noteIndex) => {
          if (typeof note !== "string") {
            throw new Error(
              `Note ${noteIndex + 1} in "${subtopic}" is not text`
            );
          }

          const normalizedNote =
            note.trim();

          if (!normalizedNote) {
            throw new Error(
              `Empty note found in "${subtopic}"`
            );
          }

          totalNotes += 1;

          return normalizedNote;
        }
      );

      return {
        subtopic,
        notes
      };
    }
  );

  return {
    output: {
      topic,
      subtopics
    },

    totalNotes,
    totalSubtopics: subtopics.length
  };
}

// ─────────────────────────────────────────────
// GENERATE NOTES
// No max_output_tokens setting.
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

          text: {
            format: {
              type: "json_schema",
              name: "neetss_medicine_notes",
              strict: true,
              schema: NOTES_JSON_SCHEMA
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
// RELEASE EXPIRED NOTES LOCKS
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
      `Failed to release expired locks: ${error.message}`
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
// PROCESS ONE ROW
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
    `🚀 NEET SS MEDICINE NOTES WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE}`
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
    "❌ Fatal NEET SS Medicine notes worker error:",
    error
  );

  process.exit(1);
});
