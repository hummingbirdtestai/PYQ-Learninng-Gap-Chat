require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────

const TABLE = "neetpg_pyt_source";

const INPUT_COL = "jsonb_output";
const OUTPUT_COL = "mcq_json";
const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

const MODEL =
  process.env.NEETPG_MCQ_MODEL ||
  "gpt-5.6-terra";

const LIMIT = parseIntegerEnv(
  "NEETPG_MCQ_LIMIT",
  5,
  1,
  50
);

const BATCH_SIZE = parseIntegerEnv(
  "NEETPG_MCQ_BATCH_SIZE",
  2,
  1,
  10
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "NEETPG_MCQ_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "NEETPG_MCQ_LOCK_TTL_MIN",
  30,
  5,
  240
);

const API_RETRIES = parseIntegerEnv(
  "NEETPG_MCQ_API_RETRIES",
  2,
  0,
  5
);

const WORKER_ID =
  process.env.WORKER_ID ||
  `neetpg-mcq-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// Paste your complete prompt between backticks.
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = String.raw`
You are an expert medical MCQ writer for USMLE Step 1/2CK, NEET-PG, and FMGE. tAKE THE FLASH CARD Q--> As that have PYQs and future Questions , create 10 medically accurate, exam-standard clinical vignette MCQs. Fror Each MCQ STEM Write a 30–40 word clinical stem requiring 2–3 levels of reasoning: infer the condition → recognize the relevant complication/anatomy/physiology → answer the tested concept. Never reveal the answer pathway. Do not explicitly name the target diagnosis, structure, gene, enzyme, vessel, pathway, biochemical state, or mechanism being tested. Show findings; make the student infer them. Use a logical clinical sequence where applicable: context/risk → presentation → relevant examination/vitals → relevant investigations/intervention → lead-in. Include exactly one plausible red herring only when it genuinely competes with the correct diagnosis. Discriminatory Value Rule Every stem detail must do at least one: Support the correct answer. Weaken a distractor. Establish necessary timing/severity/context. Serve as the intentional red herring. Delete everything else. Never add decorative demographics, history, normal findings, routine vitals, or irrelevant tests merely to make the vignette realistic. Use raw values with reference ranges instead of labels such as “anemia,” “hyperkalemia,” or “leukocytosis.” Include labs/vitals/imaging only when relevant to solving the question. Never invent irrelevant data to satisfy formatting. Vitals must physiologically match the clinical state. The final lead-in must be neutral and contain no diagnostic or mechanistic hint. OPTIONS Provide exactly four competitive options (A–D), each 2–5 words. Options must be: grammatically and structurally parallel, similar in specificity, medically plausible, mutually distinct, from the same conceptual category. Avoid giveaway opposites, obviously unrelated distractors, or one option that differs conspicuously in length/structure. ACCURACY Medical, anatomical, embryological, pharmacological, and biochemical facts must be textbook-accurate. Management questions must follow current accepted guidelines. Do not oversimplify anatomical boundaries or mechanisms. EXPLANATION Explain: Correct Answer Summary: answer + core reason. Diagnostic Pathway: concise stepwise reasoning from clues to answer. Why Other Options Fail: individually explain B/C/D or whichever are incorrect. Examiner's Trap: identify the intended misconception/buzzword trap. Do not merely restate the answer. FINAL QUALITY CHECK Before output, verify: Stem = 30–40 words. Options = 2–5 words each. No answer giveaway. Every stem detail has discriminatory value. Exactly one best answer. No medically incorrect distractor logic. No unnecessary information. OUTPUT Return ONLY valid JSON CONTAINNING EACH MCQ as a object in the JSON : { "Stem": "...", "A": "...", "B": "...", "C": "...", "D": "...", "Correct Answer": "A", "Explanation": { "Correct Answer Summary": "...", "Diagnostic Pathway": ["...", "...", "..."], "Why the Other Options Fail": { "B": "...", "C": "...", "D": "..." }, "Examiner's Trap": "..." } }

IMPORTANT JSON REQUIREMENT:

Return exactly one valid JSON object using this structure:

{
  "mcqs": [
    {
      "Stem": "...",
      "A": "...",
      "B": "...",
      "C": "...",
      "D": "...",
      "Correct Answer": "A",
      "Explanation": {
        "Correct Answer Summary": "...",
        "Diagnostic Pathway": ["...", "...", "..."],
        "Why the Other Options Fail": {
          "B": "...",
          "C": "...",
          "D": "..."
        },
        "Examiner's Trap": "..."
      }
    }
  ]
}

The mcqs array must contain exactly 10 MCQ objects.
Do not include Markdown fences or any text outside JSON.
`.trim();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE MCQ PROMPT HERE"
  )
) {
  throw new Error(
    "Paste your complete prompt into SYSTEM_PROMPT"
  );
}

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

function isRetryable(error) {
  const status = Number(error?.status);

  return (
    status === 408 ||
    status === 409 ||
    status === 429 ||
    status >= 500 ||
    /timeout|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(
      String(error?.message || error)
    )
  );
}

// ─────────────────────────────────────────────
// BUILD USER INPUT
// ─────────────────────────────────────────────

function buildUserInput(row) {
  return [
    `SUBJECT: ${row.subject}`,
    `TOPIC SERIAL NUMBER: ${row.serial_number}`,
    `EXACT TOPIC: ${row.topic}`,
    `NUMBER OF TIMES ASKED: ${row.number_of_times_asked}`,
    "",
    "FLASHCARDS:",
    JSON.stringify(row.jsonb_output, null, 2)
  ].join("\n");
}

// ─────────────────────────────────────────────
// CALL OPENAI
// ─────────────────────────────────────────────

async function callOpenAI(row) {
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
          ]
        });

      const rawOutput =
        response.output_text?.trim();

      if (!rawOutput) {
        throw new Error(
          "OpenAI returned empty content"
        );
      }

      return validateMcqOutput(rawOutput);
    } catch (error) {
      lastError = error;

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
// PARSE AND VALIDATE JSON
// ─────────────────────────────────────────────

function cleanJsonOutput(rawOutput) {
  return rawOutput
    .replace(/^\s*```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();
}

function validateMcqOutput(rawOutput) {
  const cleaned = cleanJsonOutput(rawOutput);

  let parsed;

  try {
    parsed = JSON.parse(cleaned);
  } catch (error) {
    throw new Error(
      `Invalid JSON returned: ${error.message}`
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

  if (!Array.isArray(parsed.mcqs)) {
    throw new Error(
      'Output must contain an "mcqs" array'
    );
  }

  if (parsed.mcqs.length !== 10) {
    throw new Error(
      `Expected exactly 10 MCQs but received ${parsed.mcqs.length}`
    );
  }

  parsed.mcqs.forEach((mcq, index) => {
    validateOneMcq(mcq, index + 1);
  });

  return parsed;
}

function validateOneMcq(mcq, number) {
  if (
    !mcq ||
    typeof mcq !== "object" ||
    Array.isArray(mcq)
  ) {
    throw new Error(
      `MCQ ${number} is not an object`
    );
  }

  const requiredStrings = [
    "Stem",
    "A",
    "B",
    "C",
    "D",
    "Correct Answer"
  ];

  for (const field of requiredStrings) {
    if (
      typeof mcq[field] !== "string" ||
      !mcq[field].trim()
    ) {
      throw new Error(
        `MCQ ${number} is missing ${field}`
      );
    }

    mcq[field] = mcq[field].trim();
  }

  mcq["Correct Answer"] =
    mcq["Correct Answer"].toUpperCase();

  if (
    !["A", "B", "C", "D"].includes(
      mcq["Correct Answer"]
    )
  ) {
    throw new Error(
      `MCQ ${number} has invalid Correct Answer`
    );
  }

  const explanation = mcq.Explanation;

  if (
    !explanation ||
    typeof explanation !== "object" ||
    Array.isArray(explanation)
  ) {
    throw new Error(
      `MCQ ${number} has no valid Explanation`
    );
  }

  if (
    typeof explanation[
      "Correct Answer Summary"
    ] !== "string" ||
    !explanation[
      "Correct Answer Summary"
    ].trim()
  ) {
    throw new Error(
      `MCQ ${number} has no Correct Answer Summary`
    );
  }

  if (
    !Array.isArray(
      explanation["Diagnostic Pathway"]
    ) ||
    explanation[
      "Diagnostic Pathway"
    ].length === 0
  ) {
    throw new Error(
      `MCQ ${number} has no Diagnostic Pathway`
    );
  }

  const optionFailures =
    explanation["Why the Other Options Fail"];

  if (
    !optionFailures ||
    typeof optionFailures !== "object" ||
    Array.isArray(optionFailures)
  ) {
    throw new Error(
      `MCQ ${number} has no option explanations`
    );
  }

  if (
    typeof explanation["Examiner's Trap"] !==
      "string" ||
    !explanation["Examiner's Trap"].trim()
  ) {
    throw new Error(
      `MCQ ${number} has no Examiner's Trap`
    );
  }

  validateWordCount(
    mcq.Stem,
    30,
    40,
    `MCQ ${number} Stem`
  );

  for (const option of ["A", "B", "C", "D"]) {
    validateWordCount(
      mcq[option],
      2,
      5,
      `MCQ ${number} option ${option}`
    );
  }
}

function validateWordCount(
  text,
  minimum,
  maximum,
  label
) {
  const words = String(text)
    .trim()
    .split(/\s+/)
    .filter(Boolean);

  if (
    words.length < minimum ||
    words.length > maximum
  ) {
    throw new Error(
      `${label} has ${words.length} words; expected ${minimum}–${maximum}`
    );
  }
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

  const { data: availableRows, error: selectError } =
    await supabase
      .from(TABLE)
      .select(
        [
          "id",
          "subject",
          "serial_number",
          "topic",
          "number_of_times_asked",
          "jsonb_output"
        ].join(",")
      )
      .not(INPUT_COL, "is", null)
      .is(OUTPUT_COL, null)
      .eq(LOCK_COL, false)
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

  const { data: lockedRows, error: lockError } =
    await supabase
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
          "jsonb_output"
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

async function saveSuccess(rowId, mcqJson) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: mcqJson,
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null
    })
    .eq("id", rowId)
    .eq(LOCK_COL, true)
    .is(OUTPUT_COL, null)
    .select("id");

  if (error) {
    throw new Error(
      `Failed to save MCQs: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "MCQs were not saved because the row lock changed"
    );
  }
}

// ─────────────────────────────────────────────
// RELEASE FAILED ROW
// ─────────────────────────────────────────────

async function releaseFailedRow(rowId) {
  const { error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null
    })
    .eq("id", rowId)
    .eq(LOCK_COL, true)
    .is(OUTPUT_COL, null);

  if (error) {
    console.error(
      `❌ Failed to unlock ${rowId}:`,
      error.message
    );
  }
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────

async function processRow(row) {
  console.log(
    `🧠 Generating | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const mcqJson =
      await callOpenAI(row);

    await saveSuccess(
      row.id,
      mcqJson
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | ${row.topic}`
    );
  } catch (error) {
    console.error(
      `❌ Failed | ${row.subject} | ${row.serial_number} | ${row.topic}:`,
      error?.message || error
    );

    await releaseFailedRow(row.id);
  }
}

// ─────────────────────────────────────────────
// PROCESS IN BATCHES
// ─────────────────────────────────────────────

async function processRowsInBatches(rows) {
  for (
    let index = 0;
    index < rows.length;
    index += BATCH_SIZE
  ) {
    const batch = rows.slice(
      index,
      index + BATCH_SIZE
    );

    await Promise.allSettled(
      batch.map((row) =>
        processRow(row)
      )
    );
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────

async function main() {
  console.log(
    `🚀 NEETPG MCQ WORKER STARTED: ${WORKER_ID}`
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
      console.error(
        "❌ Worker loop error:",
        error?.message || error
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
