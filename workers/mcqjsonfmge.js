require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL =
  process.env.FMGE_MCQ_MODEL || "gpt-5-mini";

const LIMIT = parseInt(
  process.env.FMGE_MCQ_LIMIT || "10",
  10
);

const BATCH_SIZE = parseInt(
  process.env.FMGE_MCQ_BATCH_SIZE || "5",
  10
);

const SLEEP_MS = parseInt(
  process.env.FMGE_MCQ_LOOP_SLEEP_MS || "500",
  10
);

const LOCK_TTL_MIN = parseInt(
  process.env.FMGE_MCQ_LOCK_TTL_MIN || "30",
  10
);

const WORKER_ID =
  process.env.WORKER_ID ||
  `fmge-mcq-worker-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 6)}`;

const TABLE = "fmge_mock_question_bank";

const INPUT_COL = "question_concept";
const OUTPUT_COL = "mcq_json";
const LOCK_COL = "mcq_generation_lock";
const LOCK_AT = "mcq_generation_locked_at";

console.log("🚀 FMGE MOCK QUESTION WORKER STARTED:", WORKER_ID);

// ─────────────────────────────────────────────
// PROMPT — USED AS PROVIDED
// ─────────────────────────────────────────────
function buildPrompt(questionConcept) {
  return `
You are an expert medical MCQ writer for USMLE Step 1/2CK, NEET-PG, and FMGE. Convert each supplied PYT/concept into ONE medically accurate, exam-standard clinical vignette MCQ.

STEM

Write a 30–40 word clinical stem requiring 2–3 levels of reasoning: infer the condition → recognize the relevant complication/anatomy/physiology → answer the tested concept.

Never reveal the answer pathway. Do not explicitly name the target diagnosis, structure, gene, enzyme, vessel, pathway, biochemical state, or mechanism being tested. Show findings; make the student infer them.

Use a logical clinical sequence where applicable:
context/risk → presentation → relevant examination/vitals → relevant investigations/intervention → lead-in.

Include exactly one plausible red herring only when it genuinely competes with the correct diagnosis.

Discriminatory Value Rule

Every stem detail must do at least one:

Support the correct answer.
Weaken a distractor.
Establish necessary timing/severity/context.
Serve as the intentional red herring.

Delete everything else. Never add decorative demographics, history, normal findings, routine vitals, or irrelevant tests merely to make the vignette realistic.

Use raw values with reference ranges instead of labels such as “anemia,” “hyperkalemia,” or “leukocytosis.” Include labs/vitals/imaging only when relevant to solving the question. Never invent irrelevant data to satisfy formatting.

Vitals must physiologically match the clinical state.

The final lead-in must be neutral and contain no diagnostic or mechanistic hint.

OPTIONS

Provide exactly four competitive options (A–D), each 2–5 words.

Options must be:

grammatically and structurally parallel,
similar in specificity,
medically plausible,
mutually distinct,
from the same conceptual category.

Avoid giveaway opposites, obviously unrelated distractors, or one option that differs conspicuously in length/structure.

ACCURACY

Medical, anatomical, embryological, pharmacological, and biochemical facts must be textbook-accurate. Management questions must follow current accepted guidelines. Do not oversimplify anatomical boundaries or mechanisms.

EXPLANATION

Explain:

Correct Answer Summary: answer + core reason.
Diagnostic Pathway: concise stepwise reasoning from clues to answer.
Why Other Options Fail: individually explain B/C/D or whichever are incorrect.
Examiner's Trap: identify the intended misconception/buzzword trap.

Do not merely restate the answer.

FINAL QUALITY CHECK

Before output, verify:

Stem = 30–40 words.
Options = 2–5 words each.
No answer giveaway.
Every stem detail has discriminatory value.
Exactly one best answer.
No medically incorrect distractor logic.
No unnecessary information.
OUTPUT

Return ONLY valid JSON:

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

INPUT QUESTION CONCEPT:

${questionConcept}
`;
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

function isRetryable(error) {
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(
    String(error?.message || error)
  );
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const response = await openai.chat.completions.create({
      model: MODEL,
      messages: [
        {
          role: "user",
          content: prompt
        }
      ]
    });

    const output =
      response.choices?.[0]?.message?.content?.trim();

    if (!output) {
      throw new Error("OpenAI returned an empty response");
    }

    return output;
  } catch (error) {
    if (isRetryable(error) && attempt <= 2) {
      const delay = 1000 * attempt;

      console.warn(
        `⚠️ OpenAI retry ${attempt}/2 after ${delay} ms`
      );

      await sleep(delay);
      return callOpenAI(prompt, attempt + 1);
    }

    throw error;
  }
}

// ─────────────────────────────────────────────
// EXTRACT JSON AND EXPLANATION
// ─────────────────────────────────────────────
function parseGeneratedOutput(rawOutput) {
  let cleaned = rawOutput.trim();

  // Remove opening Markdown fence when present
  cleaned = cleaned.replace(
    /^\s*```(?:json)?\s*/i,
    ""
  );

  const firstBrace = cleaned.indexOf("{");

  if (firstBrace === -1) {
    throw new Error("No JSON object found in model output");
  }

  let depth = 0;
  let inString = false;
  let escaped = false;
  let closingBrace = -1;

  for (let i = firstBrace; i < cleaned.length; i += 1) {
    const character = cleaned[i];

    if (escaped) {
      escaped = false;
      continue;
    }

    if (character === "\\" && inString) {
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
          closingBrace = i;
          break;
        }
      }
    }
  }

  if (closingBrace === -1) {
    throw new Error("JSON object is incomplete");
  }

  const jsonText = cleaned.slice(
    firstBrace,
    closingBrace + 1
  );

  const parsed = JSON.parse(jsonText);

  let explanation = cleaned
    .slice(closingBrace + 1)
    .replace(/^\s*```\s*/i, "")
    .replace(/^\s*(?:svg\s*)?/i, "")
    .replace(/^\s*Explanation\s*:\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();

  const stem = String(
    parsed.Stem ?? parsed.stem ?? ""
  ).trim();

  const optionA = String(
    parsed.A ?? parsed.a ?? ""
  ).trim();

  const optionB = String(
    parsed.B ?? parsed.b ?? ""
  ).trim();

  const optionC = String(
    parsed.C ?? parsed.c ?? ""
  ).trim();

  const optionD = String(
    parsed.D ?? parsed.d ?? ""
  ).trim();

  const correctAnswer = String(
    parsed["Correct Answer"] ??
    parsed.correct_answer ??
    parsed.correctAnswer ??
    ""
  )
    .trim()
    .toUpperCase();

  if (!stem) {
    throw new Error("Generated JSON is missing Stem");
  }

  if (!optionA || !optionB || !optionC || !optionD) {
    throw new Error("Generated JSON is missing one or more options");
  }

  if (!["A", "B", "C", "D"].includes(correctAnswer)) {
    throw new Error(
      `Invalid correct answer: ${correctAnswer || "missing"}`
    );
  }

  if (!explanation) {
    throw new Error("Generated explanation is missing");
  }

  /*
   * Store both display-friendly keys and correct_answer.
   *
   * correct_answer is included because your database CHECK
   * constraint validates this key.
   */
  return {
    Stem: stem,
    A: optionA,
    B: optionB,
    C: optionC,
    D: optionD,
    "Correct Answer": correctAnswer,
    correct_answer: correctAnswer,
    Explanation: explanation
  };
}

// ─────────────────────────────────────────────
// RELEASE EXPIRED LOCKS
// ─────────────────────────────────────────────
async function releaseExpiredLocks() {
  const cutoff = new Date(
    Date.now() - LOCK_TTL_MIN * 60 * 1000
  ).toISOString();

  const { error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: false,
      [LOCK_AT]: null,
      generation_status: "pending",
      updated_at: new Date().toISOString()
    })
    .eq(LOCK_COL, true)
    .lt(LOCK_AT, cutoff)
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
        "id, subject, pyt, question_concept, concept_number"
      )
      .not(INPUT_COL, "is", null)
      .is(OUTPUT_COL, null)
      .eq(LOCK_COL, false)
      .eq("generation_status", "pending")
      .eq("active", true)
      .order("created_at", { ascending: true })
      .limit(limit);

  if (selectError) {
    throw new Error(
      `Failed to select rows: ${selectError.message}`
    );
  }

  if (!availableRows?.length) {
    return [];
  }

  const ids = availableRows.map((row) => row.id);
  const lockedAt = new Date().toISOString();

  const { data: lockedRows, error: lockError } =
    await supabase
      .from(TABLE)
      .update({
        [LOCK_COL]: true,
        [LOCK_AT]: lockedAt,
        generation_status: "generating",
        updated_at: lockedAt
      })
      .in("id", ids)
      .eq(LOCK_COL, false)
      .eq("generation_status", "pending")
      .is(OUTPUT_COL, null)
      .eq("active", true)
      .select(
        "id, subject, pyt, question_concept, concept_number"
      );

  if (lockError) {
    throw new Error(
      `Failed to lock rows: ${lockError.message}`
    );
  }

  return lockedRows || [];
}

// ─────────────────────────────────────────────
// MARK ROW AS FAILED
// ─────────────────────────────────────────────
async function markRowFailed(rowId) {
  const { error } = await supabase
    .from(TABLE)
    .update({
      generation_status: "failed",
      [LOCK_COL]: false,
      [LOCK_AT]: null,
      updated_at: new Date().toISOString()
    })
    .eq("id", rowId)
    .eq(LOCK_COL, true);

  if (error) {
    console.error(
      `❌ Failed to mark row ${rowId} as failed:`,
      error.message
    );
  }
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  try {
    const rawOutput = await callOpenAI(
      buildPrompt(row.question_concept)
    );

    const mcqJson = parseGeneratedOutput(rawOutput);

    const { data: savedRows, error: updateError } =
      await supabase
        .from(TABLE)
        .update({
          [OUTPUT_COL]: mcqJson,
          generation_status: "ready",
          [LOCK_COL]: false,
          [LOCK_AT]: null,
          updated_at: new Date().toISOString()
        })
        .eq("id", row.id)
        .eq(LOCK_COL, true)
        .eq("generation_status", "generating")
        .is(OUTPUT_COL, null)
        .select("id");

    if (updateError) {
      throw new Error(
        `Failed to save MCQ: ${updateError.message}`
      );
    }

    if (!savedRows?.length) {
      throw new Error(
        "Row was not saved because its lock or status changed"
      );
    }

    console.log(
      `✅ MCQ generated | ${row.subject} | Concept ${row.concept_number} | ${row.id}`
    );
  } catch (error) {
    console.error(
      `❌ MCQ failed | ${row.id}:`,
      error?.message || error
    );

    await markRowFailed(row.id);
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
    const batch = rows.slice(
      index,
      index + BATCH_SIZE
    );

    await Promise.allSettled(
      batch.map((row) => processRow(row))
    );
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(
    `🧠 FMGE MOCK QUESTION WORKER RUNNING | ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model: ${MODEL} | Pickup: ${LIMIT} | Concurrent: ${BATCH_SIZE}`
  );

  while (true) {
    try {
      const rows = await claimRows(LIMIT);

      if (!rows.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`📥 Claimed ${rows.length} row(s)`);

      await processRowsInBatches(rows);
    } catch (error) {
      console.error(
        "❌ Worker loop error:",
        error?.message || error
      );

      await sleep(Math.max(SLEEP_MS, 2000));
    }
  }
})();
