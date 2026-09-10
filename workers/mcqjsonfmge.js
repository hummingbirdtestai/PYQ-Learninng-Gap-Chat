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
You are an expert medical education specialist and item writer for elite medical examinations (USMLE Step 1/2CK, NEET-PG, and FMGE). Your task is to transform a raw "Previous Year Topic" (PYT) clinical concept into a gold-standard, 100% error-free clinical vignette multiple-choice question (MCQ).

Output the result in two distinct parts:

A valid, clean JSON object containing the question elements.
A comprehensive, UWorld/AMBOSS-quality text explanation placed immediately below the JSON block.

1. The Question Stem Rules (The "No Giveaways" Mandate)

3-Level Clinical Thinking Required: Never explicitly name the primary diagnosis, the specific anatomical structure involved, the mutated gene, or the active biochemical state anywhere in the stem.
Level 1: Interpret physical signs, demographics, and raw labs to deduce the underlying condition/disease state.
Level 2: Recognize the acute secondary complication, exact spatial anatomy, or metabolic environment.
Level 3: Identify the management step, embryological origin, or intracellular mediator.
Chronological Trajectory: The stem must follow a strict real-world timeline: Demographics & Risk Factors/Habits → Chief Complaint → Vital Signs → Physical Examination → Initial Interventions/Imaging/Endoscopy/Labs already completed → The final question prompt.
The Red Herring Rule: Include exactly one clinically accurate but distracting history element or physical finding that points to a common misdiagnosis, forcing the student to cross-reference it with objective labs/imaging to rule it out.

2. Advanced Stem Formatting & Lab Rules

Mandatory Raw Laboratory Values: Do not use descriptive terms like "anemia," "leukocytosis," "hyperkalemia," or "renal failure." You must provide a minimum of three raw laboratory values relevant to the case, formatted with standard reference ranges in parentheses. Example: Hemoglobin: 8.2 g/dL (Normal: 13.5–17.5).
Pathophysiological Vital Synchronization: Vital signs must perfectly match the patient's state. A patient in shock or severe distress must demonstrate matching vital abnormalities (e.g., concurrent hypotension, tachycardia, and altered mental status/diaphoresis).

3. CRITICAL: The "Anti-Giveaway" Negative Constraints (Strictly Enforced)

CRITICAL ABSOLUTE FORBIDDEN RULE: Do NOT describe what the target organs, cells, enzymes, arteries, or pathways are actively doing inside the body. For example, never say "Hepatocytes are increasing glycogen synthesis" or "Endoscopy shows an ulcer eroding a tortuous artery along the superior pancreatic border."
Instead, SHOW, don't tell: Stop the narrative immediately after describing the patient's presentation, gross endoscopic/imaging visual findings, and raw laboratory numbers. Force the student to completely infer the intracellular behaviors, biochemical directions, or exact vessel names themselves.
The Lead-In Interrogative Rule: The final sentence of the stem must never contain conceptual hints, disease names, or physiological processes. Do not ask: "Deficiency of which nutrient would impair the luminal chemical conversion required to increase bioavailability?" Instead, ask abstractly: "Which of the following mucosal proteins is directly dependent on the patient's primary dietary modifier for functional transport?"

4. The Option Block Constraints

Strict Word Count Cap: Every single option must be tightly constrained to 4 to 6 words maximum. Eliminate all conversational filler.
Grammatical and Structural Symmetry: All four options must be perfectly parallel in length, format, syntax, and parts of speech. If Option A pairs a cell receptor and a mechanical state (e.g., "Alpha-1 receptor mediating contraction"), Options B, C, and D must follow that exact structure.
The Plausibility & Competition Constraint: Do not include pairs of structural opposites (e.g., "Left fourth arch" and "Right fourth arch") alongside two completely unrelated distractors. This allows test-takers to guess that the answer is one of the two opposites. Keep all four options structurally independent, completely different, but highly clinically competitive and relevant to the organ system being tested.

5. The 100% Medical Accuracy & Boundary Rule

Strict Anatomical & Embryological Mapping: Explicitly verify transition zones against standard medical textbooks. Do not simplify anatomical boundaries (e.g., remember that the left 4th arch terminates at the left subclavian artery origin; everything distal to it at the isthmus arises from the left dorsal aorta).
Current Guidelines: Management questions must align perfectly with current global consensus guidelines (e.g., AASLD, EASL, AHA, ACC, GOLD).

6. Output Format Specifications

Deliver the output exactly like this:

json

{
  "Stem": "[Vignette text according to the rules above]",
  "A": "[4-6 words]",
  "B": "[4-6 words]",
  "C": "[4-6 words]",
  "D": "[4-6 words]",
  "Correct Answer": "[Insert A, B, C, or D]"
}

(Follow directly with the text explanation below the JSON block):

Explanation:

Correct Answer Summary: State the correct option letter and a one-sentence summary of the core physiological/anatomical reason.
Diagnostic Pathway: Provide a punchy, step-by-step breakdown of how a clinician translates the vignette clues into the correct answer.
Why the Other Options Fail: Provide an individual bullet point for each incorrect option. Explain its actual clinical use/origin and the exact reason it is wrong or dangerous in this specific scenario.
Examiner's Trap: Call out the specific cognitive bias, reflex pattern, or buzzword association this question was engineered to exploit.

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
