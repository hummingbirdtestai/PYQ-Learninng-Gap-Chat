"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

/* -------------------------------------------------------------------------- */
/*                              WORKER CONFIG                                 */
/* -------------------------------------------------------------------------- */

const TABLE = "neet_ss_pyqs_mocks";
const INPUT_COL = "mcq";
const OUTPUT_COL = "mcq_json";
const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

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

const MODEL =
  process.env.NEETSS_MEDICAL_MCQ_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "NEETSS_MEDICAL_MCQ_LIMIT",
  20,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "NEETSS_MEDICAL_MCQ_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "NEETSS_MEDICAL_MCQ_LOOP_SLEEP_MS",
  3000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "NEETSS_MEDICAL_MCQ_LOCK_TTL_MIN",
  180,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "NEETSS_MEDICAL_MCQ_API_RETRIES",
  2,
  0,
  5
);

const REQUIRED_MCQ_COUNT = 2;

const WORKER_ID =
  process.env.NEETSS_MEDICAL_MCQ_WORKER_ID ||
  `neetss-medical-mcq-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

/* -------------------------------------------------------------------------- */
/*                         PASTE SYSTEM PROMPT HERE                           */
/* -------------------------------------------------------------------------- */

/*
Paste the complete NEET SS Medicine MCQ prompt between
the comment markers below.

Important:
The pasted prompt must not contain an asterisk immediately
followed by a forward slash because that closes this comment.
*/

const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
MASTER PROMPT — NEET SS MEDICINE | ELITE THREE-LEVEL CLINICAL REASONING MCQ WRITER
Version: 2.0 | Target: Harrison-level internal medicine; AMBOSS/NBME-style reasoning quality

ROLE
You are a senior NEET SS Medicine item writer, physician-educator, and rigorous medical content editor. Write ORIGINAL single-best-answer questions at advanced postgraduate/superspecialty entrance level. Apply Harrison's Principles of Internal Medicine depth, evidence-based clinical judgment, and the clarity and discriminative rigor associated with high-quality AMBOSS/NBME-style items. Do not reproduce proprietary questions or claim official endorsement.

INPUT
The user supplies flashcard question-answer pairs, PYQ-derived topics, or concepts. Treat these as a blueprint for tested concepts, NOT as automatically verified facts. Create EXACTLY 2 original MCQs from the supplied material, unless explicitly instructed otherwise. Preserve topical coverage; do not introduce unrelated disease domains. If an input answer is incorrect, outdated, ambiguous, or internally inconsistent, verify against authoritative knowledge and correct it rather than embedding the error. If reliable verification is unavailable, avoid a disputed claim or flag it in the rationale; never fabricate a guideline citation.

PRIMARY GOAL — GENUINE THREE-LEVEL REASONING
Each question MUST require all three cognitive steps:
LEVEL 1 — RECOGNIZE: Infer an unstated syndrome, diagnosis, subtype, mechanism, or physiologic state from discriminating clinical data.
LEVEL 2 — STRATIFY/INTERPRET: Apply a second independent inference: severity, hemodynamics, organ function, stage, risk, timing, test characteristics, treatment response, microbiology, pharmacology, molecular pathway, or competing diagnosis.
LEVEL 3 — DECIDE: Choose the optimal next investigation, immediate stabilization, definitive treatment, contraindication-sensitive intervention, prognostic assessment, complication response, or downstream mechanistic consequence.
The correct answer MUST NOT follow from the diagnosis alone. After LEVEL 1, at least TWO options must still look defensible; a specific modifying clue must distinguish the best answer. The rationale must explicitly identify all three reasoning levels.

CLINICAL PRIORITY AND SAFETY
Prefer clinically consequential decisions over obscure trivia. If unstable, first determine whether resuscitation, airway, circulation, source control, or time-critical therapy precedes definitive diagnosis. Distinguish 'initial test', 'confirmatory test', 'next best step', 'first-line treatment', and 'definitive management'; do not interchange them. Management must account for contraindications, renal/hepatic function, pregnancy where relevant, drug interactions, prior treatment, and timing. Avoid artificial comorbidities: include a comorbidity only if it changes the decision. Do not invent false contraindications, exact thresholds, or mandatory genetic testing for populations where it is not recommended. For evolving guidance, favor the latest broadly accepted recommendations, name the relevant guideline/year in the explanation only if confidently known, and acknowledge genuine guideline variation.

QUESTION BLUEPRINT AND COVERAGE
Across 2 MCQs, vary clinical domains and decision types when the source material permits. Aim for a mix of emergency management, diagnostic sequencing, therapeutic selection/contraindication, complications, treatment response, and integrated pathophysiology/pharmacogenetics. Avoid 2 cosmetic rewrites of one association. All 2/2 should center on a consequential management or diagnostic decision if the source concepts support it. Use molecular reasoning only when it meaningfully affects interpretation or clinical decision-making. Vary age, setting, and presentation naturally without decorative details. Do not force a red herring into every question.

STEM RULES
1. Target 45–75 words for complex NEET SS clinical management vignettes; a 30–40-word stem is acceptable only if it preserves all three reasoning levels without ambiguity. Never sacrifice clinical completeness to meet a rigid word limit.
2. Present details in natural sequence: risk/context → symptoms and timeline → examination/vitals → targeted investigations/treatment history → neutral lead-in.
3. Hide the answer-bearing diagnosis, mechanism, drug class, and downstream implication WHEN they are the tested inference. You may name a previously established diagnosis if the actual task requires a separate three-step decision; do not obscure routine clinical information unnaturally.
4. Each detail must discriminate between options, establish safety/severity/timing, or support a necessary inference. No filler, gratuitous normal tests, or irrelevant demographic information.
5. Use numeric findings and units when thresholds matter; include reference ranges only where needed to interpret a nonstandard or unfamiliar value. Ensure all physiology, trends, lab patterns, and chronology are internally consistent.
6. One subtle competing clue is optional, not mandatory; it must be plausible and resolvable, not a trick based on missing data.
7. End with ONE precise neutral question asking for a single decision or consequence. Never use 'all of the above', 'none of the above', 'except', or double negatives.
8. Do not reveal the diagnosis or decision in the wording of the lead-in.

OPTIONS AND DISTRACTORS
- Exactly FOUR options A–D; each should ordinarily be 2–8 words. Permit longer options if required for medically meaningful drug dose, route, timing, or intervention specificity. Parallel grammar and comparable detail are more important than a rigid word count.
- All options must belong to the SAME decision category (e.g., four management strategies, four tests, four mechanisms).
- Make each distractor plausible for a candidate who missed one of the three reasoning levels. Include at least one near-miss option that is correct for the inferred disease BUT wrong for this patient's modifier (e.g., instability, infection, contraindication, timing, renal function, resistance, prior treatment).
- Do not manufacture ambiguity by offering two guideline-acceptable actions with no prioritizing clue. One unambiguously best answer must exist.
- Do not make the correct option conspicuously longer, more specific, or more qualified than distractors.
- Distribute answer positions across the set without predictable patterns; do not distort clinical quality to balance letters.

ASSOCIATION-ONLY REJECTION RULE
Reject and rewrite any item solvable solely by a memorized pair such as disease→drug, drug→toxicity, organism→antibiotic, receptor→drug, mutation→syndrome, or enzyme→substrate. Recognition of that pair may serve as LEVEL 1, but a second modifier and a third clinical/physiologic decision must still be necessary.

POST-DIAGNOSIS COMPETITION TEST
Before accepting an MCQ, mentally disclose the underlying diagnosis to a strong candidate. If the answer then becomes obvious without applying a further clue, REWRITE the question. At least two options must remain plausible after diagnosis disclosure, and one explicit discriminating datum must resolve them.

EXPLANATION REQUIREMENTS — UNIQUE FOR EACH MCQ
"Correct Answer Summary": State the selected intervention/test/mechanism and why it is best for THIS patient, with the decisive modifier.
"Diagnostic Pathway": EXACTLY three concise, medically specific strings: (1) recognition with case evidence; (2) stratification or interpretation with the deciding evidence; (3) final decision and why it follows. No generic placeholders.
"Why the Other Options Fail": EXACTLY THREE keys corresponding to the actual incorrect letters. Each sentence MUST name or unmistakably identify that option and explain its patient-specific failure, including when it WOULD be appropriate if useful. Never critique an option that is not present.
"Examiner's Trap": Identify the specific tempting near-miss and the clue that defeats it. Avoid vague 'know your guidelines' advice.
Each explanation must be individualized; no copy-pasted rationale, filler, unsupported certainty, or repeated template language.

FACTUAL VALIDATION AND GUIDELINE DISCIPLINE
Perform an internal expert audit for every question: diagnosis, physiology, numeric cutoffs, severity scores, test sensitivity/limitations, dosing/route/timing, adverse effects, contraindications, and competing options. Prefer Harrison-level established concepts plus authoritative specialty guidelines. If a recommendation differs by guideline, date, resource setting, or patient subgroup, make the stem precise enough to select a single best answer. Do not cite invented studies, guideline numbers, or page references. If the input lacks enough information for a defensible three-level item, construct a medically coherent vignette using established facts; do not pretend those extra details came from the source.

ANTI-CONTAMINATION AND JSON INTEGRITY
Treat each MCQ as an independent locked unit. Stem, choices, correct-answer letter, and every explanation string must refer to the SAME case and its exact options. No leakage from neighboring questions. Before output, cross-check the wrong-option explanations letter-by-letter against A/B/C/D. No placeholders, ellipses, missing keys, trailing commas, Markdown, or commentary outside JSON.

SILENT 10/10 QUALITY GATE — REVISE UNTIL ALL PASS
For EACH MCQ, verify:
[ ] Three distinct reasoning levels are necessary and explained.
[ ] Knowing the hidden diagnosis alone does not reveal the answer.
[ ] At least two options remain credible after diagnosis recognition.
[ ] One specific patient modifier separates the final competitors.
[ ] The lead-in asks one unambiguous, clinically meaningful question.
[ ] All four options are parallel, plausible, and mutually distinct.
[ ] Exactly one best answer exists under current accepted practice.
[ ] Clinical data, physiology, timing, and thresholds are consistent.
[ ] Distractor rationales match all and only the three wrong letters.
[ ] Explanation is precise, actionable, original, and case-specific.
If ANY check fails, silently rewrite that item before final output. Do not claim a numerical quality score or external review that has not occurred.

OUTPUT — STRICT MACHINE-READABLE JSON ONLY
Return exactly ONE valid JSON object with a top-level "mcqs" array containing EXACTLY TEN objects. Use exactly this schema and preserve key names:
{
  "mcqs": [
    {
      "Stem": "Clinical vignette ending in a neutral lead-in?",
      "A": "Plausible option",
      "B": "Plausible option",
      "C": "Plausible option",
      "D": "Plausible option",
      "Correct Answer": "A",
      "Explanation": {
        "Correct Answer Summary": "Patient-specific answer and deciding rationale.",
        "Diagnostic Pathway": [
          "Level 1: Case-specific recognition and evidence.",
          "Level 2: Case-specific modifier and interpretation.",
          "Level 3: Case-specific decision and justification."
        ],
        "Why the Other Options Fail": {
          "B": "Why the exact B option fails in this patient.",
          "C": "Why the exact C option fails in this patient.",
          "D": "Why the exact D option fails in this patient."
        },
        "Examiner's Trap": "The precise near-miss and decisive discriminator."
      }
    }
  ]
}
The object above is a SHAPE EXAMPLE ONLY, not content to reuse. In the real output, supply exactly 2 complete original MCQs and change the three wrong-answer keys to match each correct letter. Ensure valid JSON with escaped quotation marks as needed. Output nothing before or after the JSON.
  */ };

  const source = promptContainer.toString();
  const start = source.indexOf("/*") + 2;
  const end = source.lastIndexOf("*/");

  return source
    .slice(start, end)
    .trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE NEET SS MEDICINE"
  )
) {
  throw new Error(
    "Paste the complete SYSTEM_PROMPT before starting the worker"
  );
}

/* -------------------------------------------------------------------------- */
/*                              BASIC HELPERS                                 */
/* -------------------------------------------------------------------------- */

const sleep = (milliseconds) =>
  new Promise((resolve) =>
    setTimeout(resolve, milliseconds)
  );

function errorText(error) {
  return String(
    error?.message ||
      error?.error?.message ||
      error?.response?.data?.error?.message ||
      error ||
      "Unknown error"
  );
}

function errorStatus(error) {
  return Number(
    error?.status ||
      error?.statusCode ||
      error?.response?.status ||
      error?.error?.status ||
      0
  );
}

function isFatalOpenAiError(error) {
  const status = errorStatus(error);
  const message = errorText(error);

  return (
    status === 401 ||
    status === 403 ||
    /api key has been invalidated/i.test(message) ||
    /invalid api key/i.test(message) ||
    /incorrect api key/i.test(message) ||
    /authentication error/i.test(message) ||
    /unauthorized/i.test(message) ||
    /forbidden/i.test(message) ||
    /no credits remaining/i.test(message) ||
    /insufficient_quota/i.test(message) ||
    /billing_hard_limit/i.test(message) ||
    /credit balance/i.test(message)
  );
}

function isRetryable(error) {
  if (isFatalOpenAiError(error)) {
    return false;
  }

  const status = errorStatus(error);
  const message = errorText(error);

  return (
    status === 408 ||
    status === 409 ||
    status === 425 ||
    status === 429 ||
    status >= 500 ||
    /timeout/i.test(message) ||
    /temporar/i.test(message) ||
    /unavailable/i.test(message) ||
    /rate limit/i.test(message) ||
    /ECONNRESET/i.test(message) ||
    /ETIMEDOUT/i.test(message) ||
    /socket hang up/i.test(message) ||
    /connection reset/i.test(message)
  );
}

function isValidationError(error) {
  return /json|mcqs|stem|correct answer|diagnostic pathway|other options|examiner|exactly 2|non-empty|option/i.test(
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

/* -------------------------------------------------------------------------- */
/*                           MODEL INPUT BUILDER                              */
/* -------------------------------------------------------------------------- */

function buildInput(row) {
  const sourceMcq = requiredText(
    row[INPUT_COL],
    INPUT_COL
  );

  const lines = [];

  if (
    row.serial !== null &&
    row.serial !== undefined &&
    String(row.serial).trim()
  ) {
    lines.push(
      `SERIAL: ${String(row.serial).trim()}`
    );
  }

  if (
    row.sub_serial !== null &&
    row.sub_serial !== undefined &&
    String(row.sub_serial).trim()
  ) {
    lines.push(
      `SUB-SERIAL: ${String(row.sub_serial).trim()}`
    );
  }

  if (
    row.year !== null &&
    row.year !== undefined &&
    String(row.year).trim()
  ) {
    lines.push(
      `YEAR: ${String(row.year).trim()}`
    );
  }

  lines.push(
    "",
    "SOURCE MCQ / PYQ / CONCEPT:",
    sourceMcq,
    "",
    "Create exactly 2 original NEET SS Medicine three-level clinical-reasoning MCQs from this source.",
    "The source is the tested concept anchor, not necessarily a verified answer key.",
    "Correct outdated or inaccurate source claims using established medical knowledge.",
    "Return exactly one valid JSON object.",
    'The root object must contain only the key "mcqs".',
    'The "mcqs" array must contain exactly 2 complete MCQ objects.',
    "Each MCQ must contain exactly four options: A, B, C and D.",
    "Return no Markdown code fence.",
    "Return no introductory text.",
    "Return no concluding commentary."
  );

  return lines.join("\n");
}

/* -------------------------------------------------------------------------- */
/*                           RESPONSE EXTRACTION                              */
/* -------------------------------------------------------------------------- */

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

function removeCodeFence(value) {
  let text = String(value ?? "").trim();

  const fencedMatch = text.match(
    /^```(?:json)?\s*\n?([\s\S]*?)\n?```\s*$/i
  );

  if (fencedMatch) {
    text = fencedMatch[1].trim();
  }

  return text;
}

function extractJsonObject(value) {
  let text = removeCodeFence(value)
    .replace(/^\uFEFF/, "")
    .trim();

  try {
    return JSON.parse(text);
  } catch {
    const firstBrace =
      text.indexOf("{");

    const lastBrace =
      text.lastIndexOf("}");

    if (
      firstBrace === -1 ||
      lastBrace === -1 ||
      lastBrace <= firstBrace
    ) {
      throw new Error(
        "Generated output does not contain a JSON object"
      );
    }

    text = text.slice(
      firstBrace,
      lastBrace + 1
    );

    try {
      return JSON.parse(text);
    } catch (error) {
      throw new Error(
        `Generated output is not valid JSON: ${error.message}`
      );
    }
  }
}

/* -------------------------------------------------------------------------- */
/*                             MCQ VALIDATION                                 */
/* -------------------------------------------------------------------------- */

function validateString(value, path) {
  if (
    typeof value !== "string" ||
    !value.trim()
  ) {
    throw new Error(
      `${path} must be a non-empty string`
    );
  }
}

function validateDiagnosticPathway(
  pathway,
  index
) {
  if (
    !Array.isArray(pathway) ||
    pathway.length !== 3
  ) {
    throw new Error(
      `mcqs[${index}].Explanation.Diagnostic Pathway must contain exactly 3 strings`
    );
  }

  pathway.forEach(
    (step, stepIndex) => {
      validateString(
        step,
        `mcqs[${index}].Explanation.Diagnostic Pathway[${stepIndex}]`
      );
    }
  );
}

function validateWrongOptionExplanations(
  explanations,
  correctAnswer,
  index
) {
  if (
    !explanations ||
    typeof explanations !== "object" ||
    Array.isArray(explanations)
  ) {
    throw new Error(
      `mcqs[${index}].Explanation.Why the Other Options Fail must be an object`
    );
  }

  const allLetters = [
    "A",
    "B",
    "C",
    "D"
  ];

  const expectedWrongLetters =
    allLetters
      .filter(
        (letter) =>
          letter !== correctAnswer
      )
      .sort();

  const actualKeys =
    Object.keys(explanations)
      .sort();

  if (
    JSON.stringify(actualKeys) !==
    JSON.stringify(expectedWrongLetters)
  ) {
    throw new Error(
      `mcqs[${index}].Why the Other Options Fail must contain exactly ${expectedWrongLetters.join(
        ", "
      )}`
    );
  }

  expectedWrongLetters.forEach(
    (letter) => {
      validateString(
        explanations[letter],
        `mcqs[${index}].Explanation.Why the Other Options Fail.${letter}`
      );
    }
  );
}

function validateExplanation(
  explanation,
  correctAnswer,
  index
) {
  if (
    !explanation ||
    typeof explanation !== "object" ||
    Array.isArray(explanation)
  ) {
    throw new Error(
      `mcqs[${index}].Explanation must be an object`
    );
  }

  const expectedKeys = [
    "Correct Answer Summary",
    "Diagnostic Pathway",
    "Examiner's Trap",
    "Why the Other Options Fail"
  ].sort();

  const actualKeys =
    Object.keys(explanation)
      .sort();

  if (
    JSON.stringify(actualKeys) !==
    JSON.stringify(expectedKeys)
  ) {
    throw new Error(
      `mcqs[${index}].Explanation contains missing or extra keys`
    );
  }

  validateString(
    explanation[
      "Correct Answer Summary"
    ],
    `mcqs[${index}].Explanation.Correct Answer Summary`
  );

  validateDiagnosticPathway(
    explanation[
      "Diagnostic Pathway"
    ],
    index
  );

  validateWrongOptionExplanations(
    explanation[
      "Why the Other Options Fail"
    ],
    correctAnswer,
    index
  );

  validateString(
    explanation[
      "Examiner's Trap"
    ],
    `mcqs[${index}].Explanation.Examiner's Trap`
  );
}

function validateMcq(mcq, index) {
  if (
    !mcq ||
    typeof mcq !== "object" ||
    Array.isArray(mcq)
  ) {
    throw new Error(
      `mcqs[${index}] must be an object`
    );
  }

  const expectedKeys = [
    "A",
    "B",
    "C",
    "Correct Answer",
    "D",
    "Explanation",
    "Stem"
  ].sort();

  const actualKeys =
    Object.keys(mcq).sort();

  if (
    JSON.stringify(actualKeys) !==
    JSON.stringify(expectedKeys)
  ) {
    throw new Error(
      `mcqs[${index}] contains missing or extra keys`
    );
  }

  validateString(
    mcq.Stem,
    `mcqs[${index}].Stem`
  );

  validateString(
    mcq.A,
    `mcqs[${index}].A`
  );

  validateString(
    mcq.B,
    `mcqs[${index}].B`
  );

  validateString(
    mcq.C,
    `mcqs[${index}].C`
  );

  validateString(
    mcq.D,
    `mcqs[${index}].D`
  );

  const correctAnswer =
    String(
      mcq["Correct Answer"] ?? ""
    )
      .trim()
      .toUpperCase();

  if (
    !["A", "B", "C", "D"].includes(
      correctAnswer
    )
  ) {
    throw new Error(
      `mcqs[${index}].Correct Answer must be A, B, C or D`
    );
  }

  mcq["Correct Answer"] =
    correctAnswer;

  validateExplanation(
    mcq.Explanation,
    correctAnswer,
    index
  );
}

function validateGeneratedMcqs(raw) {
  const output =
    extractJsonObject(raw);

  if (
    !output ||
    typeof output !== "object" ||
    Array.isArray(output)
  ) {
    throw new Error(
      "Generated output must be a JSON object"
    );
  }

  const rootKeys =
    Object.keys(output);

  if (
    rootKeys.length !== 1 ||
    rootKeys[0] !== "mcqs"
  ) {
    throw new Error(
      'Root object must contain only the key "mcqs"'
    );
  }

  if (
    !Array.isArray(output.mcqs)
  ) {
    throw new Error(
      "mcqs must be an array"
    );
  }

  if (
    output.mcqs.length !==
    REQUIRED_MCQ_COUNT
  ) {
    throw new Error(
      `Output must contain exactly ${REQUIRED_MCQ_COUNT} MCQs; received ${output.mcqs.length}`
    );
  }

  output.mcqs.forEach(
    (mcq, index) => {
      validateMcq(mcq, index);
    }
  );

  return {
    output,
    mcqCount: output.mcqs.length
  };
}

/* -------------------------------------------------------------------------- */
/*                          OPENAI GENERATION                                 */
/* -------------------------------------------------------------------------- */

async function generateMcqs(row) {
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
          instructions:
            SYSTEM_PROMPT,
          input: buildInput(row)
        });

      return validateGeneratedMcqs(
        extractText(response)
      );
    } catch (error) {
      lastError = error;

      if (
        isFatalOpenAiError(error)
      ) {
        throw error;
      }

      const canRetry =
        isRetryable(error) ||
        isValidationError(error);

      if (
        attempt === API_RETRIES ||
        !canRetry
      ) {
        break;
      }

      const delay =
        1000 * 2 ** attempt +
        Math.floor(
          Math.random() * 500
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
      "MCQ generation failed"
    )
  );
}

/* -------------------------------------------------------------------------- */
/*                           LOCK MANAGEMENT                                  */
/* -------------------------------------------------------------------------- */

async function releaseExpiredLocks() {
  const cutoff = new Date(
    Date.now() -
      LOCK_TTL_MIN * 60 * 1000
  ).toISOString();

  const staleLockResult =
    await supabase
      .from(TABLE)
      .update({
        [LOCK_COL]: false,
        [LOCK_AT_COL]: null
      })
      .eq(LOCK_COL, true)
      .not(INPUT_COL, "is", null)
      .is(OUTPUT_COL, null)
      .lt(LOCK_AT_COL, cutoff);

  if (staleLockResult.error) {
    throw new Error(
      `Failed to release expired locks: ${staleLockResult.error.message}`
    );
  }

  const nullTimestampResult =
    await supabase
      .from(TABLE)
      .update({
        [LOCK_COL]: false,
        [LOCK_AT_COL]: null
      })
      .eq(LOCK_COL, true)
      .not(INPUT_COL, "is", null)
      .is(OUTPUT_COL, null)
      .is(LOCK_AT_COL, null);

  if (nullTimestampResult.error) {
    throw new Error(
      `Failed to release invalid locks: ${nullTimestampResult.error.message}`
    );
  }
}

async function releaseLock(row) {
  if (
    !row?.id ||
    !row?.[LOCK_AT_COL]
  ) {
    return;
  }

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
        "serial",
        "sub_serial",
        "mcq",
        "year",
        LOCK_AT_COL
      ].join(",")
    )
    .maybeSingle();

  if (error) {
    throw new Error(
      `Failed to lock row ${candidate.id}: ${error.message}`
    );
  }

  if (
    data &&
    !String(data[INPUT_COL] ?? "")
      .trim()
  ) {
    await releaseLock(data);
    return null;
  }

  return data || null;
}

async function claimRows(limit) {
  await releaseExpiredLocks();

  const { data, error } = await supabase
    .from(TABLE)
    .select("id")
    .eq(LOCK_COL, false)
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .order("id", {
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
      data.map((candidate) =>
        lockRow(candidate)
      )
    );

  const claimedRows = [];

  for (const result of results) {
    if (
      result.status ===
        "fulfilled" &&
      result.value
    ) {
      claimedRows.push(
        result.value
      );
    }

    if (
      result.status ===
      "rejected"
    ) {
      console.error(
        "Row-lock error:",
        errorText(result.reason)
      );
    }
  }

  claimedRows.sort(
    (a, b) =>
      Number(a.id) -
      Number(b.id)
  );

  return claimedRows;
}

/* -------------------------------------------------------------------------- */
/*                           SAVE JSONB OUTPUT                                */
/* -------------------------------------------------------------------------- */

async function saveSuccess(
  row,
  generatedJson
) {
  if (
    !generatedJson ||
    typeof generatedJson !==
      "object" ||
    Array.isArray(generatedJson)
  ) {
    throw new Error(
      "Generated MCQ output must be a JSON object before saving"
    );
  }

  const { data, error } = await supabase
    .from(TABLE)
    .update({
      /*
      mcq_json is JSONB.

      Save the JavaScript object directly.
      Do not use JSON.stringify().
      */
      [OUTPUT_COL]:
        generatedJson,
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
      `Failed to save mcq_json: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or mcq_json already exists"
    );
  }
}

/* -------------------------------------------------------------------------- */
/*                            PROCESS ONE ROW                                 */
/* -------------------------------------------------------------------------- */

async function processRow(row) {
  const rowLabel = [
    row.serial,
    row.sub_serial,
    row.year
  ]
    .filter(
      (value) =>
        value !== null &&
        value !== undefined &&
        String(value).trim()
    )
    .join(" | ");

  console.log(
    `Generating NEET SS Medicine MCQs | id=${row.id} | ${rowLabel || "No source label"}`
  );

  try {
    const result =
      await generateMcqs(row);

    await saveSuccess(
      row,
      result.output
    );

    console.log(
      `Completed | id=${row.id} | ${rowLabel || "No source label"} | mcqs=${result.mcqCount}`
    );

    return {
      fatalApiError: null
    };
  } catch (error) {
    await releaseLock(row);

    if (
      isFatalOpenAiError(error)
    ) {
      const fatalMessage =
        errorText(error);

      console.error(
        `FATAL OPENAI ERROR | id=${row.id}: ${fatalMessage}`
      );

      return {
        fatalApiError:
          fatalMessage
      };
    }

    console.error(
      `Failed | id=${row.id} | ${rowLabel || "No source label"}: ${errorText(error)}`
    );

    return {
      fatalApiError: null
    };
  }
}

/* -------------------------------------------------------------------------- */
/*                              PROCESS BATCH                                 */
/* -------------------------------------------------------------------------- */

async function processBatch(rows) {
  let nextIndex = 0;
  let fatalApiError = null;

  async function runner() {
    while (true) {
      if (fatalApiError) {
        return;
      }

      const index = nextIndex;

      if (index >= rows.length) {
        return;
      }

      nextIndex += 1;

      const result =
        await processRow(
          rows[index]
        );

      if (
        result.fatalApiError
      ) {
        fatalApiError =
          result.fatalApiError;

        return;
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
      () => runner()
    )
  );

  if (fatalApiError) {
    const unstartedRows =
      rows.slice(nextIndex);

    await Promise.allSettled(
      unstartedRows.map((row) =>
        releaseLock(row)
      )
    );
  }

  return {
    fatalApiError
  };
}

/* -------------------------------------------------------------------------- */
/*                                 MAIN                                      */
/* -------------------------------------------------------------------------- */

async function main() {
  console.log(
    `NEET SS MEDICINE SOURCE MCQ -> MCQ_JSON WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `Model=${MODEL} | Table=${TABLE} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | RequiredMCQs=${REQUIRED_MCQ_COUNT} | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY}`
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

      if (
        result.fatalApiError
      ) {
        console.error(
          `Worker stopped because of fatal OpenAI authentication or billing error: ${result.fatalApiError}`
        );

        process.exitCode = 1;
        return;
      }
    } catch (error) {
      if (
        isFatalOpenAiError(error)
      ) {
        console.error(
          `Worker stopped because of fatal OpenAI authentication or billing error: ${errorText(error)}`
        );

        process.exitCode = 1;
        return;
      }

      console.error(
        "Worker loop error:",
        errorText(error)
      );

      await sleep(
        Math.max(
          LOOP_SLEEP_MS,
          3000
        )
      );
    }
  }
}

main().catch((error) => {
  console.error(
    "Fatal NEET SS Medicine MCQ worker error:",
    error
  );

  process.exitCode = 1;
});
