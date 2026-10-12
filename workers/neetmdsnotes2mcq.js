"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ============================================================
// DATABASE CONFIGURATION
// ============================================================

const TABLE = "neet_mds_pyt_source";
const INPUT_COL = "notes_json";
const OUTPUT_COL = "mcq_json";
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
  process.env.NEET_MDS_MCQ_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "NEET_MDS_MCQ_LIMIT",
  50,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "NEET_MDS_MCQ_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "NEET_MDS_MCQ_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "NEET_MDS_MCQ_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "NEET_MDS_MCQ_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = integerEnv(
  "NEET_MDS_MCQ_MAX_OUTPUT_TOKENS",
  16000,
  1000,
  50000
);

const WORKER_ID =
  process.env.NEET_MDS_MCQ_WORKER_ID ||
  `neet-mds-mcq-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ============================================================
// PASTE YOUR COMPLETE PROMPT BELOW
// ============================================================

/*
Paste your complete NEET MDS MCQ prompt between the comment markers.

Important:
The prompt must not contain the closing comment characters:
* immediately followed by /
*/

const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
# SYSTEM ROLE & GOAL
You are a coordinated panel of elite postgraduate dental question writers, psychometric item reviewers, and scientific accuracy editors specialized in NEET MDS (India) and INBDE/NBDE (American Board) conventions.

Your goal is to generate EXACTLY 10 original, highly-discriminating, single-best-answer, four-option MCQs from the supplied input JSON. The questions must reflect true 3-Level Clinical Reasoning where answers cannot be deduced by mere buzzword recognition.

# THE 10/10 EXAM ALIGNMENT RULES (CRITICAL OVERRIDES)

## RULE 1: The "Show, Don't Tell" Clinical & Histopathological Constraint
- NEVER state the diagnosis, classification category, root maturity status, pulpal vitality state, or hard-tissue injury class directly in the stem.
- Describe the raw presentation exactly as a clinician observes it or as a pathologist views it under a microscope.
- *Bad Example:* "Biopsy shows necrotic salivary acini and squamous metaplasia."
- *Good 10/10 Example:* "H&E staining reveals total loss of acinar basophilia with intact peripheral reticulin frameworks and islands of cytologically uniform, non-keratinizing squamoid cells lacking cellular pleomorphism."
- *Bad Example:* "An 11-year-old presents with an uncomplicated enamel-dentin crown fracture."
- *Good 10/10 Example:* "An 11-year-old presents following dental trauma. Clinical examination reveals a fracture line traversing a hard translucent tissue layer into an underlying solid yellow substrate, with no pinpoint reddish hemorrhage or fluid exudation."

## RULE 2: Strict Option Compression (Maximum 6 Words)
- Options must be rapid-fire, highly scannable choices. They must consist strictly of clean diagnoses, anatomical structures, specific pharmacological agents, exact clinical materials, or precise clinical actions.
- ABSOLUTE MAXIMUM: 6 whitespace-delimited words per option. NO exceptions.
- NEVER embed patient-specific variables, justifications, or conditional statements inside the choices (e.g., do not write "Cvek pulpotomy because the root is immature"). Keep choices completely clean (e.g., "Cvek pulpotomy").

## RULE 3: The "Tipping Point" Clinical Variable
- The clinical history must introduce overlapping parameters or conflicting variables (e.g., precise timeline delays, contamination contexts, or prior dental history) that force a difficult differential diagnosis between adjacent conditions.
- Design the vignette so that if a single clinical variable were altered (e.g., changing an injury timeline from "2 hours" to "36 hours", or a laboratory parameter from normal to abnormal), the correct answer would completely flip to one of the other distractors.

## RULE 4: Objective Laboratory Data vs. Subjective Interpretation
- Never use simplifying text phrases like "the patient's aPTT is prolonged" or "the bleeding time is normal."
- Provide exact numeric metrics followed by standard reference intervals (e.g., "PT: 12 seconds [Ref: 11-13.5s], aPTT: 58 seconds [Ref: 25-35s], Platelet Count: 250,000/µL"). Force the candidate to manually calculate the isolated pathway or cellular defect.

# MANDATORY THREE-LEVEL REASONING CHAIN
Every item must force the candidate to execute three distinct cognitive steps:
- Level 1 (Analyze): Synthesize raw clinical data, numeric lab markers, or morphological histopathology to determine the unstated structural status or condition.
- Level 2 (Diagnose): Apply a secondary decisive differentiator (e.g., anatomical location, elapsed time window, tissue vitality trends, or radiographic progression) to resolve a realistic competing diagnosis or material choice.
- Level 3 (Apply Action/Predict): Select the most appropriate downstream clinical intervention, management protocol, underlying mechanism, or prognosis.

# DENTAL DOMAIN SPECIFICS
1. Operative/Endodontics: Differentiate reversible vs. irreversible pulpitis using precise stimulus thresholds, lingering characteristics, and structural status. Remember: a negative cold/EPT response immediately post-trauma does not definitively confirm pulpal necrosis; factor in apical diameter and follow-up markers.
2. Oral Pathology/Radiology: Combine radiographic presentation (e.g., corticated vs. ill-defined borders, tooth resorption patterns) with raw microscopic descriptions. Distinguish reactive mimics from true malignancies (e.g., Necrotizing Sialometaplasia vs. Mucoepidermoid Carcinoma).
3. Oral & Maxillofacial Surgery/Anesthesia: Anchor questions around surgical complications, anatomical space pathways, precise nerve boundaries, and pharmacological properties (e.g., alpha-1 vasoconstriction transitioning to rebound beta-2 vasodilation causing reactionary hemorrhage).
4. Coagulation & Hematology: Evaluate clear quantitative hematological metrics. Differentiate intrinsic vs. extrinsic pathway factor deficiencies, mixing study outcomes, and target therapies (e.g., Factor VIII vs. von Willebrand Factor vs. Desmopressin indications).

# EXPLANATION FORMAT
For every question, you must provide a detailed breakdown inside the JSON structure:
- "Correct Answer Summary": State the correct option letter and a direct, scientific rationale.
- "Diagnostic Pathway": Must contain exactly three numbered logical steps mapping directly to Level 1, Level 2, and Level 3 reasoning.
- "Why the Other Options Fail": Provide a custom analysis for each of the three incorrect options, explicitly detailing the exact clinical scenario or parameter change that WOULD make that distractor the correct choice.
- "Examiner's Trap": Explain the exact surface-level cognitive shortcut, diagnostic bypass, or tempting buzzword mismatch the question uses to trap underprepared candidates.

# OUTPUT CONTRACT
Return ONLY a single valid JSON object matching the template below. Do not include introductory text, conversational preambles, or markdown formatting outside the JSON block.

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
        "Diagnostic Pathway": [
          "Step 1: [Identify the hidden condition from the raw data]",
          "Step 2: [Apply the decisive differentiator variable]",
          "Step 3: [Derive the downstream action or mechanism]"
        ],
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
  */ };

  const source = promptContainer.toString();
  const start = source.indexOf("/*") + 2;
  const end = source.lastIndexOf("*/");

  return source.slice(start, end).trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE NEET MDS MCQ PROMPT HERE"
  )
) {
  throw new Error(
    "Paste your complete prompt inside SYSTEM_PROMPT before starting the worker"
  );
}

// ============================================================
// TECHNICAL OUTPUT CONTRACT
// ============================================================

const OUTPUT_CONTRACT = `
MANDATORY TECHNICAL OUTPUT CONTRACT:

Return ONLY one valid JSON object with this exact structure:

{
  "mcqs": [
    {
      "Stem": "string",
      "A": "string",
      "B": "string",
      "C": "string",
      "D": "string",
      "Correct Answer": "A",
      "Explanation": {
        "Correct Answer Summary": "string",
        "Diagnostic Pathway": [
          "Step 1: string",
          "Step 2: string",
          "Step 3: string"
        ],
        "Why the Other Options Fail": {
          "B": "string",
          "C": "string",
          "D": "string"
        },
        "Examiner's Trap": "string"
      }
    }
  ]
}

Technical rules:

- Generate exactly 10 MCQs.
- Each MCQ must have exactly four options: A, B, C and D.
- Every option must contain no more than 6 whitespace-delimited words.
- "Correct Answer" must be exactly one letter: A, B, C or D.
- "Diagnostic Pathway" must contain exactly 3 non-empty strings.
- "Why the Other Options Fail" must explain exactly the 3 incorrect options.
- Every required field must be present and non-empty.
- Return strict JSON only.
- Do not return Markdown outside JSON.
- Do not wrap JSON in a code fence.
- Do not add introductory or concluding text.
- Do not include trailing commas.
- Do not use JavaScript object syntax.
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
  return /empty output|valid JSON|JSON object|mcqs array|exactly 10|missing|must contain|must be|option|six words|6 words|Correct Answer|Explanation|Diagnostic Pathway|Other Options|Examiner's Trap|notes_json/i.test(
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

function wordCount(value) {
  const cleaned = String(value ?? "")
    .replace(/[*_`~]/g, "")
    .trim();

  if (!cleaned) {
    return 0;
  }

  return cleaned
    .split(/\s+/)
    .filter(Boolean)
    .length;
}

// ============================================================
// SERIALIZE NOTES JSON
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
    `SERIAL NUMBER: ${row.serial_number}`,
    `NUMBER OF TIMES ASKED: ${row.number_of_times_asked}`,
    row.exam
      ? `EXAM: ${row.exam}`
      : null,
    row.year_asked
      ? `YEAR ASKED: ${row.year_asked}`
      : null,
    row.pyt
      ? `PYT: ${row.pyt}`
      : null,
    row.subtopic_classification
      ? `SUBTOPIC CLASSIFICATION: ${row.subtopic_classification}`
      : null,
    "",
    "SOURCE NEET MDS NOTES JSON:",
    serializeSource(row[INPUT_COL])
  ]
    .filter((line) => line !== null)
    .join("\n");
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
// CLEAN AND PARSE JSON
// ============================================================

function removeOuterCodeFence(value) {
  let text = requiredText(
    value,
    "Generated MCQ output"
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

    const candidate = text.slice(
      firstBrace,
      lastBrace + 1
    );

    try {
      return JSON.parse(candidate);
    } catch (error) {
      throw new Error(
        `Generated output is not valid JSON: ${error.message}`
      );
    }
  }
}

// ============================================================
// VALIDATE ONE MCQ
// ============================================================

function validateMcq(mcq, index) {
  const label = `MCQ ${index + 1}`;

  if (
    !mcq ||
    typeof mcq !== "object" ||
    Array.isArray(mcq)
  ) {
    throw new Error(
      `${label} must be a JSON object`
    );
  }

  const stem = requiredText(
    mcq.Stem,
    `${label} Stem`
  );

  const options = {};

  for (const letter of [
    "A",
    "B",
    "C",
    "D"
  ]) {
    const option = requiredText(
      mcq[letter],
      `${label} option ${letter}`
    );

    const words = wordCount(option);

    if (words > 6) {
      throw new Error(
        `${label} option ${letter} contains ${words} words; maximum is 6 words`
      );
    }

    options[letter] = option;
  }

  const normalizedOptions = Object.values(
    options
  ).map((option) =>
    option
      .toLowerCase()
      .replace(/[*_`~.,;:()[\]{}]/g, "")
      .replace(/\s+/g, " ")
      .trim()
  );

  if (
    new Set(normalizedOptions).size !== 4
  ) {
    throw new Error(
      `${label} must contain four unique options`
    );
  }

  const correctAnswer = requiredText(
    mcq["Correct Answer"],
    `${label} Correct Answer`
  ).toUpperCase();

  if (
    !["A", "B", "C", "D"].includes(
      correctAnswer
    )
  ) {
    throw new Error(
      `${label} Correct Answer must be A, B, C or D`
    );
  }

  const explanation = mcq.Explanation;

  if (
    !explanation ||
    typeof explanation !== "object" ||
    Array.isArray(explanation)
  ) {
    throw new Error(
      `${label} Explanation must be a JSON object`
    );
  }

  const correctAnswerSummary =
    requiredText(
      explanation[
        "Correct Answer Summary"
      ],
      `${label} Correct Answer Summary`
    );

  const diagnosticPathway =
    explanation[
      "Diagnostic Pathway"
    ];

  if (
    !Array.isArray(
      diagnosticPathway
    )
  ) {
    throw new Error(
      `${label} Diagnostic Pathway must be an array`
    );
  }

  if (
    diagnosticPathway.length !== 3
  ) {
    throw new Error(
      `${label} Diagnostic Pathway must contain exactly 3 steps`
    );
  }

  const normalizedPathway =
    diagnosticPathway.map(
      (step, stepIndex) =>
        requiredText(
          step,
          `${label} Diagnostic Pathway step ${stepIndex + 1}`
        )
    );

  const whyOtherOptionsFail =
    explanation[
      "Why the Other Options Fail"
    ];

  if (
    !whyOtherOptionsFail ||
    typeof whyOtherOptionsFail !==
      "object" ||
    Array.isArray(whyOtherOptionsFail)
  ) {
    throw new Error(
      `${label} Why the Other Options Fail must be a JSON object`
    );
  }

  const incorrectLetters = [
    "A",
    "B",
    "C",
    "D"
  ].filter(
    (letter) =>
      letter !== correctAnswer
  );

  const normalizedOtherOptions = {};

  for (
    const letter of incorrectLetters
  ) {
    normalizedOtherOptions[letter] =
      requiredText(
        whyOtherOptionsFail[letter],
        `${label} explanation for incorrect option ${letter}`
      );
  }

  const examinerTrap = requiredText(
    explanation["Examiner's Trap"],
    `${label} Examiner's Trap`
  );

  return {
    Stem: stem,
    A: options.A,
    B: options.B,
    C: options.C,
    D: options.D,
    "Correct Answer": correctAnswer,
    Explanation: {
      "Correct Answer Summary":
        correctAnswerSummary,
      "Diagnostic Pathway":
        normalizedPathway,
      "Why the Other Options Fail":
        normalizedOtherOptions,
      "Examiner's Trap":
        examinerTrap
    }
  };
}

// ============================================================
// VALIDATE COMPLETE MCQ OUTPUT
// ============================================================

function validateMcqOutput(value) {
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value)
  ) {
    throw new Error(
      "Generated output must be a JSON object"
    );
  }

  if (!Array.isArray(value.mcqs)) {
    throw new Error(
      "Generated output must contain an mcqs array"
    );
  }

  if (value.mcqs.length !== 10) {
    throw new Error(
      `Generated output contains ${value.mcqs.length} MCQs; expected exactly 10`
    );
  }

  const mcqs = value.mcqs.map(
    validateMcq
  );

  const normalizedStems = mcqs.map(
    (mcq) =>
      mcq.Stem
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim()
  );

  if (
    new Set(normalizedStems).size !==
    mcqs.length
  ) {
    throw new Error(
      "Generated output contains duplicate MCQ stems"
    );
  }

  const answerDistribution = {
    A: 0,
    B: 0,
    C: 0,
    D: 0
  };

  for (const mcq of mcqs) {
    answerDistribution[
      mcq["Correct Answer"]
    ] += 1;
  }

  const json = {
    mcqs
  };

  return {
    json,
    mcqCount: mcqs.length,
    characters:
      JSON.stringify(json).length,
    answerDistribution
  };
}

// ============================================================
// GENERATE MCQS
// ============================================================

async function generateMcqs(row) {
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

      return validateMcqOutput(
        parsedOutput
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
      "NEET MDS MCQ generation failed"
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
      `Failed to release expired MCQ locks: ${error.message}`
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
        "serial_number",
        "topic",
        "number_of_times_asked",
        "subtopic_classification",
        "exam",
        "year_asked",
        "pyt",
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
      "id,serial_number"
    )
    .eq(LOCK_COL, false)
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .order("serial_number", {
      ascending: true
    })
    .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending NEET MDS MCQ rows: ${error.message}`
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
// SAVE SUCCESSFUL MCQ JSON
// ============================================================

async function saveSuccess(row, mcqJson) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: mcqJson,
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
      "Save rejected because the lock changed or MCQs already exist"
    );
  }
}

// ============================================================
// RELEASE ONE ROW LOCK
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
    `Generating NEET MDS MCQs | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateMcqs(row);

    await saveSuccess(
      row,
      result.json
    );

    console.log(
      `Completed | ${row.subject} | ${row.serial_number} | ${row.topic} | mcqs=${result.mcqCount} | answers=A:${result.answerDistribution.A},B:${result.answerDistribution.B},C:${result.answerDistribution.C},D:${result.answerDistribution.D} | characters=${result.characters}`
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
      `Failed | ${row.subject} | ${row.serial_number} | ${row.topic}: ${errorText(error)}`
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
    `NEET MDS NOTES -> MCQ WORKER STARTED: ${WORKER_ID}`
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
        `Claimed ${rows.length} pending NEET MDS MCQ row(s)`
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
    "Fatal NEET MDS MCQ worker error:",
    error
  );

  process.exit(1);
});
