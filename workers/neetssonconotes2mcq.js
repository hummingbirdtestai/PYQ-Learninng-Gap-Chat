"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

const TABLE = "neet_ss_oncology_pyt_source";
const INPUT_COL = "notes_json";
const OUTPUT_COL = "mcq_json";
const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

const EXPECTED_MCQ_COUNT = 10;
const OPTION_LETTERS = ["A", "B", "C", "D"];

function integerEnv(name, fallback, min, max) {
  const value = Number.parseInt(
    process.env[name] || String(fallback),
    10
  );

  if (!Number.isInteger(value) || value < min || value > max) {
    throw new Error(
      `${name} must be an integer between ${min} and ${max}`
    );
  }

  return value;
}

const MODEL =
  process.env.NEETSS_ONCOLOGY_MCQ_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "NEETSS_ONCOLOGY_MCQ_LIMIT",
  50,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "NEETSS_ONCOLOGY_MCQ_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "NEETSS_ONCOLOGY_MCQ_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "NEETSS_ONCOLOGY_MCQ_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "NEETSS_ONCOLOGY_MCQ_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = integerEnv(
  "NEETSS_ONCOLOGY_MCQ_MAX_OUTPUT_TOKENS",
  16000,
  4000,
  50000
);

const WORKER_ID =
  process.env.NEETSS_ONCOLOGY_MCQ_WORKER_ID ||
  `neetss-oncology-mcq-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

/*
PASTE YOUR COMPLETE PROMPT INSIDE THE COMMENT BELOW.

Markdown, JSON examples, quotation marks and backticks are safe here.

IMPORTANT:
The prompt must not contain the comment-closing characters:
* followed immediately by /
*/
const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
NEET SS MEDICAL ONCOLOGY — 10/10 CLINICAL DECISION MCQ MASTER PROMPT

ROLE
You are a senior NEET SS Medical Oncology examiner, ABIM Medical Oncology board examiner, DeVita/MD Anderson clinical educator, molecular tumor board chair, and critical oncology trial editor. Create exceptionally discriminating, current, patient-specific questions, not textbook recall.

INPUT CONTRACT
You receive ONE JSON object with the exact top-level keys:
{
  "topic": "...",
  "subtopics": [ { "subtopic": "...", "notes": ["..."] } ]
}
Read EVERY subtopic and EVERY note. Treat notes as source-supplied clinical claims, not automatically verified truth. Extract the key clinical decision axes, then build EXACTLY 10 novel clinical MCQs spanning the highest-yield distinct axes across the input. Preserve topic scope; do not silently substitute another cancer. No input flashcards are required. The input may cover solid tumors, hematologic malignancies, or general oncology; adapt appropriately.

SOURCE-TO-QUESTION COVERAGE
1. Internally inventory all subtopics and notes, prioritizing staging, pathology, tissue testing, biomarkers, prognostic/predictive interpretation, curative versus palliative intent, treatment eligibility, surgical/radiation interface, treatment sequencing, resistance, toxicities, and landmark evidence.
2. Spread ten questions across the source subtopics as evenly as clinical importance allows; avoid ten variants of the same treatment decision. Where there are more than ten subtopics, cover the most discriminating cross-cutting decisions.
3. The notes are the minimum curriculum, not a license to invent evidence. Use current validated oncology knowledge to check accuracy; if source claims conflict with established evidence or the evidence cutoff, correct the underlying clinical decision rather than reproduce unsafe/outdated management. Do not fabricate verification or pretend to have live guideline access.
4. Distinguish approvals, guideline-preferred regimens, optional alternatives, off-label approaches, and investigational treatments. Never turn a trial signal into routine standard of care.
5. Avoid overly general questions about the tumor name. The answer must turn on a second discriminator after the tumor is recognized.

ONCOLOGY-SPECIFIC DECISION AXES
- Exact stage, TNM/FIGO/AJCC or disease-specific risk system, resectability, nodal status, metastatic burden, and treatment intent.
- Histology, grade, subtype, tumor origin, pathology pitfalls, and actionable vs nonactionable alterations.
- Biomarker specimen/assay adequacy; NGS vs IHC vs FISH vs PCR, predictive versus prognostic utility, resistance mutation, germline versus somatic implication, false-negative testing and reflex testing.
- ECOG/fitness, age only when consequential, creatinine clearance, bilirubin, hepatic impairment, marrow reserve, neuropathy, immune disease, infection, pregnancy/fertility goals, and organ dysfunction.
- Sequence of surgery, systemic therapy, chemoradiation, brachytherapy, targeted therapy, endocrine therapy, immunotherapy, antibody-drug conjugates, transplantation or cellular therapy where appropriate.
- Neoadjuvant/adjuvant/maintenance/consolidation versus definitive treatment; line of therapy, previous drug exposure, response and progression timing.
- RECIST or disease-specific response, minimal residual disease only where validated, pseudoprogression, immune toxicity, febrile neutropenia, tumor lysis, spinal cord compression, SVC syndrome and other emergencies.
- Landmark trial eligibility and actual endpoint when relevant; do not invent hazard ratios, survival benefits, noninferiority claims, or extrapolate trial populations.
- Palliative care and goals-of-care choices where clinically decisive, without replacing potentially curative therapy.

EVIDENCE AND TIME STANDARD
Evidence cutoff: current as of October 2026, or the date explicitly supplied by the caller. Never assume 2027 future approvals are already established. Use DeVita and MD Anderson for clinical depth, NCCN/ESMO/ASCO and disease-specific standards for current practice, and accurately characterized landmark randomized trials. If evidence is contested, rapidly evolving, region-dependent, or unavailable, avoid falsely asserting a single universal standard; choose a better-established question instead.

QUESTION CONSTRUCTION
Create exactly TEN questions. Each stem must contain 30–40 words (count whitespace-separated words; keep the final neutral question within this limit). Clinical data must be coherent. Every question must require: (1) infer a clinical state, (2) interpret a decisive modifier, (3) choose one downstream action or interpretation. Four options only, A–D, each 2–5 words, similar in grammar, specificity, and clinical category. Include at least two genuinely competitive post-diagnosis options. One and only one best answer.

HIGH-DIFFICULTY SAFEGUARDS
Do not use a naked drug–mutation, disease–trial, gene–syndrome, or adverse-effect association as a complete question. A molecular alteration may be in the stem when needed to force the treatment choice, but require another meaningful discriminator (line, stage, resistance, contraindication, etc.). Do not conceal essential information just to make a question ambiguous. Use plausible distractors, not obsolete or absurd options. A trial name may be in the explanation but avoid giving away the answer in the stem. All option-specific explanations must be tied to the precise patient's facts.

## CORE QUESTION STANDARD

For each MCQ, write a 30–40 word clinical stem requiring 2–3 levels of reasoning:

clinical pattern/data
→ infer the hidden diagnosis, physiologic state, or subtype
→ identify an important oncology modifier
→ choose the downstream investigation, treatment, escalation, complication, prevention strategy, mechanism, or interpretation.

The oncology modifier may involve:

- stage and treatment intent
- weight or body surface area
- histology and molecular subtype
- disease severity
- hemodynamic stability
- hydration/perfusion
- neurologic status
- respiratory status
- organ dysfunction
- treatment already received
- treatment response/failure
- timing of illness
- contraindication
- prior immunotherapy exposure
- immune status
- hereditary cancer syndrome
- genetic/metabolic context
- performance status and goals of care
- nutritional status
- organ reserve and treatment fitness
- oncology-specific threshold
- drug toxicity
- complication risk.

Diagnosis recognition alone must NEVER answer the question.

## TRUE 3-STEP REASONING RULE

Every MCQ must require:

STEP 1 — Recognize the hidden oncology clinical state from raw findings.

STEP 2 — Interpret severity, physiology, subtype, complication, treatment response, contraindication, or another modifying factor.

STEP 3 — Select the precise next investigation, management step, escalation, rescue intervention, prevention strategy, mechanism, or interpretation.

If identifying the diagnosis immediately reveals the answer, REWRITE the question.

## ONCOLOGY CONTEXT RULE

Age must matter whenever clinically relevant.

Do not use age merely as decoration.

Use oncology age-specific physiology and management where appropriate:

neonate → infant → toddler → patient → adolescent.

When relevant, distinguish oncology management from adult management.

Growth, development, histology and molecular subtype, weight, pubertal stage, congenital disease, genetic disease, vaccination status, nutritional state, and family history should appear only when they alter diagnosis or management.

## STABILIZATION-FIRST RULE

For acutely ill patientren, force the candidate to recognize whether stabilization precedes definitive diagnosis or disease-specific treatment.

Where relevant, test:

airway
→ breathing
→ circulation
→ neurologic status
→ glucose
→ shock correction
→ electrolyte abnormalities
→ definitive treatment.

Do not allow a sophisticated diagnostic test to become the answer when immediate stabilization is clinically required.

## POST-DIAGNOSIS COMPETITION RULE

After the examinee correctly identifies the diagnosis or physiologic state, the answer must STILL NOT be obvious.

At least TWO answer choices must remain medically plausible until one additional clue is applied.

The discriminating clue should involve one or more of:

age
severity
timing
hemodynamic status
organ function
treatment history
treatment response
contraindication
test limitation
genotype
immune status
complication
drug toxicity
treatment line
guideline threshold.

If diagnosis alone eliminates three options, REWRITE the MCQ.

## ASSOCIATION-ONLY REJECTION RULE

Reject any question solvable by one memorized association such as:

disease → gene
disease → drug
organism → antibiotic
drug → adverse effect
enzyme → substrate
syndrome → chromosome
mutation → disease
buzzword → diagnosis.

The association may establish STEP 1, but another clinical decision must still be required.

## ONCOLOGY CLINICAL VULNERABILITY RULE

NEET SS Oncologys frequently tests patientren in whom the standard answer changes because of physiologic vulnerability.

When appropriate, incorporate ONE meaningful modifying factor such as:

poor performance status
cachexia
shock
hypoxemia
renal dysfunction
hepatic dysfunction
immunodeficiency
cardiac dysfunction
hereditary cancer predisposition
neurologic deterioration
previous therapy
treatment failure
drug toxicity
sepsis
tumor lysis or metabolic disturbance.

Do NOT artificially add comorbidities merely to increase difficulty.

The modifier must genuinely change interpretation or management.

## ONCOLOGY THRESHOLD RULE

When the topic contains clinically established oncology thresholds, use them to discriminate between plausible choices.

Examples include:

staging and response thresholds
blood pressure thresholds
oxygenation thresholds
fluid/electrolyte correction limits
hepatic impairment thresholds
response assessment criteria
fertility-preservation criteria
severity classifications
radiotherapy dose constraints
dose-modification thresholds
renal function
treatment escalation criteria.

Never invent a threshold.

Use a numeric cutoff only when it is well-established and necessary to solve the question.

## MANAGEMENT-SEQUENCE RULE

Prefer questions in which several interventions are correct eventually but only ONE is correct NOW.

Possible architecture:

recognize disease
→ determine stability/severity
→ identify what has already been done
→ determine the next step.

This should create realistic competition between:

stabilization vs definitive therapy
diagnostic test vs treatment
initial therapy vs escalation
continued treatment vs rescue treatment
observation vs intervention
broad therapy vs targeted therapy.

## TREATMENT-FAILURE RULE

Where appropriate, test what happens AFTER first-line treatment.

Use:

initial therapy
→ reassessment
→ objective evidence of response/failure
→ escalation or modification.

Do not repeatedly ask only for first-line treatment.

## ONCOLOGY MECHANISM DEPTH

When suitable, push clinical scenarios into advanced oncology mechanisms involving:

tumor evolution and clonal selection
genetic pathways
receptors
enzymes
transporters
ion channels
immune pathways
oncogenic metabolic vulnerabilities
endocrine resistance
cardiopulmonary treatment toxicity
renal clearance and drug toxicity
neurologic physiology.

However, molecular detail must remain clinically relevant.

Do not create obscure molecular trivia merely to make a question difficult.

## STEM CONSTRUCTION

Each stem must contain 30–40 words.

Never explicitly name the target diagnosis, syndrome, gene, enzyme, receptor, pathway, biochemical state, complication, or mechanism being tested when doing so gives away the answer.

SHOW findings and make the student infer the condition.

Use the logical sequence when applicable:

patient and cancer context
→ presentation
→ relevant examination/vitals
→ relevant investigations
→ previous intervention/response
→ neutral lead-in.

Every stem detail must have discriminatory value.

Each detail must:

1. support the correct pathway,
2. weaken a competing option,
3. establish timing/severity/context, OR
4. serve as one intentional red herring.

Delete everything else.

Do not add decorative demographics, routine normal findings, irrelevant investigations, or unnecessary vital signs.

Use raw values where interpretation itself is being tested.

Vitals and laboratory values must physiologically match the patient's clinical state.

## RED-HERRING RULE

Use exactly ONE plausible red herring only when it creates genuine diagnostic or management competition.

Do not insert misleading information simply to make the question harder.

## LEAD-IN RULE

The final question must be neutral.

Good examples:

"What is the most appropriate next step?"

"Which intervention is most appropriate now?"

"Which investigation would best guide further management?"

"Which mechanism best explains this finding?"

"Which complication should be addressed first?"

Do not reveal the diagnostic pathway in the lead-in.

## OPTIONS

Provide exactly FOUR options: A–D.

Each option must contain 2–5 words.

Options must be:

- grammatically parallel
- structurally parallel
- similar in specificity
- medically plausible
- mutually distinct
- from the same conceptual category.

Prefer near-identical, clinically competitive options differentiated by:

timing
dose strategy
sequence
threshold
route
escalation
contraindication
investigation choice
treatment response
mechanistic nuance.

Avoid obviously absurd distractors.

## ACCURACY STANDARD

All medical oncology, solid tumor, hematologic malignancy, molecular pathology, immunotherapy, radiotherapy-interface, supportive oncology, clinical-trial, and treatment-toxicity concepts must be textbook accurate.

Management must follow accepted contemporary oncology standards consistent with Nelson Oncologys and major specialty guidelines.

Never invent:

drug doses
stage cutoffs
treatment calculations
dose-adjustment rules
severity classifications
diagnostic criteria
radiation constraints
genetic associations
treatment thresholds.

If a precise number is uncertain, construct the question without depending upon that number.

## EXPLANATION

Every MCQ must contain:

### Correct Answer Summary
State the answer and the precise clinical reason it is correct.

### Diagnostic Pathway
Provide concise sequential reasoning showing how the candidate should move from the clinical clues to the final decision.

Use explicit clinical facts rather than generic reasoning language.

### Why the Other Options Fail
Explain EACH of the three incorrect options separately.

Every explanation must state why that specific option is wrong, premature, contraindicated, inferior, or inappropriate for THIS patient.

### Examiner's Trap
Identify the exact misconception that could cause a strong candidate to select the competing answer.

Do not simply restate the correct answer.

## UNIQUE EXPLANATION REQUIREMENT

Every string inside the Explanation object must be completely customized to that specific vignette.

NO placeholders.

NO generic text such as:

"Infer the diagnosis."
"Apply the modifying clue."
"This option is incorrect."
"Consider severity."

Each explanation must explicitly reference the actual disease process, treatment, investigation, physiology, drug, threshold, or clinical clue relevant to that MCQ.

## STRICT STEM–OPTION–EXPLANATION ALIGNMENT

The Stem, A–D options, Correct Answer, Diagnostic Pathway, incorrect-option explanations, and Examiner's Trap must remain within the SAME oncology clinical scenario.

Absolutely NO cross-question contamination or bleed-through is permitted.

## WRONG-OPTION KEY RULE

The "Why the Other Options Fail" object must contain EXACTLY THREE keys corresponding to the incorrect choices.

If Correct Answer = A:
keys must be B, C, D.

If Correct Answer = B:
keys must be A, C, D.

If Correct Answer = C:
keys must be A, B, D.

If Correct Answer = D:
keys must be A, B, C.

Before output, verify that each explanation discusses the EXACT option text represented by that key.

## ANSWER DISTRIBUTION

Randomize correct answers across A, B, C, and D.

Avoid obvious repeated answer-position patterns.

## FINAL 10/10 QUALITY AUDIT

Before accepting each question, internally verify:

1. Stem contains 30–40 words.
2. The case tests genuine medical oncology decision-making.
3. Every detail has discriminatory value.
4. Diagnosis is not explicitly revealed.
5. Diagnosis alone cannot answer the question.
6. At least 2 options remain plausible after diagnosis.
7. An additional modifying clue determines the answer.
8. The question requires 2–3 reasoning steps.
9. Options contain 2–5 words each.
10. Options belong to the same conceptual category.
11. Exactly one answer is best.
12. Oncology physiology and management are accurate.
13. No invented threshold or dose is used.
14. Management sequence is clinically correct.
15. Every incorrect option has a specific explanation.
16. Explanation keys match the actual options.
17. No explanation text has leaked from another MCQ.
18. No placeholder or generic rationale exists.
19. The Examiner's Trap identifies a genuine high-level misconception.
20. The question reaches NEET SS Oncologys / ABP / AMBOSS-style decision depth.

Any MCQ failing ANY mandatory rule must be rewritten before output.

## OUTPUT

Return ONLY valid JSON.

The root object must be:

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
          "...",
          "...",
          "..."
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

The "mcqs" array MUST contain EXACTLY 10 MCQ objects.

Return no Markdown fences.

Return no introductory text.

Return no commentary outside the JSON.

ADDITIONAL ONCOLOGY QUALITY AUDIT (MANDATORY)
- All 10 stems and all 40 options obey length rules.
- At least two choices remain defensible after initial diagnosis, with the modifier deciding the correct answer.
- No unverified trial, biomarker, guideline, drug indication, stage definition, treatment threshold, or approval is invented.
- Curative intent is not confused with metastatic/palliative sequencing; localized and metastatic treatment paradigms are not conflated.
- Molecular and histologic subtypes are not treated as interchangeable.
- Exact wrong-option keys match the three incorrect answer letters; no option is explained under another letter.
- Every explanation names the specific oncologic reason and distinguishes timing, eligibility, evidence, toxicity, or clinical priority.
- Do not output hidden analysis, citations outside the JSON, Markdown, or commentary. Return exactly the JSON root and keys defined above.

INPUT JSON:
{{INPUT_JSON}}
  */ };

  const source = promptContainer.toString();
  const start = source.indexOf("/*") + 2;
  const end = source.lastIndexOf("*/");

  return source.slice(start, end).trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE NEET SS MEDICAL ONCOLOGY MCQ PROMPT HERE"
  )
) {
  throw new Error(
    "Paste the complete inline SYSTEM_PROMPT before starting the worker"
  );
}

const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

function errorText(error) {
  return String(
    error?.message ||
      error?.error?.message ||
      error ||
      "Unknown error"
  );
}

function isFatalOpenAiError(error) {
  const status = Number(
    error?.status ||
      error?.statusCode ||
      error?.response?.status
  );

  return (
    status === 401 ||
    status === 403 ||
    /api key has been invalidated|invalid api key|incorrect api key|authentication error|unauthorized|forbidden|no credits remaining|insufficient_quota|billing|credit balance|billing_hard_limit/i.test(
      errorText(error)
    )
  );
}

function isRetryable(error) {
  if (isFatalOpenAiError(error)) {
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

function requiredText(value, label, minimumLength = 1) {
  const text = String(value ?? "").trim();

  if (text.length < minimumLength) {
    throw new Error(`${label} must be a non-empty string`);
  }

  return text;
}

function normalizeText(value) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function countWords(value) {
  return String(value ?? "")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .length;
}

function serializeNotes(value) {
  if (value === null || value === undefined) {
    throw new Error("notes_json is missing");
  }

  if (typeof value === "string") {
    const text = value.trim();

    if (!text) {
      throw new Error("notes_json is empty");
    }

    try {
      return JSON.stringify(JSON.parse(text), null, 2);
    } catch {
      throw new Error(
        "notes_json contains an invalid JSON string"
      );
    }
  }

  return JSON.stringify(value, null, 2);
}

function buildInput(row) {
  return [
    `TOPIC: ${requiredText(row.topic, "Topic")}`,
    `SUBJECT: ${requiredText(row.subject, "Subject")}`,
    `SERIAL NUMBER: ${row.serial_number}`,
    "",
    "INPUT JSON:",
    serializeNotes(row[INPUT_COL]),
    "",
    `Generate exactly ${EXPECTED_MCQ_COUNT} NEET SS Medical Oncology MCQs.`,
    "Each stem must contain 30–40 whitespace-separated words.",
    "Each MCQ must contain exactly four options: A, B, C and D.",
    "Each option must contain 2–5 whitespace-separated words.",
    "Return only one valid JSON object matching the required schema.",
    "Do not use Markdown code fences.",
    "Do not add commentary before or after the JSON."
  ].join("\n");
}

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

  const text = pieces.join("\n").trim();

  if (!text) {
    throw new Error("OpenAI returned empty output");
  }

  return text;
}

function removeCodeFence(value) {
  let text = String(value ?? "").trim();

  const match = text.match(
    /^```(?:json)?\s*([\s\S]*?)\s*```$/i
  );

  if (match) {
    text = match[1].trim();
  }

  return text;
}

function parseJsonOutput(rawText) {
  const cleaned = removeCodeFence(rawText);

  try {
    return JSON.parse(cleaned);
  } catch {
    const firstBrace = cleaned.indexOf("{");
    const lastBrace = cleaned.lastIndexOf("}");

    if (
      firstBrace === -1 ||
      lastBrace === -1 ||
      lastBrace <= firstBrace
    ) {
      throw new Error(
        "Output does not contain a JSON object"
      );
    }

    const candidate = cleaned.slice(
      firstBrace,
      lastBrace + 1
    );

    try {
      return JSON.parse(candidate);
    } catch (error) {
      throw new Error(
        `Model returned invalid JSON: ${error.message}`
      );
    }
  }
}

function validateExactKeys(object, expectedKeys, label) {
  if (
    !object ||
    typeof object !== "object" ||
    Array.isArray(object)
  ) {
    throw new Error(`${label} must be an object`);
  }

  const actual = Object.keys(object).sort();
  const expected = [...expectedKeys].sort();

  if (
    JSON.stringify(actual) !==
    JSON.stringify(expected)
  ) {
    throw new Error(
      `${label} must contain exactly: ${expectedKeys.join(", ")}`
    );
  }
}

function validateStem(stem, mcqNumber) {
  const text = requiredText(
    stem,
    `MCQ ${mcqNumber} Stem`,
    30
  );

  const wordCount = countWords(text);

  if (wordCount < 30 || wordCount > 40) {
    throw new Error(
      `MCQ ${mcqNumber} Stem contains ${wordCount} words; required range is 30–40`
    );
  }

  if (!text.endsWith("?")) {
    throw new Error(
      `MCQ ${mcqNumber} Stem must end with a question mark`
    );
  }

  return text;
}

function validateOption(value, mcqNumber, letter) {
  const text = requiredText(
    value,
    `MCQ ${mcqNumber} option ${letter}`
  );

  const wordCount = countWords(text);

  if (wordCount < 2 || wordCount > 5) {
    throw new Error(
      `MCQ ${mcqNumber} option ${letter} contains ${wordCount} words; required range is 2–5`
    );
  }

  return text;
}

function validateExplanation(
  explanation,
  mcqNumber,
  correctAnswer
) {
  validateExactKeys(
    explanation,
    [
      "Correct Answer Summary",
      "Diagnostic Pathway",
      "Why the Other Options Fail",
      "Examiner's Trap"
    ],
    `MCQ ${mcqNumber} Explanation`
  );

  explanation["Correct Answer Summary"] =
    requiredText(
      explanation["Correct Answer Summary"],
      `MCQ ${mcqNumber} Correct Answer Summary`,
      15
    );

  explanation["Examiner's Trap"] =
    requiredText(
      explanation["Examiner's Trap"],
      `MCQ ${mcqNumber} Examiner's Trap`,
      10
    );

  const pathway =
    explanation["Diagnostic Pathway"];

  if (
    !Array.isArray(pathway) ||
    pathway.length !== 3
  ) {
    throw new Error(
      `MCQ ${mcqNumber} Diagnostic Pathway must contain exactly 3 steps`
    );
  }

  explanation["Diagnostic Pathway"] =
    pathway.map((step, index) =>
      requiredText(
        step,
        `MCQ ${mcqNumber} Diagnostic Pathway step ${index + 1}`,
        10
      )
    );

  const failures =
    explanation["Why the Other Options Fail"];

  if (
    !failures ||
    typeof failures !== "object" ||
    Array.isArray(failures)
  ) {
    throw new Error(
      `MCQ ${mcqNumber} Why the Other Options Fail must be an object`
    );
  }

  const requiredWrongLetters =
    OPTION_LETTERS.filter(
      (letter) => letter !== correctAnswer
    ).sort();

  const actualWrongLetters =
    Object.keys(failures).sort();

  if (
    JSON.stringify(requiredWrongLetters) !==
    JSON.stringify(actualWrongLetters)
  ) {
    throw new Error(
      `MCQ ${mcqNumber} wrong-option keys must be exactly: ${requiredWrongLetters.join(", ")}`
    );
  }

  for (const letter of requiredWrongLetters) {
    failures[letter] = requiredText(
      failures[letter],
      `MCQ ${mcqNumber} explanation for option ${letter}`,
      15
    );
  }

  return explanation;
}

function validateMcqOutput(value) {
  validateExactKeys(
    value,
    ["mcqs"],
    "MCQ output"
  );

  if (!Array.isArray(value.mcqs)) {
    throw new Error('"mcqs" must be an array');
  }

  if (value.mcqs.length !== EXPECTED_MCQ_COUNT) {
    throw new Error(
      `Expected exactly ${EXPECTED_MCQ_COUNT} MCQs, received ${value.mcqs.length}`
    );
  }

  const normalizedStems = new Set();

  value.mcqs.forEach((mcq, index) => {
    const mcqNumber = index + 1;

    validateExactKeys(
      mcq,
      [
        "Stem",
        "A",
        "B",
        "C",
        "D",
        "Correct Answer",
        "Explanation"
      ],
      `MCQ ${mcqNumber}`
    );

    mcq.Stem = validateStem(
      mcq.Stem,
      mcqNumber
    );

    const normalizedOptions = new Set();

    for (const letter of OPTION_LETTERS) {
      mcq[letter] = validateOption(
        mcq[letter],
        mcqNumber,
        letter
      );

      const normalized =
        normalizeText(mcq[letter]);

      if (normalizedOptions.has(normalized)) {
        throw new Error(
          `MCQ ${mcqNumber} contains duplicate options`
        );
      }

      normalizedOptions.add(normalized);
    }

    const correctAnswer = requiredText(
      mcq["Correct Answer"],
      `MCQ ${mcqNumber} Correct Answer`
    ).toUpperCase();

    if (!OPTION_LETTERS.includes(correctAnswer)) {
      throw new Error(
        `MCQ ${mcqNumber} Correct Answer must be A, B, C or D`
      );
    }

    mcq["Correct Answer"] = correctAnswer;

    mcq.Explanation = validateExplanation(
      mcq.Explanation,
      mcqNumber,
      correctAnswer
    );

    const normalizedStem =
      normalizeText(mcq.Stem);

    if (normalizedStems.has(normalizedStem)) {
      throw new Error(
        `MCQ ${mcqNumber} duplicates another stem`
      );
    }

    normalizedStems.add(normalizedStem);
  });

  return value;
}

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
          instructions: SYSTEM_PROMPT,
          input: buildInput(row),
          max_output_tokens: MAX_OUTPUT_TOKENS
        });

      const rawText =
        extractText(response);

      const parsed =
        parseJsonOutput(rawText);

      const validated =
        validateMcqOutput(parsed);

      return {
        json: validated,
        mcqs: validated.mcqs.length,
        characters: rawText.length
      };
    } catch (error) {
      lastError = error;

      if (isFatalOpenAiError(error)) {
        throw error;
      }

      const validationFailure =
        /empty output|JSON|MCQ|mcqs|Stem|option|Correct Answer|Explanation|Diagnostic Pathway|Other Options|Examiner|word|duplicate|notes_json|exactly/i.test(
          errorText(error)
        );

      if (
        attempt === API_RETRIES ||
        (!isRetryable(error) &&
          !validationFailure)
      ) {
        break;
      }

      const delay =
        1000 * 2 ** attempt +
        Math.floor(Math.random() * 400);

      console.warn(
        `Retry ${attempt + 1}/${API_RETRIES} after ${delay} ms: ${errorText(error)}`
      );

      await sleep(delay);
    }
  }

  throw (
    lastError ||
    new Error(
      "NEET SS Medical Oncology MCQ generation failed"
    )
  );
}

async function verifySchema() {
  const { error } = await supabase
    .from(TABLE)
    .select(
      [
        "id",
        INPUT_COL,
        OUTPUT_COL,
        LOCK_COL,
        LOCK_AT_COL
      ].join(",")
    )
    .limit(1);

  if (error) {
    throw new Error(
      `Schema check failed: ${error.message}`
    );
  }
}

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

async function lockRow(candidate) {
  const lockedAt =
    new Date().toISOString();

  const { data, error } =
    await supabase
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
          "subtopic",
          "number_of_times_asked",
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

async function claimRows(limit) {
  await releaseExpiredLocks();

  const { data, error } =
    await supabase
      .from(TABLE)
      .select("id,serial_number")
      .eq(LOCK_COL, false)
      .not(INPUT_COL, "is", null)
      .is(OUTPUT_COL, null)
      .order("serial_number", {
        ascending: true
      })
      .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending oncology MCQ rows: ${error.message}`
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

    if (result.status === "rejected") {
      console.error(
        "Row-lock error:",
        errorText(result.reason)
      );
    }
  }

  return rows;
}

async function saveSuccess(row, mcqJson) {
  const { data, error } =
    await supabase
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
      `Failed to save oncology MCQs: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or mcq_json already exists"
    );
  }
}

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
      `Failed to release MCQ lock ${row.id}: ${error.message}`
    );
  }
}

async function processRow(row) {
  console.log(
    `Generating oncology MCQs | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateMcqs(row);

    await saveSuccess(
      row,
      result.json
    );

    console.log(
      `Completed | ${row.subject} | ${row.serial_number} | ${row.topic} | mcqs=${result.mcqs} | characters=${result.characters}`
    );

    return {
      fatalApiError: false
    };
  } catch (error) {
    await releaseLock(row);

    if (isFatalOpenAiError(error)) {
      console.error(
        `Fatal OpenAI authentication/billing error: ${errorText(error)}`
      );

      return {
        fatalApiError: true
      };
    }

    console.error(
      `Failed | ${row.subject} | ${row.serial_number} | ${row.topic}: ${errorText(error)}`
    );

    return {
      fatalApiError: false
    };
  }
}

async function processBatch(rows) {
  let next = 0;
  let fatalApiError = false;

  async function runner() {
    while (
      next < rows.length &&
      !fatalApiError
    ) {
      const index = next;
      next += 1;

      const result =
        await processRow(rows[index]);

      if (result.fatalApiError) {
        fatalApiError = true;
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

  if (fatalApiError) {
    await Promise.allSettled(
      rows
        .slice(next)
        .map(releaseLock)
    );
  }

  return {
    fatalApiError
  };
}

async function main() {
  await verifySchema();

  console.log(
    `NEET SS MEDICAL ONCOLOGY NOTES -> MCQ WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `Model=${MODEL} | Table=${TABLE} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | MCQs=${EXPECTED_MCQ_COUNT} | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY} | MaxOutputTokens=${MAX_OUTPUT_TOKENS}`
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
        `Claimed ${rows.length} pending oncology MCQ row(s)`
      );

      const result =
        await processBatch(rows);

      if (result.fatalApiError) {
        console.error(
          "Worker stopped because the OpenAI key is invalid, unauthorized, or billing-blocked."
        );
        process.exit(1);
      }
    } catch (error) {
      if (isFatalOpenAiError(error)) {
        console.error(
          `Fatal OpenAI error: ${errorText(error)}`
        );
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

main().catch((error) => {
  console.error(
    "Fatal oncology MCQ worker error:",
    error
  );

  process.exit(1);
});
