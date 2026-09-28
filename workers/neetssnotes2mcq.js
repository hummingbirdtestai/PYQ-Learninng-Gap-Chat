"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// DATABASE CONFIGURATION
// notes_json → mcq_json
// ─────────────────────────────────────────────

const TABLE =
  "neet_ss_pediatrics_pyt_source";

const INPUT_COL =
  "notes_json";

const OUTPUT_COL =
  "mcq_json";

const LOCK_COL =
  "generation_lock";

const LOCK_AT_COL =
  "generation_locked_at";

const REQUIRED_MCQ_COUNT = 10;
const MIN_STEM_WORDS = 30;
const MAX_STEM_WORDS = 40;
const MIN_OPTION_WORDS = 2;
const MAX_OPTION_WORDS = 5;

const OPTION_LETTERS = [
  "A",
  "B",
  "C",
  "D"
];

// ─────────────────────────────────────────────
// ENVIRONMENT CONFIGURATION
// ─────────────────────────────────────────────

function parseIntegerEnv(
  name,
  fallback,
  min,
  max
) {
  const value = Number.parseInt(
    process.env[name] ||
      String(fallback),
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
  process.env.NEET_SS_PEDS_MCQ_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT =
  parseIntegerEnv(
    "NEET_SS_PEDS_MCQ_LIMIT",
    50,
    1,
    100
  );

const BATCH_SIZE =
  parseIntegerEnv(
    "NEET_SS_PEDS_MCQ_BATCH_SIZE",
    5,
    1,
    20
  );

const LOOP_SLEEP_MS =
  parseIntegerEnv(
    "NEET_SS_PEDS_MCQ_LOOP_SLEEP_MS",
    1000,
    250,
    60000
  );

const LOCK_TTL_MIN =
  parseIntegerEnv(
    "NEET_SS_PEDS_MCQ_LOCK_TTL_MIN",
    120,
    5,
    1440
  );

const API_RETRIES =
  parseIntegerEnv(
    "NEET_SS_PEDS_MCQ_API_RETRIES",
    2,
    0,
    5
  );

const WORKER_ID =
  process.env.NEET_SS_PEDS_MCQ_WORKER_ID ||
  `neet-ss-peds-mcq-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = `
You are an expert Senior NEET SS Pediatrics / American Board of Pediatrics / NBME / AMBOSS examiner.

Take the supplied Flashcard Question → Answer material containing PYQs and future questions and create EXACTLY 10 medically accurate, exam-standard, superspecialty pediatric clinical vignette MCQs.

The questions must test advanced PEDIATRIC CLINICAL DECISION-MAKING rather than simple diagnosis or factual recall.

## CORE QUESTION STANDARD

For each MCQ, write a 30–40 word clinical stem requiring 2–3 levels of reasoning:

clinical pattern/data
→ infer the hidden diagnosis, physiologic state, or subtype
→ identify an important pediatric modifier
→ choose the downstream investigation, treatment, escalation, complication, prevention strategy, mechanism, or interpretation.

The pediatric modifier may involve:

- age or developmental stage
- weight or body surface area
- gestational age
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
- immunization status
- immune status
- congenital anomaly
- genetic/metabolic context
- growth/puberty
- nutritional status
- neonatal physiology
- pediatric-specific threshold
- drug toxicity
- complication risk.

Diagnosis recognition alone must NEVER answer the question.

## TRUE 3-STEP REASONING RULE

Every MCQ must require:

STEP 1 — Recognize the hidden pediatric clinical state from raw findings.

STEP 2 — Interpret severity, physiology, subtype, complication, treatment response, contraindication, or another modifying factor.

STEP 3 — Select the precise next investigation, management step, escalation, rescue intervention, prevention strategy, mechanism, or interpretation.

If identifying the diagnosis immediately reveals the answer, REWRITE the question.

## PEDIATRIC CONTEXT RULE

Age must matter whenever clinically relevant.

Do not use age merely as decoration.

Use pediatric age-specific physiology and management where appropriate:

neonate → infant → toddler → child → adolescent.

When relevant, distinguish pediatric management from adult management.

Growth, development, gestational age, weight, pubertal stage, congenital disease, genetic disease, vaccination status, nutritional state, and family history should appear only when they alter diagnosis or management.

## STABILIZATION-FIRST RULE

For acutely ill children, force the candidate to recognize whether stabilization precedes definitive diagnosis or disease-specific treatment.

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
developmental stage
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

## PEDIATRIC CLINICAL VULNERABILITY RULE

NEET SS Pediatrics frequently tests children in whom the standard answer changes because of physiologic vulnerability.

When appropriate, incorporate ONE meaningful modifying factor such as:

prematurity
severe malnutrition
shock
hypoxemia
renal dysfunction
hepatic dysfunction
immunodeficiency
congenital heart disease
metabolic disease
neurologic deterioration
previous therapy
treatment failure
drug toxicity
sepsis
fluid/electrolyte disturbance.

Do NOT artificially add comorbidities merely to increase difficulty.

The modifier must genuinely change interpretation or management.

## PEDIATRIC THRESHOLD RULE

When the topic contains clinically established pediatric thresholds, use them to discriminate between plausible choices.

Examples include:

age-dependent normal values
blood pressure thresholds
oxygenation thresholds
fluid/electrolyte correction limits
bilirubin treatment thresholds
growth criteria
pubertal criteria
severity classifications
ventilation parameters
drug dosing limits
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

## PEDIATRIC MECHANISM DEPTH

When suitable, push clinical scenarios into advanced pediatric mechanisms involving:

developmental physiology
genetic pathways
receptors
enzymes
transporters
ion channels
immune pathways
metabolic pathways
endocrine feedback
cardiopulmonary physiology
renal handling
neurologic physiology.

However, molecular detail must remain clinically relevant.

Do not create obscure molecular trivia merely to make a question difficult.

## STEM CONSTRUCTION

Each stem must contain 30–40 words.

Never explicitly name the target diagnosis, syndrome, gene, enzyme, receptor, pathway, biochemical state, complication, or mechanism being tested when doing so gives away the answer.

SHOW findings and make the student infer the condition.

Use the logical sequence when applicable:

age/developmental context
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

Vitals and laboratory values must physiologically match the child's clinical state.

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

All pediatric medicine, neonatology, genetics, nutrition, infectious disease, emergency medicine, critical care, cardiology, pulmonology, nephrology, neurology, gastroenterology, endocrinology, hematology-oncology, rheumatology, immunology, metabolic medicine, adolescent medicine, and developmental concepts must be textbook accurate.

Management must follow accepted contemporary pediatric standards consistent with Nelson Pediatrics and major specialty guidelines.

Never invent:

drug doses
age cutoffs
fluid calculations
electrolyte correction rates
severity classifications
diagnostic criteria
ventilator parameters
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

Every explanation must state why that specific option is wrong, premature, contraindicated, inferior, or inappropriate for THIS child.

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

The Stem, A–D options, Correct Answer, Diagnostic Pathway, incorrect-option explanations, and Examiner's Trap must remain within the SAME pediatric clinical scenario.

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
2. The patient is genuinely pediatric.
3. Every detail has discriminatory value.
4. Diagnosis is not explicitly revealed.
5. Diagnosis alone cannot answer the question.
6. At least 2 options remain plausible after diagnosis.
7. An additional modifying clue determines the answer.
8. The question requires 2–3 reasoning steps.
9. Options contain 2–5 words each.
10. Options belong to the same conceptual category.
11. Exactly one answer is best.
12. Pediatric physiology and management are accurate.
13. No invented threshold or dose is used.
14. Management sequence is clinically correct.
15. Every incorrect option has a specific explanation.
16. Explanation keys match the actual options.
17. No explanation text has leaked from another MCQ.
18. No placeholder or generic rationale exists.
19. The Examiner's Trap identifies a genuine high-level misconception.
20. The question reaches NEET SS Pediatrics / ABP / AMBOSS-style decision depth.

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
`.trim();

if (!SYSTEM_PROMPT) {
  throw new Error(
    "SYSTEM_PROMPT cannot be empty"
  );
}

// ─────────────────────────────────────────────
// STRUCTURED OUTPUT SCHEMA
// Non-strict mode is intentional because
// wrong-option keys vary with the correct answer.
// Manual validation below enforces the exact keys.
// ─────────────────────────────────────────────

const MCQ_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["mcqs"],
  properties: {
    mcqs: {
      type: "array",
      minItems: REQUIRED_MCQ_COUNT,
      maxItems: REQUIRED_MCQ_COUNT,
      items: {
        type: "object",
        additionalProperties: false,
        required: [
          "Stem",
          "A",
          "B",
          "C",
          "D",
          "Correct Answer",
          "Explanation"
        ],
        properties: {
          Stem: {
            type: "string"
          },
          A: {
            type: "string"
          },
          B: {
            type: "string"
          },
          C: {
            type: "string"
          },
          D: {
            type: "string"
          },
          "Correct Answer": {
            type: "string",
            enum: OPTION_LETTERS
          },
          Explanation: {
            type: "object",
            additionalProperties: false,
            required: [
              "Correct Answer Summary",
              "Diagnostic Pathway",
              "Why the Other Options Fail",
              "Examiner's Trap"
            ],
            properties: {
              "Correct Answer Summary": {
                type: "string"
              },
              "Diagnostic Pathway": {
                type: "array",
                minItems: 2,
                items: {
                  type: "string"
                }
              },
              "Why the Other Options Fail": {
                type: "object",
                additionalProperties: {
                  type: "string"
                }
              },
              "Examiner's Trap": {
                type: "string"
              }
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
  return /no credits remaining|insufficient_quota|billing|credit balance|billing_hard_limit/i.test(
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

function requireString(
  value,
  label
) {
  const normalized =
    String(value ?? "").trim();

  if (!normalized) {
    throw new Error(
      `${label} must be a non-empty string`
    );
  }

  return normalized;
}

function countWords(value) {
  return String(value)
    .trim()
    .split(/\s+/u)
    .filter(Boolean)
    .length;
}

function serializeJsonInput(value) {
  if (
    value === null ||
    value === undefined
  ) {
    throw new Error(
      "Notes input is missing"
    );
  }

  if (typeof value === "string") {
    const trimmed = value.trim();

    if (!trimmed) {
      throw new Error(
        "Notes input is empty"
      );
    }

    try {
      return JSON.stringify(
        JSON.parse(trimmed),
        null,
        2
      );
    } catch {
      return trimmed;
    }
  }

  return JSON.stringify(
    value,
    null,
    2
  );
}

function sameStringSet(
  actual,
  expected
) {
  if (
    actual.length !==
    expected.length
  ) {
    return false;
  }

  const left =
    [...actual].sort();

  const right =
    [...expected].sort();

  return left.every(
    (value, index) =>
      value === right[index]
  );
}

// ─────────────────────────────────────────────
// BUILD INPUT
// Sends topic and notes_json.
// ─────────────────────────────────────────────

function buildUserInput(row) {
  return [
    `TOPIC / PYT: ${requireString(
      row.topic,
      "Topic"
    )}`,
    "",
    "RAPID REVISION NOTES:",
    serializeJsonInput(
      row[INPUT_COL]
    ),
    "",
    "Generate exactly 10 database-ready MCQs now."
  ].join("\n");
}

// ─────────────────────────────────────────────
// RESPONSE EXTRACTION
// ─────────────────────────────────────────────

function extractResponseText(response) {
  if (
    typeof response?.output_text ===
      "string" &&
    response.output_text.trim()
  ) {
    return response.output_text.trim();
  }

  const collected = [];

  for (
    const outputItem of
    response?.output || []
  ) {
    for (
      const contentItem of
      outputItem?.content || []
    ) {
      if (
        contentItem?.type ===
          "output_text" &&
        typeof contentItem.text ===
          "string"
      ) {
        collected.push(
          contentItem.text
        );
      }
    }
  }

  const text =
    collected.join("\n").trim();

  if (!text) {
    throw new Error(
      "OpenAI returned empty output"
    );
  }

  return text;
}

function cleanJsonText(rawOutput) {
  return String(rawOutput)
    .trim()
    .replace(
      /^\s*```(?:json)?\s*/i,
      ""
    )
    .replace(
      /\s*```\s*$/i,
      ""
    )
    .trim();
}

// ─────────────────────────────────────────────
// MCQ VALIDATION
// ─────────────────────────────────────────────

function validateAndNormalize(rawOutput) {
  let parsed;

  try {
    parsed = JSON.parse(
      cleanJsonText(rawOutput)
    );
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
      "Generated MCQs must be one JSON object"
    );
  }

  if (
    !sameStringSet(
      Object.keys(parsed),
      ["mcqs"]
    )
  ) {
    throw new Error(
      "Generated output must contain exactly mcqs"
    );
  }

  if (
    !Array.isArray(parsed.mcqs) ||
    parsed.mcqs.length !==
      REQUIRED_MCQ_COUNT
  ) {
    throw new Error(
      `Generated output must contain exactly ${REQUIRED_MCQ_COUNT} MCQs`
    );
  }

  const expectedMcqKeys = [
    "Stem",
    "A",
    "B",
    "C",
    "D",
    "Correct Answer",
    "Explanation"
  ];

  const expectedExplanationKeys = [
    "Correct Answer Summary",
    "Diagnostic Pathway",
    "Why the Other Options Fail",
    "Examiner's Trap"
  ];

  const answerDistribution = {
    A: 0,
    B: 0,
    C: 0,
    D: 0
  };

  const seenStems =
    new Set();

  const mcqs =
    parsed.mcqs.map(
      (mcq, index) => {
        const position =
          index + 1;

        if (
          !mcq ||
          typeof mcq !== "object" ||
          Array.isArray(mcq)
        ) {
          throw new Error(
            `MCQ ${position} is not an object`
          );
        }

        if (
          !sameStringSet(
            Object.keys(mcq),
            expectedMcqKeys
          )
        ) {
          throw new Error(
            `MCQ ${position} has invalid fields`
          );
        }

        const Stem =
          requireString(
            mcq.Stem,
            `MCQ ${position} Stem`
          );

        const stemWordCount =
          countWords(Stem);

        if (
          stemWordCount < MIN_STEM_WORDS ||
          stemWordCount > MAX_STEM_WORDS
        ) {
          throw new Error(
            `MCQ ${position} Stem has ${stemWordCount} words; required range is ${MIN_STEM_WORDS}-${MAX_STEM_WORDS}`
          );
        }

        const stemKey =
          Stem
            .replace(/\s+/g, " ")
            .trim()
            .toLowerCase();

        if (
          seenStems.has(stemKey)
        ) {
          throw new Error(
            `MCQ ${position} duplicates another Stem`
          );
        }

        seenStems.add(stemKey);

        const options = {};

        for (
          const letter of
          OPTION_LETTERS
        ) {
          const option =
            requireString(
              mcq[letter],
              `MCQ ${position} option ${letter}`
            );

          const optionWordCount =
            countWords(option);

          if (
            optionWordCount <
              MIN_OPTION_WORDS ||
            optionWordCount >
              MAX_OPTION_WORDS
          ) {
            throw new Error(
              `MCQ ${position} option ${letter} has ${optionWordCount} words; required range is ${MIN_OPTION_WORDS}-${MAX_OPTION_WORDS}`
            );
          }

          options[letter] =
            option;
        }

        const normalizedOptions =
          OPTION_LETTERS.map(
            (letter) =>
              options[letter]
                .replace(/\s+/g, " ")
                .trim()
                .toLowerCase()
          );

        if (
          new Set(
            normalizedOptions
          ).size !== 4
        ) {
          throw new Error(
            `MCQ ${position} contains duplicate options`
          );
        }

        const correctAnswer =
          requireString(
            mcq["Correct Answer"],
            `MCQ ${position} Correct Answer`
          ).toUpperCase();

        if (
          !OPTION_LETTERS.includes(
            correctAnswer
          )
        ) {
          throw new Error(
            `MCQ ${position} has an invalid Correct Answer`
          );
        }

        answerDistribution[
          correctAnswer
        ] += 1;

        const explanation =
          mcq.Explanation;

        if (
          !explanation ||
          typeof explanation !==
            "object" ||
          Array.isArray(explanation)
        ) {
          throw new Error(
            `MCQ ${position} Explanation is invalid`
          );
        }

        if (
          !sameStringSet(
            Object.keys(explanation),
            expectedExplanationKeys
          )
        ) {
          throw new Error(
            `MCQ ${position} Explanation has invalid fields`
          );
        }

        const correctSummary =
          requireString(
            explanation[
              "Correct Answer Summary"
            ],
            `MCQ ${position} Correct Answer Summary`
          );

        if (
          !Array.isArray(
            explanation[
              "Diagnostic Pathway"
            ]
          ) ||
          explanation[
            "Diagnostic Pathway"
          ].length < 2
        ) {
          throw new Error(
            `MCQ ${position} Diagnostic Pathway must contain at least 2 steps`
          );
        }

        const diagnosticPathway =
          explanation[
            "Diagnostic Pathway"
          ].map(
            (step, stepIndex) =>
              requireString(
                step,
                `MCQ ${position} Diagnostic Pathway step ${
                  stepIndex + 1
                }`
              )
          );

        const failures =
          explanation[
            "Why the Other Options Fail"
          ];

        if (
          !failures ||
          typeof failures !==
            "object" ||
          Array.isArray(failures)
        ) {
          throw new Error(
            `MCQ ${position} Why the Other Options Fail is invalid`
          );
        }

        const expectedWrongLetters =
          OPTION_LETTERS.filter(
            (letter) =>
              letter !==
              correctAnswer
          );

        const actualWrongLetters =
          Object.keys(failures);

        if (
          !sameStringSet(
            actualWrongLetters,
            expectedWrongLetters
          )
        ) {
          throw new Error(
            `MCQ ${position} incorrect-option keys must be ${expectedWrongLetters.join(
              ", "
            )}`
          );
        }

        const normalizedFailures = {};

        for (
          const letter of
          expectedWrongLetters
        ) {
          normalizedFailures[letter] =
            requireString(
              failures[letter],
              `MCQ ${position} explanation for option ${letter}`
            );
        }

        const examinerTrap =
          requireString(
            explanation[
              "Examiner's Trap"
            ],
            `MCQ ${position} Examiner's Trap`
          );

        return {
          Stem,
          A: options.A,
          B: options.B,
          C: options.C,
          D: options.D,
          "Correct Answer":
            correctAnswer,
          Explanation: {
            "Correct Answer Summary":
              correctSummary,
            "Diagnostic Pathway":
              diagnosticPathway,
            "Why the Other Options Fail":
              normalizedFailures,
            "Examiner's Trap":
              examinerTrap
          }
        };
      }
    );

  for (
    const letter of
    OPTION_LETTERS
  ) {
    if (
      answerDistribution[letter] < 2
    ) {
      throw new Error(
        `Correct Answer ${letter} appears fewer than 2 times`
      );
    }
  }

  return {
    output: {
      mcqs
    },
    mcqCount:
      mcqs.length,
    answerDistribution
  };
}

// ─────────────────────────────────────────────
// OPENAI GENERATION
// No max_output_tokens supplied.
// ─────────────────────────────────────────────

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

          input:
            buildUserInput(row),

          text: {
            format: {
              type: "json_schema",
              name:
                "neet_ss_pediatrics_mcqs",

              // Non-strict is required because
              // the three wrong-option keys vary.
              strict: false,

              schema:
                MCQ_SCHEMA
            }
          }
        });

      return validateAndNormalize(
        extractResponseText(response)
      );
    } catch (error) {
      lastError = error;

      if (
        isCreditExhaustionError(
          error
        )
      ) {
        throw error;
      }

      const validationError =
        /invalid JSON|one JSON object|exactly mcqs|exactly 10 MCQs|invalid fields|non-empty string|Stem has|duplicates another Stem|option .* words|duplicate options|invalid Correct Answer|Explanation is invalid|Diagnostic Pathway|Why the Other Options Fail|incorrect-option keys|Examiner's Trap|appears fewer than 2 times|Notes input/i.test(
          getErrorText(error)
        );

      const shouldRetry =
        isRetryableError(error) ||
        validationError;

      if (
        attempt === API_RETRIES ||
        !shouldRetry
      ) {
        break;
      }

      const delay =
        1000 * 2 ** attempt +
        Math.floor(
          Math.random() * 400
        );

      console.warn(
        `⚠️ Retry ${
          attempt + 1
        }/${API_RETRIES} after ${delay} ms: ${getErrorText(
          error
        )}`
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

// ─────────────────────────────────────────────
// RELEASE EXPIRED LOCKS
// Only pending MCQ rows are unlocked.
// ─────────────────────────────────────────────

async function releaseExpiredLocks() {
  const cutoff =
    new Date(
      Date.now() -
      LOCK_TTL_MIN *
        60 *
        1000
    ).toISOString();

  const { error } =
    await supabase
      .from(TABLE)
      .update({
        [LOCK_COL]: false,
        [LOCK_AT_COL]: null
      })
      .eq(
        LOCK_COL,
        true
      )
      .not(
        INPUT_COL,
        "is",
        null
      )
      .is(
        OUTPUT_COL,
        null
      )
      .lt(
        LOCK_AT_COL,
        cutoff
      );

  if (error) {
    throw new Error(
      `Failed to release expired Pediatrics MCQ locks: ${error.message}`
    );
  }
}

// ─────────────────────────────────────────────
// LOCK ONE ROW
// ─────────────────────────────────────────────

async function lockOneRow(row) {
  const lockedAt =
    new Date().toISOString();

  const { data, error } =
    await supabase
      .from(TABLE)
      .update({
        [LOCK_COL]: true,
        [LOCK_AT_COL]: lockedAt
      })
      .eq(
        "id",
        row.id
      )
      .eq(
        LOCK_COL,
        false
      )
      .not(
        INPUT_COL,
        "is",
        null
      )
      .is(
        OUTPUT_COL,
        null
      )
      .select(
        [
          "id",
          "subject",
          "serial_number",
          "topic",
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
// CLAIM AVAILABLE ROWS
// Picks only:
// notes_json IS NOT NULL
// mcq_json IS NULL
// generation_lock = false
// ─────────────────────────────────────────────

async function claimRows(limit) {
  await releaseExpiredLocks();

  const {
    data: availableRows,
    error
  } = await supabase
    .from(TABLE)
    .select(
      [
        "id",
        "serial_number"
      ].join(",")
    )
    .not(
      INPUT_COL,
      "is",
      null
    )
    .is(
      OUTPUT_COL,
      null
    )
    .eq(
      LOCK_COL,
      false
    )
    .order(
      "serial_number",
      {
        ascending: true
      }
    )
    .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending Pediatrics MCQ rows: ${error.message}`
    );
  }

  if (!availableRows?.length) {
    return [];
  }

  const lockResults =
    await Promise.allSettled(
      availableRows.map(
        (row) =>
          lockOneRow(row)
      )
    );

  const claimedRows = [];

  for (
    const result of
    lockResults
  ) {
    if (
      result.status ===
        "fulfilled" &&
      result.value
    ) {
      claimedRows.push(
        result.value
      );
    } else if (
      result.status ===
        "rejected"
    ) {
      console.error(
        "❌ Row-lock error:",
        getErrorText(
          result.reason
        )
      );
    }
  }

  return claimedRows;
}

// ─────────────────────────────────────────────
// SAVE SUCCESS
// Prevents overwriting existing mcq_json.
// ─────────────────────────────────────────────

async function saveSuccess(
  row,
  generatedOutput
) {
  const { data, error } =
    await supabase
      .from(TABLE)
      .update({
        [OUTPUT_COL]:
          generatedOutput,
        [LOCK_COL]:
          false,
        [LOCK_AT_COL]:
          null
      })
      .eq(
        "id",
        row.id
      )
      .eq(
        LOCK_COL,
        true
      )
      .eq(
        LOCK_AT_COL,
        row[LOCK_AT_COL]
      )
      .is(
        OUTPUT_COL,
        null
      )
      .select("id");

  if (error) {
    throw new Error(
      `Failed to save Pediatrics MCQs: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the row lock changed or MCQs already exist"
    );
  }
}

// ─────────────────────────────────────────────
// RELEASE OWNED LOCK
// ─────────────────────────────────────────────

async function releaseRowLock(row) {
  const { error } =
    await supabase
      .from(TABLE)
      .update({
        [LOCK_COL]: false,
        [LOCK_AT_COL]: null
      })
      .eq(
        "id",
        row.id
      )
      .eq(
        LOCK_COL,
        true
      )
      .eq(
        LOCK_AT_COL,
        row[LOCK_AT_COL]
      )
      .is(
        OUTPUT_COL,
        null
      );

  if (error) {
    console.error(
      `❌ Failed to release lock ${row.id}: ${error.message}`
    );
  }
}

async function releaseClaimedRows(
  rows
) {
  await Promise.allSettled(
    rows.map(
      (row) =>
        releaseRowLock(row)
    )
  );
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────

async function processRow(row) {
  console.log(
    `🧠 Generating Pediatrics MCQs | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateMcqs(row);

    await saveSuccess(
      row,
      result.output
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | MCQS=${result.mcqCount} | A=${result.answerDistribution.A} | B=${result.answerDistribution.B} | C=${result.answerDistribution.C} | D=${result.answerDistribution.D}`
    );

    return {
      creditExhausted: false
    };
  } catch (error) {
    await releaseRowLock(row);

    if (
      isCreditExhaustionError(
        error
      )
    ) {
      console.error(
        "🛑 OpenAI credits exhausted. Worker will stop safely."
      );

      return {
        creditExhausted: true
      };
    }

    console.error(
      `❌ Failed | ${row.subject} | ${row.serial_number} | ${row.topic}: ${getErrorText(
        error
      )}`
    );

    return {
      creditExhausted: false
    };
  }
}

// ─────────────────────────────────────────────
// CONTROLLED CONCURRENCY
// ─────────────────────────────────────────────

async function processWithConcurrency(
  rows
) {
  let nextIndex = 0;
  let creditExhausted = false;

  async function runner() {
    while (
      nextIndex < rows.length &&
      !creditExhausted
    ) {
      const currentIndex =
        nextIndex;

      nextIndex += 1;

      const result =
        await processRow(
          rows[currentIndex]
        );

      if (
        result.creditExhausted
      ) {
        creditExhausted = true;
      }
    }
  }

  const runnerCount =
    Math.min(
      BATCH_SIZE,
      rows.length
    );

  await Promise.all(
    Array.from(
      {
        length: runnerCount
      },
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
    `🚀 NEET SS PEDIATRICS MCQ WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE} | MCQs=${REQUIRED_MCQ_COUNT}`
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
        `📥 Claimed ${rows.length} Pediatrics notes set(s)`
      );

      const result =
        await processWithConcurrency(
          rows
        );

      if (
        result.creditExhausted
      ) {
        console.error(
          "🛑 Worker stopped because API credits are unavailable."
        );

        process.exit(1);
      }
    } catch (error) {
      if (
        isCreditExhaustionError(
          error
        )
      ) {
        console.error(
          "🛑 Worker stopped: OpenAI credits exhausted."
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
    "❌ Fatal Pediatrics MCQ worker error:",
    error
  );

  process.exit(1);
});
