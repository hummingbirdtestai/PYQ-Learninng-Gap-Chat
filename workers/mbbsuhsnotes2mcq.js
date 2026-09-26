"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// DATABASE CONFIGURATION
// notes_json → mcq_json
// ─────────────────────────────────────────────

const TABLE = "mbbs_pyt_source";
const INPUT_COL = "notes_json";
const OUTPUT_COL = "mcq_json";

const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

// ─────────────────────────────────────────────
// ENVIRONMENT CONFIGURATION
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
      `${name} must be an integer between ${min} and ${max}`
    );
  }

  return value;
}

const MODEL =
  process.env.MBBS_UHS_MCQ_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = parseIntegerEnv(
  "MBBS_UHS_MCQ_LIMIT",
  50,
  1,
  100
);

const BATCH_SIZE = parseIntegerEnv(
  "MBBS_UHS_MCQ_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "MBBS_UHS_MCQ_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "MBBS_UHS_MCQ_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = parseIntegerEnv(
  "MBBS_UHS_MCQ_API_RETRIES",
  2,
  0,
  5
);

const WORKER_ID =
  process.env.MBBS_UHS_MCQ_WORKER_ID ||
  `mbbs-uhs-mcq-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = `
You are an expert **KNRUHS/UHS MBBS University Examination MCQ writer** creating topic-wise MCQs for **uMedico UHS MBBS Distinction Rapid Revision**.

Your task is to convert the supplied **Rapid Revision Notes** into exactly **10 medically accurate, high-yield, university-exam-standard MCQs**.

These questions are intended primarily for **KNRUHS/UHS MBBS professional examinations**.

They are NOT primarily intended to imitate USMLE, NEET-PG, INI-CET, FMGE, or other postgraduate entrance examinations.

The learning sequence is:

**Rapid Revision Notes → Active Recall → UHS University MCQ Practice → Distinction**

The MCQs must reinforce the facts a student is expected to recall, recognize, differentiate, or apply in an MBBS university examination.

---

# INPUT

**Exam/Course:** {{EXAM}}

**Subject:** {{SUBJECT}}

**Topic / University Question:** {{TOPIC}}

**Rapid Revision Notes:**
{{NOTES_JSON}}

---

# 1. PRIMARY PURPOSE

Generate the **smallest useful set of 10 MCQs** that actively tests the most important examinable facts from the supplied topic.

The questions should help a student:

- Recall high-yield facts
- Recognize important concepts
- Differentiate commonly confused facts
- Apply knowledge to simple clinical situations
- Identify important investigations
- Recognize characteristic findings
- Select appropriate treatment where relevant
- Recognize adverse effects and contraindications
- Recognize important complications
- Prepare specifically for MBBS university MCQs

Do NOT turn straightforward MBBS facts into unnecessarily complicated clinical vignettes.

Difficulty should arise from **knowing the subject**, not from decoding an artificially complicated stem.

A well-prepared MBBS student should generally answer each question within **20–45 seconds**.

---

# 2. SOURCE-BOUND GENERATION

Every MCQ must be based primarily on the supplied **Rapid Revision Notes**.

Do NOT introduce unrelated textbook trivia merely to make questions difficult.

The supplied notes define the intended examinable scope.

You may use standard undergraduate MBBS knowledge only when necessary to construct medically valid distractors or make a supplied fact testable.

Do NOT expand the tested concept beyond the level represented in the notes.

Prioritize:

1. Core definition
2. Classification
3. Characteristic features
4. Important examples
5. Important mechanisms
6. Important investigations
7. Important treatment
8. Important adverse effects
9. Important contraindications
10. Important complications
11. Common clinical correlations
12. Frequently confused concepts
13. Examiner-favorite distinctions

Do not test obscure trivia while important facts from the supplied notes remain untested.

---

# 3. SUBJECT-ADAPTIVE QUESTION DISTRIBUTION

First internally classify the subject as either:

### A. PRECLINICAL / PARACLINICAL

Examples:

- Anatomy
- Physiology
- Biochemistry
- Pathology
- Pharmacology
- Microbiology
- Forensic Medicine
- Community Medicine

For these subjects generate:

**MCQ 1–5 → Direct University Recall**

**MCQ 6–8 → Applied University**

**MCQ 9–10 → Distinction Challenge**

Distribution:

**5 Direct + 3 Applied + 2 Distinction**

---

### B. CLINICAL

Examples:

- General Medicine
- General Surgery
- Obstetrics
- Gynaecology
- Pediatrics
- Ophthalmology
- ENT

For these subjects generate:

**MCQ 1–4 → Direct University Recall**

**MCQ 5–8 → Applied University**

**MCQ 9–10 → Distinction Challenge**

Distribution:

**4 Direct + 4 Applied + 2 Distinction**

---

# 4. DIRECT UNIVERSITY RECALL MCQs

These questions should resemble traditional **1-mark MBBS university MCQs**.

Test one important concept.

Appropriate targets include:

### Anatomy
- Boundaries
- Contents
- Relations
- Nerve supply
- Blood supply
- Attachments
- Actions
- Embryological origin
- Histological identification
- Applied anatomy

### Physiology
- Normal functions
- Regulatory mechanisms
- Normal values
- Reflexes
- Physiological responses
- Important curves
- Hormonal effects

### Biochemistry
- Enzymes
- Pathways
- Vitamins
- Deficiencies
- Metabolic products
- Inheritance
- Important biochemical reactions

### Pharmacology
- Classification
- Mechanism
- Drug of choice
- Therapeutic use
- Adverse effect
- Contraindication
- Important interaction

### Pathology
- Etiology
- Pathogenesis
- Morphology
- Characteristic finding
- Tumor marker
- Complication

### Microbiology
- Organism
- Transmission
- Virulence factor
- Diagnostic test
- Culture characteristic
- Prevention
- Treatment where relevant

### Community Medicine
- Definitions
- Epidemiological measures
- Screening
- Prevention levels
- National programs
- Vaccination
- Biostatistics

### Clinical Subjects
- Diagnostic feature
- Characteristic presentation
- Investigation of choice
- Treatment
- Drug of choice
- Complication
- Important diagnostic criterion
- Important threshold

Preferred stem length:

**8–25 words**

Examples of architecture:

"Which of the following...?"

"The most common cause of... is:"

"The nerve supply of... is:"

"Which drug inhibits...?"

"The investigation of choice for... is:"

"Which finding is characteristic of...?"

"Which of the following is NOT...?"

Use direct questions when direct recall is the appropriate university-level test.

Do NOT convert these questions into clinical vignettes merely for sophistication.

---

# 5. APPLIED UNIVERSITY MCQs

Applied MCQs should test whether the student can use a high-yield fact in a simple clinical or practical context.

Architecture:

**Short clinical/applied clue → recognize concept → answer**

Usually require **one meaningful inference**.

Examples:

Symptom pattern → diagnosis

Drug exposure → adverse effect

Disease → likely complication

Clinical finding → involved structure

Laboratory pattern → underlying disorder

Presentation → investigation

Presentation → initial treatment

Exposure → organism

Deficiency → manifestation

Trauma → anatomical structure involved

Keep the scenario concise.

Preferred stem length:

**15–40 words**

Every detail should contribute to solving the question.

Do NOT add irrelevant:

- Demographics
- Vital signs
- Laboratory values
- Imaging
- Past history
- Examination findings

unless they contribute directly to the tested concept.

---

# 6. DISTINCTION CHALLENGE MCQs

The final two questions should test deeper understanding while remaining strictly appropriate for an undergraduate MBBS university examination.

Architecture:

**Clinical/applied clue → identify concept → apply one additional fact**

These may require two linked facts.

Examples:

Recognize disease → identify complication

Recognize drug → identify contraindication

Recognize anatomical lesion → predict deficit

Recognize syndrome → choose investigation

Recognize laboratory pattern → determine mechanism

Recognize clinical condition → select appropriate management

Recognize pathology → predict consequence

The student should need to understand the topic rather than merely recognize one buzzword.

However:

**Do NOT convert these into USMLE/UWorld/NEET-PG-style multi-step puzzles.**

Do NOT require:

- Complex guideline algorithms
- Rare exceptions
- Specialty-level management
- Multiple competing diagnoses
- Advanced prognostic scores
- Obscure molecular details
- Postgraduate-level treatment decisions

unless explicitly present in the supplied notes.

Preferred stem length:

**20–50 words**

---

# 7. UNIVERSITY-REALISM RULE

Before accepting each question, ask:

**"Could this reasonably appear as a 1-mark MCQ in an MBBS university examination?"**

If YES → retain it.

If NO because it is too specialized, complicated, obscure, or postgraduate-oriented → rewrite it.

Questions should emphasize **core undergraduate knowledge**.

---

# 8. CLINICAL-SUBJECT RULE

For Medicine, Surgery, Obstetrics, Gynaecology, Pediatrics, Ophthalmology, and ENT, clinical application is encouraged.

However, keep cases concise.

Prefer:

**Presentation → diagnosis**

**Presentation → investigation**

**Presentation → treatment**

**Disease → complication**

**Drug → adverse effect**

**Clinical finding → mechanism**

over long diagnostic puzzles.

A clinical question should contain only the information required to solve it.

---

# 9. OPTIONS

Every MCQ must contain exactly four options:

**A, B, C, D**

Options must be:

- Medically plausible
- Mutually distinct
- Grammatically parallel
- Similar in specificity
- From the same conceptual category
- Concise whenever possible

Preferred option length:

**1–6 words**

Distractors should preferably represent:

- Common student confusions
- Closely related structures
- Drugs from the same class
- Similar diseases
- Similar organisms
- Related mechanisms
- Commonly confused values
- Related investigations

Avoid obviously absurd distractors.

There must be exactly **one best answer**.

---

# 10. CORRECT-ANSWER RANDOMIZATION

Randomize the correct answer across:

**A / B / C / D**

Do NOT repeatedly make the same option correct.

Across 10 questions, distribute correct answers reasonably among all four positions.

Avoid predictable patterns such as:

A, B, C, D, A, B, C, D...

Do not sacrifice medical accuracy merely to achieve numerical balance.

---

# 11. NEGATIVE-STEM QUESTIONS

Questions containing:

- NOT
- EXCEPT
- FALSE
- INCORRECT
- UNLIKELY

are permitted because they occur in university examinations.

However:

Maximum **2 negative-stem questions per 10 MCQs**.

The negative word must be written in CAPITAL LETTERS.

Avoid double negatives.

---

# 12. FACT-COVERAGE RULE

The 10 questions collectively should represent the **major examinable subtopics** within the supplied notes.

Do NOT generate multiple questions testing essentially the same fact while another major fact remains untested.

For example, if a pharmacology topic contains:

- Classification
- Mechanism
- Uses
- Adverse effects
- Contraindications

the MCQs should sample across these domains rather than generating five questions only about mechanism.

For a clinical topic containing:

- Etiology
- Clinical features
- Investigations
- Diagnosis
- Management
- Complications

the MCQs should similarly distribute coverage.

---

# 13. HIGH-YIELD PRIORITY RULE

When there are more facts than can be tested in 10 questions, prioritize:

**Must-know > commonly examined > clinically important > discriminating > minor detail**

The MCQ bank is intended for **rapid revision**, not exhaustive textbook assessment.

---

# 14. EXPLANATION STANDARD

Explanations must reinforce revision without becoming textbook paragraphs.

Every MCQ must contain:

### Correct Answer Summary

One concise sentence explaining why the answer is correct.

### Why the Other Options Fail

Give one concise, medically specific reason for EACH of the three incorrect options.

### Recall Pearl

Give one short, high-yield fact the student should retain for the university examination.

For straightforward direct questions, explanations should remain brief.

For Applied and Distinction questions, explanations may contain slightly more reasoning when necessary.

Do NOT provide lengthy USMLE-style diagnostic pathways.

---

# 15. STRICT EXPLANATION ALIGNMENT

Every explanation must be specifically written for that MCQ.

The explanation must correspond exactly to:

- Stem
- Option A
- Option B
- Option C
- Option D
- Correct Answer

Absolutely NO:

- Placeholder text
- Generic reasoning templates
- Copied explanations
- Content from another question
- Unrelated diseases
- Unrelated drugs
- Unrelated anatomy
- Truncated explanations

---

# 16. WHY OTHER OPTIONS FAIL — MANDATORY RULE

The "Why the Other Options Fail" object must contain exactly THREE keys corresponding to the incorrect options.

If "Correct Answer": "A":

Keys must be "B", "C", "D".

If "Correct Answer": "B":

Keys must be "A", "C", "D".

If "Correct Answer": "C":

Keys must be "A", "B", "D".

If "Correct Answer": "D":

Keys must be "A", "B", "C".

Each explanation must specifically explain why the actual option written under that letter is incorrect or inferior.

---

# 17. MEDICAL ACCURACY

All content must be consistent with standard undergraduate MBBS teaching.

Maintain textbook accuracy in:

- Anatomy
- Embryology
- Histology
- Physiology
- Biochemistry
- Pharmacology
- Pathology
- Microbiology
- Community Medicine
- Forensic Medicine
- General Medicine
- General Surgery
- Obstetrics
- Gynaecology
- Pediatrics
- Ophthalmology
- ENT

For treatment questions, use accepted undergraduate-standard management.

Do not introduce controversial, experimental, or highly specialized recommendations unless explicitly required by the supplied notes.

---

# 18. NO ARTIFICIAL DIFFICULTY

Reject and rewrite a question if difficulty comes primarily from:

- Excessively long stem
- Irrelevant clinical details
- Obscure trivia
- Ambiguous wording
- Multiple defensible answers
- Unnecessarily similar wording
- Advanced guideline minutiae
- Postgraduate-level knowledge outside supplied notes

Difficulty should come from **knowledge and application**, not confusion.

---

# 19. INTERNAL FINAL QUALITY AUDIT

Before producing the final JSON, internally verify every question.

### STRUCTURE

- Exactly 10 MCQs
- Exactly four options each
- Exactly one correct answer
- Valid difficulty label
- Valid JSON

### DISTRIBUTION

For preclinical/paraclinical subjects:

- 5 Direct
- 3 Applied
- 2 Distinction

For clinical subjects:

- 4 Direct
- 4 Applied
- 2 Distinction

### CONTENT

- Major notes adequately sampled
- No unnecessary repetition
- No major high-yield area ignored
- No obscure trivia replacing core knowledge
- Appropriate undergraduate level

### QUESTION QUALITY

- Direct questions are genuinely direct
- Applied questions require useful application
- Distinction questions require approximately two linked facts
- Clinical stems contain only discriminatory information
- Distractors are plausible
- Exactly one best answer

### EXPLANATION QUALITY

- Correct answer explanation is accurate
- All three incorrect options are explained
- Explanation letters exactly match option text
- Recall Pearl is useful and specific
- No placeholder or cross-question contamination

### UNIVERSITY REALISM

Finally ask:

**"Would these 10 questions be useful the night before a KNRUHS/UHS MBBS examination?"**

If any question is unnecessarily postgraduate, obscure, verbose, or low-yield, replace it with a more useful university-level question.

---

# 20. OUTPUT FORMAT

Return ONLY one valid JSON object.

Do NOT include Markdown fences.

Do NOT include introductory text.

Do NOT include concluding text.

Use exactly this structure:

{
  "mcqs": [
    {
      "Stem": "...",
      "A": "...",
      "B": "...",
      "C": "...",
      "D": "...",
      "Correct Answer": "A",
      "Difficulty": "Direct",
      "Explanation": {
        "Correct Answer Summary": "...",
        "Why the Other Options Fail": {
          "B": "...",
          "C": "...",
          "D": "..."
        },
        "Recall Pearl": "..."
      }
    }
  ]
}

Allowed "Difficulty" values are exactly:

- "Direct"
- "Applied"
- "Distinction"

The "mcqs" array must contain exactly 10 MCQ objects.

Return syntactically valid, directly parseable JSON only.
`.trim();

if (!SYSTEM_PROMPT) {
  throw new Error("SYSTEM_PROMPT cannot be empty");
}

// ─────────────────────────────────────────────
// STRUCTURED OUTPUT SCHEMA
//
// Why the Other Options Fail is represented as
// an array during generation because strict JSON
// Schema cannot conditionally require three keys
// based on Correct Answer.
//
// It is converted to the exact requested object
// before being saved.
// ─────────────────────────────────────────────

const MCQ_GENERATION_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["mcqs"],
  properties: {
    mcqs: {
      type: "array",
      minItems: 10,
      maxItems: 10,
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
          "Difficulty",
          "Explanation"
        ],
        properties: {
          Stem: {
            type: "string",
            minLength: 1
          },
          A: {
            type: "string",
            minLength: 1
          },
          B: {
            type: "string",
            minLength: 1
          },
          C: {
            type: "string",
            minLength: 1
          },
          D: {
            type: "string",
            minLength: 1
          },
          "Correct Answer": {
            type: "string",
            enum: ["A", "B", "C", "D"]
          },
          Difficulty: {
            type: "string",
            enum: [
              "Direct",
              "Applied",
              "Distinction"
            ]
          },
          Explanation: {
            type: "object",
            additionalProperties: false,
            required: [
              "Correct Answer Summary",
              "Why the Other Options Fail",
              "Recall Pearl"
            ],
            properties: {
              "Correct Answer Summary": {
                type: "string",
                minLength: 1
              },
              "Why the Other Options Fail": {
                type: "array",
                minItems: 3,
                maxItems: 3,
                items: {
                  type: "object",
                  additionalProperties: false,
                  required: [
                    "Option",
                    "Reason"
                  ],
                  properties: {
                    Option: {
                      type: "string",
                      enum: [
                        "A",
                        "B",
                        "C",
                        "D"
                      ]
                    },
                    Reason: {
                      type: "string",
                      minLength: 1
                    }
                  }
                }
              },
              "Recall Pearl": {
                type: "string",
                minLength: 1
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

function requireString(value, label) {
  const normalized =
    String(value ?? "").trim();

  if (!normalized) {
    throw new Error(
      `${label} must be a non-empty string`
    );
  }

  return normalized;
}

function serializeJson(value) {
  if (typeof value === "string") {
    return value;
  }

  return JSON.stringify(value, null, 2);
}

function isClinicalSubject(subject) {
  const normalized =
    String(subject || "")
      .trim()
      .toLowerCase();

  return [
    "general medicine",
    "medicine",
    "general surgery",
    "surgery",
    "obstetrics",
    "gynaecology",
    "gynecology",
    "pediatrics",
    "paediatrics",
    "ophthalmology",
    "ent"
  ].includes(normalized);
}

// ─────────────────────────────────────────────
// BUILD INPUT
// ─────────────────────────────────────────────

function buildUserInput(row) {
  return [
    `EXAM/COURSE: ${
      row.exam ||
      "KNRUHS/UHS MBBS University Examination"
    }`,
    `SUBJECT: ${row.subject}`,
    `TOPIC / UNIVERSITY QUESTION: ${row.topic}`,
    `SUBJECT TYPE: ${
      isClinicalSubject(row.subject)
        ? "CLINICAL"
        : "PRECLINICAL_OR_PARACLINICAL"
    }`,
    "",
    "RAPID REVISION NOTES:",
    serializeJson(row[INPUT_COL]),
    "",
    "Generate exactly 10 database-ready MCQs now.",
    "",
    "IMPORTANT GENERATION FORMAT:",
    "Inside Why the Other Options Fail, return an array of exactly three objects.",
    'Each object must have "Option" and "Reason".',
    "Include only the three incorrect option letters.",
    "The worker will convert this array into the final keyed object before saving."
  ].join("\n");
}

// ─────────────────────────────────────────────
// RESPONSE EXTRACTION
// ─────────────────────────────────────────────

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

  const text = collected.join("\n").trim();

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
// VALIDATION AND NORMALIZATION
// ─────────────────────────────────────────────

function validateAndNormalize(
  rawOutput,
  subject
) {
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
    !Array.isArray(parsed.mcqs) ||
    parsed.mcqs.length !== 10
  ) {
    throw new Error(
      `Generated output must contain exactly 10 MCQs; received ${
        Array.isArray(parsed.mcqs)
          ? parsed.mcqs.length
          : 0
      }`
    );
  }

  const allowedAnswers =
    new Set(["A", "B", "C", "D"]);

  const allowedDifficulties =
    new Set([
      "Direct",
      "Applied",
      "Distinction"
    ]);

  const seenStems = new Set();

  const difficultyCounts = {
    Direct: 0,
    Applied: 0,
    Distinction: 0
  };

  const answerCounts = {
    A: 0,
    B: 0,
    C: 0,
    D: 0
  };

  let negativeStemCount = 0;

  const mcqs = parsed.mcqs.map(
    (mcq, index) => {
      const position = index + 1;

      if (
        !mcq ||
        typeof mcq !== "object" ||
        Array.isArray(mcq)
      ) {
        throw new Error(
          `MCQ ${position} is not an object`
        );
      }

      const stem = requireString(
        mcq.Stem,
        `MCQ ${position} Stem`
      );

      const stemKey = stem
        .replace(/\s+/g, " ")
        .toLowerCase();

      if (seenStems.has(stemKey)) {
        throw new Error(
          `MCQ ${position} duplicates another stem`
        );
      }

      seenStems.add(stemKey);

      if (
        /\b(NOT|EXCEPT|FALSE|INCORRECT|UNLIKELY)\b/.test(
          stem
        )
      ) {
        negativeStemCount += 1;
      }

      const options = {
        A: requireString(
          mcq.A,
          `MCQ ${position} option A`
        ),
        B: requireString(
          mcq.B,
          `MCQ ${position} option B`
        ),
        C: requireString(
          mcq.C,
          `MCQ ${position} option C`
        ),
        D: requireString(
          mcq.D,
          `MCQ ${position} option D`
        )
      };

      const normalizedOptions =
        Object.values(options).map(
          (option) =>
            option
              .replace(/\s+/g, " ")
              .toLowerCase()
        );

      if (
        new Set(normalizedOptions).size !== 4
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
        !allowedAnswers.has(correctAnswer)
      ) {
        throw new Error(
          `MCQ ${position} has invalid Correct Answer`
        );
      }

      answerCounts[correctAnswer] += 1;

      const difficulty =
        requireString(
          mcq.Difficulty,
          `MCQ ${position} Difficulty`
        );

      if (
        !allowedDifficulties.has(difficulty)
      ) {
        throw new Error(
          `MCQ ${position} has invalid Difficulty`
        );
      }

      difficultyCounts[difficulty] += 1;

      const explanation =
        mcq.Explanation;

      if (
        !explanation ||
        typeof explanation !== "object" ||
        Array.isArray(explanation)
      ) {
        throw new Error(
          `MCQ ${position} Explanation is invalid`
        );
      }

      const correctSummary =
        requireString(
          explanation[
            "Correct Answer Summary"
          ],
          `MCQ ${position} Correct Answer Summary`
        );

      const recallPearl =
        requireString(
          explanation["Recall Pearl"],
          `MCQ ${position} Recall Pearl`
        );

      const failureItems =
        explanation[
          "Why the Other Options Fail"
        ];

      if (
        !Array.isArray(failureItems) ||
        failureItems.length !== 3
      ) {
        throw new Error(
          `MCQ ${position} must explain exactly three incorrect options`
        );
      }

      const expectedIncorrectLetters =
        ["A", "B", "C", "D"].filter(
          (letter) =>
            letter !== correctAnswer
        );

      const failureObject = {};

      for (
        let failureIndex = 0;
        failureIndex <
        failureItems.length;
        failureIndex += 1
      ) {
        const item =
          failureItems[failureIndex];

        if (
          !item ||
          typeof item !== "object" ||
          Array.isArray(item)
        ) {
          throw new Error(
            `MCQ ${position} incorrect-option explanation ${
              failureIndex + 1
            } is invalid`
          );
        }

        const letter =
          requireString(
            item.Option,
            `MCQ ${position} incorrect option letter`
          ).toUpperCase();

        const reason =
          requireString(
            item.Reason,
            `MCQ ${position} reason for option ${letter}`
          );

        if (
          !allowedAnswers.has(letter)
        ) {
          throw new Error(
            `MCQ ${position} explanation has invalid option ${letter}`
          );
        }

        if (letter === correctAnswer) {
          throw new Error(
            `MCQ ${position} explains the correct option as incorrect`
          );
        }

        if (failureObject[letter]) {
          throw new Error(
            `MCQ ${position} repeats explanation for option ${letter}`
          );
        }

        failureObject[letter] = reason;
      }

      const actualIncorrectLetters =
        Object.keys(failureObject).sort();

      if (
        actualIncorrectLetters.join(",") !==
        expectedIncorrectLetters
          .slice()
          .sort()
          .join(",")
      ) {
        throw new Error(
          `MCQ ${position} incorrect-option explanation letters do not match the answer`
        );
      }

      const orderedFailureObject = {};

      for (
        const letter of
        expectedIncorrectLetters
      ) {
        orderedFailureObject[letter] =
          failureObject[letter];
      }

      return {
        Stem: stem,
        A: options.A,
        B: options.B,
        C: options.C,
        D: options.D,
        "Correct Answer": correctAnswer,
        Difficulty: difficulty,
        Explanation: {
          "Correct Answer Summary":
            correctSummary,
          "Why the Other Options Fail":
            orderedFailureObject,
          "Recall Pearl":
            recallPearl
        }
      };
    }
  );

  if (negativeStemCount > 2) {
    throw new Error(
      `Generated ${negativeStemCount} negative-stem MCQs; maximum is 2`
    );
  }

  const clinical =
    isClinicalSubject(subject);

  const expectedCounts = clinical
    ? {
        Direct: 4,
        Applied: 4,
        Distinction: 2
      }
    : {
        Direct: 5,
        Applied: 3,
        Distinction: 2
      };

  for (
    const difficulty of
    Object.keys(expectedCounts)
  ) {
    if (
      difficultyCounts[difficulty] !==
      expectedCounts[difficulty]
    ) {
      throw new Error(
        `Invalid difficulty distribution: expected ${expectedCounts.Direct} Direct, ${expectedCounts.Applied} Applied and ${expectedCounts.Distinction} Distinction`
      );
    }
  }

  for (
    const letter of
    ["A", "B", "C", "D"]
  ) {
    if (answerCounts[letter] === 0) {
      throw new Error(
        `Correct-answer distribution does not use option ${letter}`
      );
    }
  }

  return {
    output: {
      mcqs
    },
    directCount:
      difficultyCounts.Direct,
    appliedCount:
      difficultyCounts.Applied,
    distinctionCount:
      difficultyCounts.Distinction,
    negativeStemCount,
    answerCounts
  };
}

// ─────────────────────────────────────────────
// OPENAI GENERATION
// No max_output_tokens supplied
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
                "mbbs_uhs_mcqs",
              strict: true,
              schema:
                MCQ_GENERATION_SCHEMA
            }
          }
        });

      return validateAndNormalize(
        extractResponseText(response),
        row.subject
      );
    } catch (error) {
      lastError = error;

      if (
        isCreditExhaustionError(error)
      ) {
        throw error;
      }

      const validationError =
        /invalid JSON|exactly 10 MCQs|not an object|duplicate|invalid Correct Answer|invalid Difficulty|Explanation is invalid|incorrect options|negative-stem|difficulty distribution|Correct-answer distribution|non-empty string/i.test(
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

  throw lastError;
}

// ─────────────────────────────────────────────
// RELEASE EXPIRED LOCKS
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
      `Failed to release expired MBBS MCQ locks: ${error.message}`
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
          "exam",
          "course_id",
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
        "subject",
        "serial_number",
        "topic"
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
      `Failed to find pending MBBS MCQ rows: ${error.message}`
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

  for (const result of lockResults) {
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
      `Failed to save MBBS MCQs: ${error.message}`
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

async function releaseClaimedRows(rows) {
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
    `📝 Generating UHS MCQs | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateMcqs(row);

    await saveSuccess(
      row,
      result.output
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | DIRECT=${result.directCount} | APPLIED=${result.appliedCount} | DISTINCTION=${result.distinctionCount} | ANSWERS=A:${result.answerCounts.A},B:${result.answerCounts.B},C:${result.answerCounts.C},D:${result.answerCounts.D}`
    );

    return {
      creditExhausted: false
    };
  } catch (error) {
    await releaseRowLock(row);

    if (
      isCreditExhaustionError(error)
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
    `🚀 MBBS UHS MCQ WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE} | MCQs=10`
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
        `📥 Claimed ${rows.length} MBBS topic(s)`
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
        isCreditExhaustionError(error)
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
    "❌ Fatal MBBS UHS MCQ worker error:",
    error
  );

  process.exit(1);
});
