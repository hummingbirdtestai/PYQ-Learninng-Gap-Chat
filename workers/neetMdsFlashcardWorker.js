"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// DATABASE CONFIGURATION
// ─────────────────────────────────────────────

const TABLE = "neet_mds_pyt_source";

const INPUT_COL = "pyq_content";
const OUTPUT_COL = "jsonb_output";

const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

const MAX_FUTURE_CARDS = 20;

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
  process.env.NEET_MDS_FLASHCARD_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = parseIntegerEnv(
  "NEET_MDS_FLASHCARD_LIMIT",
  50,
  1,
  100
);

const BATCH_SIZE = parseIntegerEnv(
  "NEET_MDS_FLASHCARD_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "NEET_MDS_FLASHCARD_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "NEET_MDS_FLASHCARD_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = parseIntegerEnv(
  "NEET_MDS_FLASHCARD_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = parseIntegerEnv(
  "NEET_MDS_FLASHCARD_MAX_OUTPUT_TOKENS",
  16000,
  4000,
  30000
);

const WORKER_ID =
  process.env.NEET_MDS_FLASHCARD_WORKER_ID ||
  `neet-mds-flashcard-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// EMBEDDED SYSTEM PROMPT
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = `
You are a Senior NEET MDS / INI-CET MDS / NBDE-INBDE Dental Examiner and Dental Medical Educator with expert command of all 20 NEET MDS subjects.

TASK
Convert the supplied NEET MDS Previous Year Questions (PYQs) belonging to ONE PYT into a high-accuracy, deduplicated PYQ + Future-PYQ Flashcard Bank.

The bank must represent:
1. What NEET MDS has already asked.
2. What NEET MDS / INI-CET MDS can logically ask next from the SAME PYT.

Quality must approximate NEET MDS, INI-CET MDS, NBDE/INBDE and high-quality AMBOSS-style dental reasoning while remaining faithful to the Indian dental examination context.

PRIMARY OBJECTIVE
1. Identify the actual tested concepts.
2. Remove genuine repetitions.
3. Merge repeated PYQs testing the same fact/concept.
4. Preserve ALL years in which the concept was asked.
5. Retain distinct PYQs when they test meaningfully different facts.
6. Correct malformed historical wording ONLY when the intended concept is confidently recoverable.
7. Never invent content for unrecoverable historical PYQs.
8. Upgrade overly direct historical PYQs into high-quality NBDE/INBDE/AMBOSS-style clinical questions while preserving the SAME tested concept and historical provenance.
9. Generate a proportionate Future NEET MDS Q→A flashcard set covering probable extensions of the same PYT.
10. Generate NO MORE THAN 20 FUTURE_PREDICTED cards for any PYT.
11. Expand selectively into clinical application, differentiation, investigation, treatment, anatomy, pathology, radiology, dental materials, complications, mechanisms and examiner traps only where genuinely relevant.
12. Return ONLY valid JSON.

HISTORICAL PYQ INTEGRITY
Classify each input PYQ as VALID DISTINCT PYQ, DUPLICATE/CONCEPT-EQUIVALENT, or UNRECOVERABLE/INSUFFICIENT SOURCE.
Entries such as "Question/answer not recoverable", "Not stated confidently", or equivalent contain no reliable examinable content. Do not turn them into historical flashcards. Count them as unrecoverable.
Never manufacture an answer, stem, image finding, or examination year.

CONCEPTUAL DEDUPLICATION
Deduplicate by tested knowledge, not wording alone.
Example: "Which muscle protrudes the mandible?" and "Protrusion is primarily caused by which muscle?" test the same fact and should merge.
But "Lateral pterygoid → protrusion" and "Lateral pterygoid → depression/opening" test different functional relationships and must remain separate even if the answer is identical.
Do not over-merge questions differing in qualifier, mechanism, relationship, diagnosis, investigation, treatment, complication, anatomy, or exception.

YEAR CONSOLIDATION
For retained historical cards:
"years_asked": ["2017","2022"]
Use chronological order and never invent years.
For future cards:
"years_asked": []
"source_type": "FUTURE_PREDICTED"
Historical cards:
"source_type": "PYQ"

FUTURE NEET MDS EXPANSION
Identify the knowledge neighbourhood surrounding the PYT. Future cards must extend the examiner's testing axis rather than paraphrase existing PYQs.
Use only scientifically relevant dimensions: definition; most/least common; anatomy; origin/insertion; nerve/blood supply; action; relations; development; histology; physiology; biochemistry; pathogenesis; etiology; presentation; signs; differential diagnosis; investigation/gold standard; radiology; histopathology; markers; classification/staging/grading; treatment/next step; indications/contraindications; drugs/mechanisms/adverse effects; complications; prognosis/recurrence; surgical approach/anatomy; operative complications; dental-material composition/properties/setting/manipulation/failure; restorative/endodontic/periodontal/orthodontic/prosthodontic/pediatric decisions; oral pathology/radiology/medicine/surgery; medical emergencies; systemic disease–oral manifestations; drug–dental interactions; infection control; public-health calculations; exceptions and examiner traps.
Never force irrelevant dimensions merely to increase card count.

QUESTION DEPTH
Use a deliberate mixture:
LEVEL 1 — Core Recall: direct high-yield facts appropriate to NEET MDS.
LEVEL 2 — Integrated Application: link at least two facts.
LEVEL 3 — Clinical Discrimination: 2–3 linked reasoning steps resembling difficult NEET MDS / INI-CET MDS / NBDE-INBDE reasoning.

For Level 2/3, avoid association-only questions when the topic permits deeper testing. Recognition of a classic clue should identify the broad state; an additional discriminator should determine the answer.
Useful discriminators: site, age, tooth vitality, crown/root relationship, radiographic border, histology, aspiration, recurrence, systemic association, pulpal/periodontal status, severity, timing, contraindication, treatment history, anatomy, material property, force system, growth status, drug interaction, organ function, or guideline threshold.

STRICT ANTI-GIVEAWAY / TRUE 3-LEVEL CHALLENGE RULE:
- A card labeled "CLINICAL_DISCRIMINATION" must genuinely require at least 2–3 sequential reasoning decisions; a long clinical stem alone does NOT make a question Level 3.
- The answer must not be obtainable merely by matching one giveaway keyword, phrase, classic association, or definition in the stem.
- Whenever the topic permits, construct the reasoning pathway as:
  Level 1: identify the relevant clinical/dental state or problem;
  Level 2: interpret a meaningful discriminator such as vitality, root maturity, site, imaging, histology, mechanism, material property, treatment history, timing, anatomy, complication, or contraindication;
  Level 3: distinguish between plausible competing diagnoses, investigations, procedures, materials, mechanisms, or management choices to reach the single best answer.
- Include only discriminators that materially affect the answer. Do not artificially lengthen stems or add irrelevant details merely to simulate difficulty.
- If a question can still be solved from one obvious clue, classify it as "CORE_RECALL" or "INTEGRATED_APPLICATION" rather than "CLINICAL_DISCRIMINATION".
- For topics with natural competing choices, deliberately test discrimination between those alternatives rather than restating the defining feature of one answer.
- Examples of appropriate discrimination axes include: concussion vs subluxation vs luxation; apexogenesis vs apexification vs regenerative endodontics; vital vs non-vital bleaching; conventional vs supplemental anesthesia; diagnosis vs next-best treatment; similar lesions/materials/procedures with one decisive differentiator.
- Do not change the historical concept or correct answer merely to create difficulty. For upgraded PYQs, increase reasoning only through scientifically valid context and discriminators surrounding the SAME original tested knowledge point.

CLINICAL VIGNETTES
When clinically applicable, use concise, information-dense dental vignettes:
context → complaint → relevant examination → tooth/periodontal status → relevant imaging/investigation → neutral question.
Every detail must have discriminatory value. Do not add decorative demographics, routine normal findings, irrelevant labs, or unnecessary history.

SUBJECT-SPECIFIC ACCURACY
Oral Radiology: preserve exact radiolucent/radiopaque pattern, locularity, border, crown/root relationship, displacement/resorption, tooth association, site, midline behavior, periosteal reaction, calcification and landmarks. Never invent imaging findings.
Oral Pathology: integrate site, age, clinical presentation, radiology, histology, molecular change, behavior, recurrence, malignant potential and management where applicable.
Dental Materials: connect composition → setting reaction → manipulation → property → clinical consequence. Distinguish laboratory properties from clinical effects.
Prosthodontics: accurately test support, stability, retention, impressions, border molding, occlusion, articulators, jaw relations, facebow, vertical dimension, centric relation, pontics, connectors, finish lines, biomechanics, Kennedy classification, surveying and implants.
Conservative Dentistry/Endodontics: integrate symptoms → pulpal/periapical diagnosis → vitality → radiology → restorability → treatment → complication/rescue. Preserve diagnostic terminology and root-development status.
Periodontology: distinguish probing depth, CAL, inflammation, bone loss, mobility, furcation, risk modifiers, staging/grading and management. Never infer CAL solely from probing depth.
Orthodontics: keep growth, skeletal/dental relationships, cephalometrics, biomechanics, force systems, anchorage, appliance choice and timing internally consistent.
Pedodontics: integrate age, dentition/development, vitality, trauma/caries, radiology, behavior and treatment. Distinguish primary/permanent teeth and open/closed apex.
OMFS: integrate presentation → anatomy/imaging → diagnosis → indication → approach → structure at risk → complication/rescue. Anatomical relationships must be exact.
Public Health Dentistry: calculations and epidemiological concepts must be mathematically correct. Never interchange incidence/prevalence, sensitivity/specificity, or predictive values.
Basic Medical Sciences: maintain textbook accuracy and use dental relevance only when scientifically natural.

QUESTION/ANSWER RULES
Each card must have ONE unambiguous best answer.
Answers should usually be 1–8 words and must not contain mini-explanations.
Questions must be independently understandable.
Do not use "Which of the following?" without options.
Do not reference unavailable images/tables/diagrams.
Historical PYQs must retain the original tested concept, answer and year provenance. However, when an input PYQ is overly direct, one-line, recall-only, or association-based, CONVERT its question stem into an NBDE/INBDE/AMBOSS-standard clinical or applied vignette while still labeling it "PYQ".

STRICT HISTORICAL PYQ UPGRADE RULE:
- Preserve exactly the knowledge point tested by the original PYQ.
- Do not change the correct answer merely to make the question harder.
- Do not add a second independent concept that changes what is being tested.
- When clinically applicable, target approximately 30–45 words for the upgraded stem.
- Build approximately 3 levels of thinking:
  Level 1: recognize the clinical/dental context;
  Level 2: interpret a relevant discriminator, mechanism, investigation, material property, radiographic clue, pathology finding, or treatment context;
  Level 3: identify the original tested concept as the single best answer.
- Every added detail must be scientifically correct and relevant to the original concept.
- Do not add decorative demographics, irrelevant normal findings, or invented patient details that imply unsupported historical facts.
- If the original concept is inherently nonclinical, use a 30–45-word applied laboratory, material-science, anatomical, radiographic, pathological, procedural, or decision-based stem instead of forcing a patient vignette.
- The upgraded wording is an educational reconstruction of the historical tested concept; "years_asked" records provenance of the concept, not a claim that the reconstructed wording appeared verbatim in the examination.

FUTURE-PYQ RULE
Future cards are examiner-style predicted questions, never represented as actual PYQs.
Prioritize high-yield extensions that test concepts a strong dental examiner could reasonably derive from the supplied PYT.
Avoid low-value trivia, obscure facts without examination relevance, redundant paraphrases, and multiple cards testing essentially the same fact.

SENSE OF PROPORTION — STRICT
- Maximum FUTURE_PREDICTED cards per PYT = 20. Never exceed 20.
- Twenty is a hard ceiling, NOT a target.
- Generate fewer than 20 whenever the knowledge neighbourhood is adequately covered with fewer cards.
- Scale expansion to the breadth and importance of the supplied PYQs.
- A narrow PYT should receive a narrow future bank; do not inflate a small concept into an encyclopedic topic review.
- Prefer 8–15 strong future cards for a moderately broad PYT when that is sufficient.
- Use 16–20 only when several genuinely distinct, high-yield testing axes are directly supported by the PYT.
- Stop generating when additional cards would become trivia, remote associations, repetition, or marginally useful examination content.
- Depth is more important than card count.

ACCURACY & QUALITY AUDIT
Before output, verify every card:
- belongs to the supplied PYT;
- has one defensible answer;
- contains no fabricated historical provenance;
- is not conceptually duplicated;
- uses correct dental/medical terminology;
- has internally consistent clinical/radiographic/anatomical details;
- is appropriate for NEET MDS/INI-CET MDS;
- contains no placeholder text.
If uncertain about a historical question, classify it as unrecoverable rather than guessing.

OUTPUT — STRICT JSON ONLY
Return exactly ONE valid JSON object and nothing outside it:

{
  "topic": "Exact PYT/topic",
  "cards": [
    {
      "serial_number": 1,
      "source_type": "PYQ",
      "years_asked": ["2017"],
      "difficulty": "CLINICAL_DISCRIMINATION",
      "subtopic": "Exact subtopic",
      "question": "Approximately 30–45-word NBDE/INBDE/AMBOSS-style applied or clinical stem preserving the exact historical tested concept",
      "answer": "Concise answer"
    },
    {
      "serial_number": 2,
      "source_type": "FUTURE_PREDICTED",
      "years_asked": [],
      "difficulty": "CLINICAL_DISCRIMINATION",
      "subtopic": "Exact subtopic",
      "question": "High-yield predicted NEET MDS / INI-CET MDS question",
      "answer": "Concise answer"
    }
  ],
  "unrecoverable_pyqs": [
    {
      "year_asked": "2018",
      "reason": "Question/answer not reliably recoverable from supplied source"
    }
  ]
}

ALLOWED "difficulty" VALUES ONLY:
"CORE_RECALL"
"INTEGRATED_APPLICATION"
"CLINICAL_DISCRIMINATION"

MECHANICAL JSON RULES
- Valid JSON only; double quotes around all keys and string values.
- No Markdown fences, comments, preamble, explanation, or trailing text.
- DO NOT output "source_pyq_summary" or any counts/summary object.
- "serial_number" must be sequential starting at 1.
- PYQ cards must appear before FUTURE_PREDICTED cards.
- Historical duplicate years must be consolidated into one "years_asked" array.
- Maximum number of source_type="FUTURE_PREDICTED" cards is 20.
- Twenty future cards is a ceiling, not a quota.
- Never output unrecoverable source material as a card.
- Never create placeholder cards merely to reach a fixed count.
- For overly direct historical PYQs, upgrade the stem to approximately 30–45 words and 3-level applied reasoning while preserving the exact original tested concept and answer.
- Do not falsely imply that reconstructed clinical wording was the verbatim historical examination stem.
- Maintain sense of proportion: comprehensively cover only the high-yield testing neighbourhood justified by the supplied PYT.
`.trim();

if (!SYSTEM_PROMPT) {
  throw new Error(
    "SYSTEM_PROMPT cannot be empty"
  );
}

// ─────────────────────────────────────────────
// STRUCTURED OUTPUT SCHEMA
// ─────────────────────────────────────────────

const FLASHCARD_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: [
    "topic",
    "cards",
    "unrecoverable_pyqs"
  ],
  properties: {
    topic: {
      type: "string",
      minLength: 1
    },
    cards: {
      type: "array",
      minItems: 1,
      items: {
        type: "object",
        additionalProperties: false,
        required: [
          "serial_number",
          "source_type",
          "years_asked",
          "difficulty",
          "subtopic",
          "question",
          "answer"
        ],
        properties: {
          serial_number: {
            type: "integer",
            minimum: 1
          },
          source_type: {
            type: "string",
            enum: [
              "PYQ",
              "FUTURE_PREDICTED"
            ]
          },
          years_asked: {
            type: "array",
            items: {
              type: "string"
            }
          },
          difficulty: {
            type: "string",
            enum: [
              "CORE_RECALL",
              "INTEGRATED_APPLICATION",
              "CLINICAL_DISCRIMINATION"
            ]
          },
          subtopic: {
            type: "string",
            minLength: 1
          },
          question: {
            type: "string",
            minLength: 1
          },
          answer: {
            type: "string",
            minLength: 1
          }
        }
      }
    },
    unrecoverable_pyqs: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        required: [
          "year_asked",
          "reason"
        ],
        properties: {
          year_asked: {
            type: "string"
          },
          reason: {
            type: "string",
            minLength: 1
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

function countWords(value) {
  return String(value)
    .trim()
    .split(/\s+/u)
    .filter(Boolean)
    .length;
}

function normalizeYear(value) {
  return String(value || "")
    .trim();
}

function serializeJson(value) {
  if (typeof value === "string") {
    return value;
  }

  return JSON.stringify(
    value,
    null,
    2
  );
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
    `SOURCE EXAM: ${row.exam || "Not separately supplied"}`,
    `SOURCE YEAR: ${row.year_asked || "Years contained in PYQ JSON"}`,
    `SOURCE PYT: ${row.pyt || row.topic}`,
    `SOURCE SUBTOPIC CLASSIFICATION: ${row.subtopic_classification || "Not separately supplied"}`,
    "",
    "SUPPLIED NEET MDS PYQ JSON:",
    serializeJson(row[INPUT_COL])
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

  for (
    const outputItem of
    response?.output || []
  ) {
    for (
      const contentItem of
      outputItem?.content || []
    ) {
      if (
        contentItem?.type === "output_text" &&
        typeof contentItem.text === "string"
      ) {
        collected.push(
          contentItem.text
        );
      }
    }
  }

  const text = collected
    .join("\n")
    .trim();

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
// OUTPUT VALIDATION
// ─────────────────────────────────────────────

function requireString(
  value,
  label,
  allowEmpty = false
) {
  const normalized =
    String(value ?? "").trim();

  if (
    !allowEmpty &&
    !normalized
  ) {
    throw new Error(
      `${label} must be a non-empty string`
    );
  }

  return normalized;
}

function validateAndNormalize(
  rawOutput,
  expectedTopic
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
      "Generated output must be one JSON object"
    );
  }

  if (
    !Array.isArray(parsed.cards) ||
    parsed.cards.length === 0
  ) {
    throw new Error(
      "Generated output contains no cards"
    );
  }

  if (
    !Array.isArray(
      parsed.unrecoverable_pyqs
    )
  ) {
    throw new Error(
      "Generated output is missing unrecoverable_pyqs array"
    );
  }

  let futureCount = 0;
  let pyqCount = 0;
  let futureSectionStarted = false;

  const seenQuestions =
    new Set();

  const cards =
    parsed.cards.map(
      (card, index) => {
        const position =
          index + 1;

        if (
          !card ||
          typeof card !== "object" ||
          Array.isArray(card)
        ) {
          throw new Error(
            `Card ${position} is not an object`
          );
        }

        if (
          Number(card.serial_number) !==
          position
        ) {
          throw new Error(
            `Card ${position} must have serial_number ${position}`
          );
        }

        const sourceType =
          requireString(
            card.source_type,
            `Card ${position} source_type`
          ).toUpperCase();

        if (
          sourceType !== "PYQ" &&
          sourceType !==
            "FUTURE_PREDICTED"
        ) {
          throw new Error(
            `Card ${position} has invalid source_type`
          );
        }

        if (
          sourceType ===
          "FUTURE_PREDICTED"
        ) {
          futureSectionStarted = true;
          futureCount += 1;
        } else {
          if (
            futureSectionStarted
          ) {
            throw new Error(
              `PYQ card ${position} appears after FUTURE_PREDICTED cards`
            );
          }

          pyqCount += 1;
        }

        if (
          futureCount >
          MAX_FUTURE_CARDS
        ) {
          throw new Error(
            `Generated ${futureCount} future cards; maximum is ${MAX_FUTURE_CARDS}`
          );
        }

        if (
          !Array.isArray(
            card.years_asked
          )
        ) {
          throw new Error(
            `Card ${position} years_asked must be an array`
          );
        }

        const yearsAsked =
          [
            ...new Set(
              card.years_asked
                .map(normalizeYear)
                .filter(Boolean)
            )
          ].sort(
            (a, b) =>
              a.localeCompare(
                b,
                undefined,
                {
                  numeric: true
                }
              )
          );

        if (
          sourceType === "PYQ" &&
          yearsAsked.length === 0
        ) {
          throw new Error(
            `PYQ card ${position} has no historical year`
          );
        }

        if (
          sourceType ===
            "FUTURE_PREDICTED" &&
          yearsAsked.length !== 0
        ) {
          throw new Error(
            `Future card ${position} must have an empty years_asked array`
          );
        }

        const difficulty =
          requireString(
            card.difficulty,
            `Card ${position} difficulty`
          ).toUpperCase();

        if (
          ![
            "CORE_RECALL",
            "INTEGRATED_APPLICATION",
            "CLINICAL_DISCRIMINATION"
          ].includes(difficulty)
        ) {
          throw new Error(
            `Card ${position} has invalid difficulty`
          );
        }

        const subtopic =
          requireString(
            card.subtopic,
            `Card ${position} subtopic`
          );

        const question =
          requireString(
            card.question,
            `Card ${position} question`
          );

        const answer =
          requireString(
            card.answer,
            `Card ${position} answer`
          );

        const answerWords =
          countWords(answer);

        if (
          answerWords < 1 ||
          answerWords > 8
        ) {
          throw new Error(
            `Card ${position} answer has ${answerWords} words; required range is 1–8`
          );
        }

        const questionKey =
          question
            .replace(/\s+/g, " ")
            .toLowerCase();

        if (
          seenQuestions.has(
            questionKey
          )
        ) {
          throw new Error(
            `Card ${position} duplicates another question`
          );
        }

        seenQuestions.add(
          questionKey
        );

        return {
          serial_number:
            position,
          source_type:
            sourceType,
          years_asked:
            yearsAsked,
          difficulty,
          subtopic,
          question,
          answer
        };
      }
    );

  const unrecoverablePyqs =
    parsed.unrecoverable_pyqs.map(
      (item, index) => {
        if (
          !item ||
          typeof item !== "object" ||
          Array.isArray(item)
        ) {
          throw new Error(
            `Unrecoverable entry ${index + 1} is invalid`
          );
        }

        return {
          year_asked:
            requireString(
              item.year_asked,
              `Unrecoverable entry ${index + 1} year_asked`,
              true
            ),
          reason:
            requireString(
              item.reason,
              `Unrecoverable entry ${index + 1} reason`
            )
        };
      }
    );

  return {
    output: {
      topic: expectedTopic,
      cards,
      unrecoverable_pyqs:
        unrecoverablePyqs
    },
    pyqCount,
    futureCount,
    unrecoverableCount:
      unrecoverablePyqs.length,
    totalCount:
      cards.length
  };
}

// ─────────────────────────────────────────────
// OPENAI GENERATION
// ─────────────────────────────────────────────

async function generateFlashcards(
  row
) {
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

          max_output_tokens:
            MAX_OUTPUT_TOKENS,

          text: {
            format: {
              type: "json_schema",
              name:
                "neet_mds_flashcards",
              strict: true,
              schema:
                FLASHCARD_SCHEMA
            }
          }
        });

      return validateAndNormalize(
        extractResponseText(
          response
        ),
        row.topic
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
        /invalid JSON|contains no cards|unrecoverable_pyqs|serial_number|source_type|historical year|years_asked|future cards|invalid difficulty|answer has|duplicates another question|appears after/i.test(
          getErrorText(error)
        );

      const shouldRetry =
        isRetryableError(error) ||
        validationError;

      if (
        attempt ===
          API_RETRIES ||
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
        `⚠️ Retry ${attempt + 1}/${API_RETRIES} after ${delay} ms: ${getErrorText(error)}`
      );

      await sleep(delay);
    }
  }

  throw lastError;
}

// ─────────────────────────────────────────────
// EXPIRED LOCK RECOVERY
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

  const { data, error } =
    await supabase
      .from(TABLE)
      .update({
        [LOCK_COL]: true,
        [LOCK_AT_COL]:
          lockedAt
      })
      .eq(
        "id",
        row.id
      )
      .eq(
        LOCK_COL,
        false
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
    .is(
      OUTPUT_COL,
      null
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
    .neq(
      INPUT_COL,
      JSON.stringify({
        pyqs: []
      })
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
      `Failed to find pending rows: ${error.message}`
    );
  }

  if (
    !availableRows?.length
  ) {
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
        row[
          LOCK_AT_COL
        ]
      )
      .is(
        OUTPUT_COL,
        null
      )
      .select("id");

  if (error) {
    throw new Error(
      `Failed to save flashcards: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the row lock changed or output already exists"
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
        row[
          LOCK_AT_COL
        ]
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
    `🦷 Generating flashcards | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateFlashcards(
        row
      );

    await saveSuccess(
      row,
      result.output
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | PYQ=${result.pyqCount} | FUTURE=${result.futureCount} | UNRECOVERABLE=${result.unrecoverableCount} | TOTAL=${result.totalCount}`
    );

    return {
      creditExhausted:
        false
    };
  } catch (error) {
    await releaseRowLock(
      row
    );

    if (
      isCreditExhaustionError(
        error
      )
    ) {
      console.error(
        "🛑 OpenAI credits exhausted. Worker will stop safely."
      );

      return {
        creditExhausted:
          true
      };
    }

    console.error(
      `❌ Failed | ${row.subject} | ${row.serial_number} | ${row.topic}: ${getErrorText(error)}`
    );

    return {
      creditExhausted:
        false
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
  let creditExhausted =
    false;

  async function runner() {
    while (
      nextIndex <
        rows.length &&
      !creditExhausted
    ) {
      const currentIndex =
        nextIndex;

      nextIndex += 1;

      const result =
        await processRow(
          rows[
            currentIndex
          ]
        );

      if (
        result.creditExhausted
      ) {
        creditExhausted =
          true;
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
        length:
          runnerCount
      },
      () => runner()
    )
  );

  if (creditExhausted) {
    await releaseClaimedRows(
      rows.slice(
        nextIndex
      )
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
    `🚀 NEET MDS FLASHCARD WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE} | Future maximum=${MAX_FUTURE_CARDS} | Max output=${MAX_OUTPUT_TOKENS}`
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
    "❌ Fatal NEET MDS flashcard worker error:",
    error
  );

  process.exit(1);
});
