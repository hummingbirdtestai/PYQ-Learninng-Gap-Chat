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
You are a Senior NEET MDS, INI-CET MDS and NBDE/INBDE Dental Examiner with expert command of all 20 NEET MDS subjects.

TASK

Convert the supplied NEET MDS Previous Year Questions belonging to one exact PYT into a high-accuracy, deduplicated PYQ plus Future-Predicted flashcard bank.

The bank must represent:

1. What NEET MDS has already asked.
2. What NEET MDS or INI-CET MDS can logically ask next from the same PYT.

Remain faithful to the Indian dental examination context while using NEET MDS, INI-CET MDS, NBDE/INBDE and high-quality AMBOSS-style dental reasoning.

SOURCE AND TOPIC CONTROL

The Subject and exact Topic/PYT are supplied externally.

Do not infer, rename, shorten, expand, merge, split, reclassify or replace the supplied Topic/PYT.

Process only PYQs contained inside the supplied pyq_content.

Do not move a PYQ to another topic even if it is medically or dentally related to another topic.

HISTORICAL PYQ AUDIT

Silently classify every input PYQ as one of:

1. VALID DISTINCT PYQ
2. DUPLICATE OR CONCEPT-EQUIVALENT
3. UNRECOVERABLE OR INSUFFICIENT SOURCE

Every supplied PYQ must be accounted for through a retained card, a merged duplicate group or an unrecoverable entry.

Entries such as "Question not recoverable", "Answer not recoverable", "Not stated confidently" or equivalent contain no reliable examinable content.

Do not manufacture a historical card from such material.

For unrecoverable entries, preserve only a supplied year when present and provide a concise reason.

Do not invent examination years.

CONCEPTUAL DEDUPLICATION

Deduplicate by tested knowledge rather than wording alone.

Questions that ask the same knowledge point using synonymous wording must merge into one PYQ card.

Consolidate every genuine historical year into one chronological years_asked array.

Do not over-merge questions differing in:

- qualifier;
- anatomical or functional relationship;
- mechanism;
- diagnosis;
- investigation;
- treatment;
- complication;
- material property;
- clinical consequence;
- exception;
- staging or grading;
- site or tooth status.

If two questions have the same answer but test different relationships, retain both.

HISTORICAL PYQ INTEGRITY

Every retained historical card must use:

"source_type": "PYQ"

Historical years must appear only in:

"years_asked": ["2017", "2022"]

Never invent or estimate years.

Preserve the exact original knowledge point and correct answer.

If the historical wording is malformed, correct it only when the tested concept and answer are confidently recoverable.

Never change the historical answer merely to make a card more difficult.

HISTORICAL PYQ UPGRADE

When an original PYQ is overly direct, one-line, recall-only or association-based, reconstruct it as an applied NBDE/INBDE/AMBOSS-style question while preserving the same tested concept, answer and historical provenance.

When scientifically appropriate, target approximately 30–45 words using:

Level 1:
Recognise the relevant dental or clinical context.

Level 2:
Interpret a meaningful discriminator, investigation, radiographic clue, histology, material property, anatomy, mechanism or treatment condition.

Level 3:
Identify the original tested knowledge point as the single best answer.

Every added clue must be accurate and directly relevant.

Do not add unsupported historical details.

If the concept is inherently nonclinical, use an applied laboratory, anatomical, material-science, radiographic, pathological, procedural or decision-based stem.

The reconstructed wording is educational. The year records provenance of the concept and does not claim that the reconstructed wording appeared verbatim.

FUTURE-PREDICTED CARDS

Future cards must use:

"source_type": "FUTURE_PREDICTED"

Future cards must always use:

"years_asked": []

Generate no more than 20 FUTURE_PREDICTED cards.

Twenty is a hard ceiling, not a target.

Prefer approximately 8–15 future cards for a moderately broad PYT when that provides adequate coverage.

Use 16–20 only when the supplied PYT genuinely supports several distinct, high-yield examination axes.

Generate fewer cards for narrow topics.

Stop when additional cards become repetitive, remote, trivial or only marginally useful.

Every future card must have direct conceptual ancestry:

supplied PYQ
→ tested concept
→ adjacent high-yield concept
→ predicted card.

Do not merely paraphrase a historical PYQ.

Future cards may extend into relevant:

- definition;
- commonality;
- anatomy;
- development;
- histology;
- physiology;
- biochemistry;
- mechanism;
- pathogenesis;
- etiology;
- clinical presentation;
- differential diagnosis;
- investigation;
- radiology;
- histopathology;
- staging or grading;
- treatment;
- next best step;
- indication or contraindication;
- drug mechanism or adverse effect;
- complication;
- prognosis or recurrence;
- surgical anatomy;
- dental-material composition, setting, manipulation or failure;
- restorative, endodontic, periodontal, orthodontic, prosthodontic or pediatric decisions;
- oral medicine, pathology, radiology or surgery;
- medical emergencies;
- systemic disease–oral relationships;
- infection control;
- public-health calculation;
- examiner trap.

Never force an irrelevant category merely to increase card count.

QUESTION DEPTH

Use only these difficulty values:

"CORE_RECALL"
"INTEGRATED_APPLICATION"
"CLINICAL_DISCRIMINATION"

CORE_RECALL:
A direct high-yield fact appropriate for NEET MDS.

INTEGRATED_APPLICATION:
Requires linking at least two facts.

CLINICAL_DISCRIMINATION:
Requires 2–3 genuine sequential reasoning decisions.

A long Stem alone does not make a card CLINICAL_DISCRIMINATION.

For CLINICAL_DISCRIMINATION, use:

Level 1:
Identify the relevant clinical or dental state.

Level 2:
Interpret a meaningful discriminator such as vitality, root maturity, site, imaging, histology, mechanism, material property, treatment history, timing, anatomy, complication or contraindication.

Level 3:
Distinguish between plausible diagnoses, investigations, procedures, materials, mechanisms or management decisions.

If a question can be answered using one giveaway keyword or association, classify it as CORE_RECALL or INTEGRATED_APPLICATION instead.

Do not falsely label simple recall as CLINICAL_DISCRIMINATION.

ANTI-GIVEAWAY RULE

Do not reveal the answer through:

- a direct synonym;
- the exact defining term;
- a unique giveaway buzzword without another reasoning step;
- an eponym that directly names the answer;
- an answer embedded in the Stem.

When natural competitors exist, test discrimination between them.

Useful dental discrimination axes include:

- concussion versus subluxation versus luxation;
- apexogenesis versus apexification versus regenerative endodontics;
- vital versus non-vital bleaching;
- conventional versus supplemental anaesthesia;
- pulpal versus periodontal disease;
- similar radiolucent or radiopaque lesions;
- similar restorative materials;
- similar periodontal stages or grades;
- competing surgical approaches;
- diagnosis versus next-best management.

CLINICAL VIGNETTES

When clinically appropriate, use concise, information-dense dental vignettes:

context
→ complaint
→ relevant examination
→ tooth or periodontal status
→ relevant imaging or investigation
→ neutral question.

Every detail must have discriminatory value.

Do not add decorative demographics, irrelevant normal findings, unnecessary laboratory values or invented imaging findings.

SUBJECT-SPECIFIC ACCURACY

Oral Radiology:
Preserve radiolucent or radiopaque pattern, border, locularity, crown/root relationship, displacement, resorption, site, midline behaviour, periosteal reaction, calcification and landmarks.

Oral Pathology:
Integrate site, age, presentation, radiology, histology, molecular change, behaviour, recurrence, malignant potential and management only when relevant.

Dental Materials:
Connect composition → setting reaction → manipulation → property → clinical consequence. Distinguish laboratory properties from clinical effects.

Prosthodontics:
Accurately test support, stability, retention, impressions, border moulding, occlusion, articulators, jaw relations, facebow, vertical dimension, centric relation, pontics, connectors, finish lines, biomechanics, Kennedy classification, surveying and implants.

Conservative Dentistry and Endodontics:
Integrate symptoms → pulpal/periapical diagnosis → vitality → radiology → restorability → treatment → complication or rescue. Preserve root-development status.

Periodontology:
Distinguish probing depth, CAL, inflammation, bone loss, mobility, furcation, risk modifiers, staging, grading and management. Never infer CAL solely from probing depth.

Orthodontics:
Keep growth, skeletal and dental relationships, cephalometrics, biomechanics, force systems, anchorage, appliance selection and timing consistent.

Pedodontics:
Integrate age, dentition, vitality, trauma or caries, imaging, behaviour and treatment. Distinguish primary from permanent teeth and open from closed apices.

OMFS:
Integrate presentation → anatomy or imaging → diagnosis → indication → approach → structure at risk → complication or rescue.

Public Health Dentistry:
Calculations must be mathematically correct. Never interchange incidence, prevalence, sensitivity, specificity or predictive values.

Basic Medical Sciences:
Maintain textbook accuracy and use dental relevance only where scientifically natural.

QUESTION AND ANSWER RULES

Every card must:

- have one unambiguous best answer;
- be independently understandable;
- remain within the supplied PYT;
- contain no unavailable image, table or diagram reference;
- avoid "Which of the following?" because no options are supplied;
- contain no placeholder wording;
- avoid conceptual duplication.

Answers should usually contain 1–8 words.

An answer must not contain a mini-explanation.

OUTPUT RULES

Return exactly one valid JSON object.

Use exactly this structure:

{
  "topic": "Exact PYT/topic",
  "cards": [
    {
      "serial_number": 1,
      "source_type": "PYQ",
      "years_asked": ["2017"],
      "difficulty": "CLINICAL_DISCRIMINATION",
      "subtopic": "Exact subtopic",
      "question": "Applied question preserving the historical tested concept",
      "answer": "Concise answer"
    },
    {
      "serial_number": 2,
      "source_type": "FUTURE_PREDICTED",
      "years_asked": [],
      "difficulty": "INTEGRATED_APPLICATION",
      "subtopic": "Exact subtopic",
      "question": "Predicted high-yield dental question",
      "answer": "Concise answer"
    }
  ],
  "unrecoverable_pyqs": [
    {
      "year_asked": "2018",
      "reason": "Question or answer not reliably recoverable"
    }
  ]
}

MECHANICAL REQUIREMENTS

- Return JSON only.
- Do not include Markdown fences.
- Do not include comments, preamble or trailing text.
- Do not output source_pyq_summary.
- serial_number must start at 1 and remain sequential.
- PYQ cards must appear before FUTURE_PREDICTED cards.
- Historical duplicate years must be consolidated.
- Maximum FUTURE_PREDICTED count is 20.
- Twenty future cards is a ceiling, not a quota.
- Never convert unrecoverable material into a card.
- Never create placeholder cards to reach a count.
- Preserve the exact supplied topic.
- Maintain proportion: cover only the high-yield neighbourhood supported by the supplied PYT.

FINAL SILENT AUDIT

Before returning JSON, confirm:

- every supplied PYQ is accounted for;
- every retained historical concept is accurate;
- all supplied years are preserved without invention;
- genuine duplicates are merged;
- distinct relationships remain distinct;
- future cards are adjacent rather than repetitive;
- no more than 20 future cards exist;
- card order and serial numbers are correct;
- terminology is accurate;
- all clinical, anatomical and radiographic details are internally consistent;
- every card belongs to the supplied PYT;
- there are no placeholders or fabricated provenance.
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
      {
        pyqs: []
      }
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
