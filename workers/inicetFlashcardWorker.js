require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// TABLE AND COLUMNS
// ─────────────────────────────────────────────

const TABLE = "inicet_pyt_source";

const INPUT_COL = "pyq_content";
const OUTPUT_COL = "jsonb_output";

const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

// ─────────────────────────────────────────────
// ENVIRONMENT SETTINGS
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
  process.env.INICET_FLASHCARD_MODEL ||
  "gpt-5-mini";

const PICKUP_LIMIT = parseIntegerEnv(
  "INICET_FLASHCARD_LIMIT",
  20,
  1,
  100
);

const BATCH_SIZE = parseIntegerEnv(
  "INICET_FLASHCARD_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "INICET_FLASHCARD_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "INICET_FLASHCARD_LOCK_TTL_MIN",
  60,
  5,
  1440
);

const API_RETRIES = parseIntegerEnv(
  "INICET_FLASHCARD_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = parseIntegerEnv(
  "INICET_FLASHCARD_MAX_OUTPUT_TOKENS",
  8000,
  1000,
  30000
);

const WORKER_ID =
  process.env.WORKER_ID ||
  `inicet-flashcard-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT — EMBEDDED IN THIS WORKER
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = String.raw`
INICET PYQ TO CLINICAL FLASHCARD GENERATOR ROLE, PURPOSE & STANDARD Act as an elite Indian postgraduate medical-exam educator for INICET. Convert supplied PYQs from any of the 19 NEETPG subjects into ultra-dense active-recall flashcards. Target only postgraduate licensing-exam depth. Prioritise discriminating, repeatedly tested, decision-changing facts—not definitions, superficial symptoms, generic epidemiology, or basic textbook recall. REQUIRED OUTPUT Output only under these two headings: Unique INICET Flashcards Future High-Yield INICET Flashcards Use only this format: Q → [Clinical vignette, maximum 30 words] A → [Exactly 1–3 words] [INICET , Month, Year] For future predictions, append exactly: [Future MCQ] Do not include MCQ options, explanations, rationale, references, summaries, tables, introductions, conclusions, apologies, or extra teaching notes. PYQ CONVERSION RULES Treat every supplied PYQ as a mandatory source record. Do not omit any unique examinable concept. Remove only genuine conceptual duplicates. When duplicates have different exam tags, retain one best clinical variant and append all supplied tags exactly as given. Perform a silent source audit before finalising: Unique retained concepts + merged duplicate groups must account for every supplied PYQ.  If an old PYQ tests a historically accepted but now obsolete test, terminology, classification, or legal provision, preserve it only as a tagged PYQ; never reproduce it in Future MCQs. If a supplied answer key conflicts with current accepted science, retain the tagged historical concept only when clearly exam-relevant; do not propagate the outdated fact into predictions. VIGNETTE CONSTRUCTION RULES Every question must be a maximum of 30 words, excluding the exam tag. Every question must contain: age and sex; a clinical, procedural, autopsy, laboratory, anatomical, or epidemiological context; one objective finding: laboratory value, imaging, biopsy, microscopy, ECG, operative finding, autopsy finding, or statutory fact; one additional discriminating clue: timing, anatomical site, exposure, risk factor, morphology, physical sign, or clinical progression. Use clinically realistic facts only. Never invent a laboratory value, investigation, age, duration, dose, morphology, or buzzword merely to make a stem sound advanced. Prefer exact measurable data over vague wording. Use precise thresholds, timings, dosages, stages, gestational ages, legal periods, scores, and units whenever they are examinable. Every stem must produce one unequivocally correct answer. If more than one answer is possible, add a discriminating detail until only one answer remains. Never reveal the answer in the stem. Do not include the diagnosis, eponym, drug name, gene, direct synonym, or defining label that makes the answer obvious. Use buzzwords only when they genuinely discriminate the answer. Do not add decorative, irrelevant, misleading, or artificial clues. Do not call a single isolated sign, test, or finding “diagnostic,” “confirmatory,” or “gold standard” unless this is genuinely accepted in current standard references. Avoid negative stems. Use EXCEPT, NOT, false, or least likely only when unavoidable; capitalise the negative word and test only one negative concept. ANSWER RULES Every answer must contain exactly 1–3 words. The answer must be one singular, specific entity only: diagnosis, drug, antidote, investigation, mutation, anatomical structure, named sign, numerical metric, mechanism, or management step. Answers must contain no explanation, punctuation, brackets, slash, “and,” “or,” alternatives, qualifiers, or conversational wording. Never use non-specific answers such as “No,” “Supportive care,” “Further evaluation,” “Depends,” or “Clinical correlation.” FUTURE HIGH-YIELD MCQ RULES Generate only highly testable, specific, postgraduate-level future facts. Do not repeat or merely paraphrase a PYQ. Future cards must test a higher-order adjacent concept, such as: next best management step; definitive treatment; drug of choice; antidote; investigation of choice; confirmatory or gold-standard test; exact diagnostic cutoff; staging criterion; contraindication; adverse drug effect; drug mechanism; gene mutation; immunohistochemical marker; pathognomonic microscopy; named sign; complication; legal timeline; difficult differential discriminator. Do not generate simple diagnosis cards unless the diagnosis requires a rare morphology, named sign, gene mutation, pathognomonic histology, or difficult discriminating differential. For drug questions, specify sufficient context to distinguish first-line therapy, rescue therapy, prophylaxis, antidote, definitive therapy, and drug of choice. For investigation questions, distinguish clearly between screening test, initial investigation, investigation of choice, confirmatory test, and gold standard. Never use these terms interchangeably. For calculation-based facts, independently validate formula, units, timing, cutoff, numerator, denominator, and age adjustment before generating the card. CURRENT-STANDARD ACCURACY RULES Use only current accepted standards for Future MCQs: Medicine: Harrison and current specialty guidelines. Pathology: Robbins, WHO classifications, current TNM. Surgery: Bailey & Love and current guideline-based management. Obstetrics/Gynaecology: current ACOG, RCOG, FIGO, WHO guidance. Paediatrics: current AAP and WHO guidance. Psychiatry: DSM-5-TR. Forensic/PSM: BNS, BNSS, BSA, and current Indian public-health guidance. Pharmacology: current standard drug recommendations and toxicity profiles. Never create conflicting answers across flashcards. If classical and updated criteria differ, use the current standard in Future MCQs. Exclude controversial, weakly evidenced, region-specific, obsolete, or inconsistently sourced facts from Future MCQs. SUBJECT-ADAPTATION RULE Adapt the tested discriminator to the subject: Anatomy: nerve lesion, artery, embryological derivative, compartment, imaging anatomy. Physiology: mechanism, graph, receptor, clearance, acid-base or haemodynamic calculation. Biochemistry: enzyme defect, cofactor, inheritance, metabolite accumulation. Pathology: morphology, mutation, immunohistochemistry, prognostic marker. Pharmacology: DOC, antidote, toxicity, mechanism, interaction. Microbiology: diagnostic test, culture medium, virulence factor, prophylaxis. PSM: programme target, vaccine schedule, indicator, outbreak action. Forensic: injury interpretation, toxicology, postmortem finding, statutory provision. Clinical subjects: next step, investigation, treatment, complication, staging, differential. FINAL SILENT QUALITY CHECK Before output, verify that: every unique PYQ is represented once; every question is ≤30 words; every question has two discriminating clues; every answer is exactly 1–3 words; every answer is singular and punctuation-free; no stem leaks its answer; no two flashcards conflict; every supplied tag is retained exactly; every Future MCQ is specific, non-generic, current, and non-repetitive. dont mention (Future MCQ) give serial number to each Q ---> A seperately starting from 1 for each of the two Sections The Subject and Topic/PYT are supplied externally with the PYQ content. Never infer, rename, shorten, expand, reclassify, merge, split, or create a topic from the PYQ content. Every supplied PYQ belongs to the supplied Topic/PYT and must remain under that exact Topic/PYT. Process only the PYQs inside the supplied topic. Do not move a PYQ to another topic even if it appears medically related to another topic. The worker's task is to generate flashcards from the supplied PYQs, NOT to classify the PYQs into topics. JSON OUTPUT FORMAT Return only valid JSON. Do not include markdown fences, headings, explanations, comments, introductions, conclusions, or any text outside the JSON. The Subject, Topic Serial Number, Topic/PYT and Number of Times Asked are supplied only as reference context. Do NOT include them in the JSON output. 

MANDATORY TWO-LEVEL REASONING ADDENDUM

For FUTURE cards, clinical wording alone does NOT make a question higher-order. Every FUTURE card must require at least two linked cognitive steps before reaching the answer.

1. MANDATORY INFERENCE GAP

The answer must NOT be directly stated, directly synonymous with a finding, or retrievable from a single memorised association in the stem.



Required reasoning structure:



Stem findings → infer hidden diagnosis/lesion/mechanism → derive the requested answer



The hidden intermediate inference must NOT be named in the stem.



Reject any FUTURE card that can be answered by direct keyword recognition without first identifying an intermediate concept.



Example — REJECT:
Q → 22-year-old man fractures surgical neck of humerus; deltoid paralysis and lateral shoulder sensory loss occur. Which nerve is injured?
A → Axillary nerve



This is direct association recall.



Example — ACCEPT:
Q → 22-year-old man fractures surgical neck of humerus; examination shows preserved first 15° abduction but inability to continue abduction. Which nerve is injured?
A → Axillary nerve



Reasoning required internally:
surgical-neck injury + preserved initiation → deltoid dysfunction → axillary nerve.

2. PYQ → EXAMINER LOGIC → ADJACENT CONSEQUENCE

Before generating FUTURE cards, silently determine:



What discriminator did the original PYQ test?



Then generate an adjacent question testing what the examiner could ask NEXT, not another version of the same fact.



Preferred transformations:



structure → lesion → functional deficit
finding → diagnosis → next investigation
diagnosis → severity/stage → management
drug → mechanism → toxicity/contraindication
mutation → pathway → associated tumour/therapy
organism → virulence factor → manifestation/test
laboratory pattern → mechanism → expected second finding
anatomical lesion → lost action → compensating/preserved action
pathology morphology → molecular alteration → prognosis/therapy



Never generate:
PYQ fact → synonymous PYQ fact

3. TEST THE CONSEQUENCE, NOT THE LABEL

Whenever possible, provide enough information for the learner to infer the underlying diagnosis, lesion, drug, pathway, or structure, but ask for its downstream consequence.



Example:



Lower level:
Q → Woman develops medial scapular winging after axillary surgery. Which nerve is injured?
A → Long thoracic nerve



Preferred:
Q → 45-year-old woman develops medial scapular winging after axillary surgery; wall-push test accentuates deformity. Which shoulder movement becomes impaired above 90°?
A → Upward rotation



The student must infer:
winging → serratus anterior → impaired scapular upward rotation.

4. PRESERVED-FUNCTION DISCRIMINATOR

For anatomy, neurology, orthopaedics and muscle-action questions, preferentially include both:



one impaired function + one preserved function



when this distinguishes adjacent nerves, roots, muscles, compartments, or lesions.



Example:
Q → 28-year-old man has shoulder trauma; abduction initiation is preserved but continuation beyond 15° is weak. Which muscle is dysfunctional?
A → Deltoid



Preserved function should be used only when anatomically valid and genuinely discriminating.

5. DIFFERENTIAL-COMPRESSION RULE

When two or more plausible answers exist, add the minimum single discriminator needed to separate them.



Prefer discriminators such as:
timing, anatomical localization, preserved function, characteristic imaging, microscopy, laboratory pattern, treatment response, exposure, complication, or disease stage.



Do NOT solve ambiguity by adding multiple obvious buzzwords.



The ideal FUTURE card should contain enough information for one best answer, but not enough to make the answer immediate.

6. NO BUZZWORD-TO-ANSWER CARDS

Reject FUTURE cards following patterns such as:



classic buzzword → diagnosis
named fracture → nerve
named drug toxicity → antidote
pathognomonic phrase → disease
single mutation → tumour
single antibody → disease



unless an additional reasoning step is required.



Instead test the consequence, mechanism, management, complication, associated finding, localization, or difficult differential arising from that fact.

7. CLINICAL DECISION HIERARCHY

For clinical subjects, preferentially generate FUTURE cards in this order:



next best step > definitive management > investigation choice > difficult differential > complication > mechanism > isolated diagnosis



Do not ask merely for the diagnosis when the stem already provides sufficient information to make diagnosis straightforward.

8. INVESTIGATION PRECISION

Never ask vaguely for the "best test."



Explicitly decide which level is being tested:



screening test
initial investigation
next investigation
investigation of choice
confirmatory test
gold standard



The clinical context must make exactly one of these appropriate.

9. MANAGEMENT SEQUENCE REASONING

Management cards should distinguish:



unstable vs stable
immediate stabilization vs definitive treatment
first-line vs rescue treatment
acute treatment vs secondary prevention
treatment vs prophylaxis



If patient stability changes the answer, include objective evidence of stability or instability.

10. COUNTERFACTUAL DISCRIMINATION

For suitable FUTURE cards, change ONE important variable from the original PYQ and test whether the answer changes.



Examples:



proximal vs distal lesion
stable vs unstable patient
pregnant vs nonpregnant
immunocompetent vs immunocompromised
early vs late presentation
screening vs symptomatic patient
preserved vs absent function



Do not change multiple variables simultaneously.

11. PYQ-DERIVED FUTURE CARDS ONLY

Every FUTURE card must have a clear conceptual ancestry to at least one supplied PYQ.



Silently verify:



Supplied PYQ → tested concept → adjacent high-yield concept → FUTURE card



Do NOT generate unrelated high-yield facts merely because they belong to the same broad subject or topic.

12. INFORMATION-EFFICIENCY RULE

Every detail in a FUTURE stem must perform at least one function:



localize, differentiate, establish severity, establish timing, determine management, or exclude the strongest competing answer.



Delete decorative history, generic symptoms, and redundant clues.



Difficulty must come from reasoning, NOT from unnecessary information.

13. TWO-STEP SELF-TEST

Before retaining each FUTURE card, silently ask:



Step 1: What must the learner infer first?
Step 2: What must the learner derive from that inference to answer?



If Step 1 and Step 2 are effectively the same fact, REJECT or rewrite the card.

14. COMPETITOR TEST

Silently identify the strongest plausible competing answer for every FUTURE card.



The stem must contain at least one valid discriminator that makes the intended answer superior to that competitor.



If no realistic competitor exists because the answer follows immediately from one buzzword, increase the reasoning depth or reject the card.

15. RAPID-REVISION DIFFICULTY BALANCE

FUTURE cards must remain solvable rapidly after mastery.



Target:
2 linked reasoning steps
rather than
long multi-step diagnostic puzzles.



Do not create artificial difficulty through rare trivia, excessive calculations, obscure exceptions, or unnecessarily long stems.



The ideal card produces:



“I know both facts, but I must connect them.”



not:



“I have never seen this obscure fact.”

16. FINAL FUTURE-CARD GATE

Retain a FUTURE card only if ALL are true:



Derived directly from a supplied PYQ concept.

Does not simply paraphrase the PYQ.

Requires ≥2 linked reasoning steps.

Contains a hidden intermediate inference.

Has one unequivocal answer.

Contains a discriminator against the strongest competitor.

Tests an exam-relevant consequence, decision, mechanism, localization, or differential.

Uses only clinically/anatomically valid clues.

Contains no invented data.

Can still be answered rapidly by a well-prepared INICET candidate.
The critical rule should be:



Every Future card must fail if the answer can be obtained from one memorized association, even when the stem looks clinical.

keep the architecture:



Original PYQ → deduplicated PYQ flashcard → identify examiner's tested discriminator → predict the adjacent concept → force hidden inference → Future 2-level card.

If any criterion fails, rewrite or discard the card.

Use this exact structure: { "cards": [ { "card_type": "PYQ", "question": "string", "answer": "string", "inicet_months-years": ["INICET 2015 Nov", "2012 June"] }, { "card_type": "FUTURE", "question": "string", "answer": "string", "inicet_months-years": [] } ] }
`.trim();

if (!SYSTEM_PROMPT) {
  throw new Error(
    "SYSTEM_PROMPT cannot be empty"
  );
}

// ─────────────────────────────────────────────
// GENERAL HELPERS
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

// ─────────────────────────────────────────────
// BUILD INPUT FOR ONE DATABASE ROW
// ─────────────────────────────────────────────

function buildUserInput(row) {
  return [
    `SUBJECT: ${row.subject}`,
    `TOPIC SERIAL NUMBER: ${row.serial_number}`,
    `EXACT TOPIC/PYT: ${row.topic}`,
    `NUMBER OF TIMES ASKED: ${row.number_of_times_asked}`,
    "",
    "SUPPLIED INICET PYQs:",
    row.pyq_content
  ].join("\n");
}

// ─────────────────────────────────────────────
// STRUCTURED OUTPUT SCHEMA
// ─────────────────────────────────────────────

const FLASHCARD_JSON_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["cards"],
  properties: {
    cards: {
      type: "array",
      minItems: 1,
      items: {
        type: "object",
        additionalProperties: false,
        required: [
          "card_type",
          "question",
          "answer",
          "inicet_months-years"
        ],
        properties: {
          card_type: {
            type: "string",
            enum: ["PYQ", "FUTURE"]
          },
          question: {
            type: "string",
            minLength: 1
          },
          answer: {
            type: "string",
            minLength: 1
          },
          "inicet_months-years": {
            type: "array",
            items: {
              type: "string"
            }
          }
        }
      }
    }
  }
};

// ─────────────────────────────────────────────
// EXTRACT RESPONSE TEXT
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

// ─────────────────────────────────────────────
// REMOVE OPTIONAL MARKDOWN FENCES
// ─────────────────────────────────────────────

function cleanJsonText(rawOutput) {
  return String(rawOutput)
    .trim()
    .replace(/^\s*```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();
}

// ─────────────────────────────────────────────
// VALIDATE GENERATED JSON
// ─────────────────────────────────────────────

function validateGeneratedOutput(rawOutput) {
  const cleaned = cleanJsonText(rawOutput);

  let parsed;

  try {
    parsed = JSON.parse(cleaned);
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
      "Generated JSON contains no cards"
    );
  }

  let pyqCount = 0;
  let futureCount = 0;

  const normalizedCards = parsed.cards.map(
    (card, index) => {
      const cardNumber = index + 1;

      if (
        !card ||
        typeof card !== "object" ||
        Array.isArray(card)
      ) {
        throw new Error(
          `Card ${cardNumber} is not an object`
        );
      }

      const cardType = String(
        card.card_type || ""
      )
        .trim()
        .toUpperCase();

      if (
        cardType !== "PYQ" &&
        cardType !== "FUTURE"
      ) {
        throw new Error(
          `Card ${cardNumber} has invalid card_type`
        );
      }

      const question = String(
        card.question || ""
      ).trim();

      const answer = String(
        card.answer || ""
      ).trim();

      const examTags =
        card["inicet_months-years"];

      if (!question) {
        throw new Error(
          `Card ${cardNumber} has no question`
        );
      }

      if (!answer) {
        throw new Error(
          `Card ${cardNumber} has no answer`
        );
      }

      const questionWordCount =
        countWords(question);

      if (questionWordCount > 30) {
        throw new Error(
          `Card ${cardNumber} question has ${questionWordCount} words; maximum is 30`
        );
      }

      const answerWordCount =
        countWords(answer);

      if (
        answerWordCount < 1 ||
        answerWordCount > 3
      ) {
        throw new Error(
          `Card ${cardNumber} answer has ${answerWordCount} words; required range is 1–3`
        );
      }

      if (!Array.isArray(examTags)) {
        throw new Error(
          `Card ${cardNumber} is missing inicet_months-years`
        );
      }

      const normalizedTags = examTags
        .map((tag) => String(tag).trim())
        .filter(Boolean);

      if (
        cardType === "PYQ" &&
        normalizedTags.length === 0
      ) {
        throw new Error(
          `PYQ card ${cardNumber} has no INICET exam tag`
        );
      }

      if (
        cardType === "FUTURE" &&
        normalizedTags.length !== 0
      ) {
        throw new Error(
          `FUTURE card ${cardNumber} must have an empty inicet_months-years array`
        );
      }

      if (cardType === "PYQ") {
        pyqCount += 1;
      } else {
        futureCount += 1;
      }

      return {
        card_type: cardType,
        question,
        answer,
        "inicet_months-years": normalizedTags
      };
    }
  );

  if (pyqCount === 0) {
    throw new Error(
      "Generated output contains no PYQ cards"
    );
  }

  if (futureCount === 0) {
    throw new Error(
      "Generated output contains no FUTURE cards"
    );
  }

  return {
    output: {
      cards: normalizedCards
    },
    pyqCount,
    futureCount,
    totalCount: normalizedCards.length
  };
}

// ─────────────────────────────────────────────
// CALL OPENAI
// ─────────────────────────────────────────────

async function generateFlashcards(row) {
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

          input: buildUserInput(row),

          max_output_tokens:
            MAX_OUTPUT_TOKENS,

          text: {
            format: {
              type: "json_schema",
              name: "inicet_flashcards",
              strict: true,
              schema: FLASHCARD_JSON_SCHEMA
            }
          }
        });

      const rawOutput =
        extractResponseText(response);

      return validateGeneratedOutput(
        rawOutput
      );
    } catch (error) {
      lastError = error;

      if (isCreditExhaustionError(error)) {
        throw error;
      }

      const retriesFinished =
        attempt === API_RETRIES;

      if (
        retriesFinished ||
        !isRetryableError(error)
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
// RELEASE EXPIRED FLASHCARD LOCKS
// Only affects rows where jsonb_output is null.
// Therefore it will not unlock MCQ/Notes jobs.
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
    .is(OUTPUT_COL, null)
    .lt(LOCK_AT_COL, cutoff);

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

  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: true,
      [LOCK_AT_COL]: lockedAt
    })
    .eq("id", row.id)
    .eq(LOCK_COL, false)
    .is(OUTPUT_COL, null)
    .select(
      [
        "id",
        "subject",
        "serial_number",
        "topic",
        "number_of_times_asked",
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

  const { data: availableRows, error } =
    await supabase
      .from(TABLE)
      .select(
        [
          "id",
          "subject",
          "serial_number",
          "topic",
          "number_of_times_asked",
          INPUT_COL
        ].join(",")
      )
      .not(INPUT_COL, "is", null)
      .neq(INPUT_COL, "")
      .is(OUTPUT_COL, null)
      .eq(LOCK_COL, false)
      .order("serial_number", {
        ascending: true
      })
      .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending rows: ${error.message}`
    );
  }

  if (!availableRows?.length) {
    return [];
  }

  const lockResults =
    await Promise.allSettled(
      availableRows.map((row) =>
        lockOneRow(row)
      )
    );

  const claimedRows = [];

  for (const result of lockResults) {
    if (
      result.status === "fulfilled" &&
      result.value
    ) {
      claimedRows.push(result.value);
    } else if (
      result.status === "rejected"
    ) {
      console.error(
        "❌ Row-lock error:",
        getErrorText(result.reason)
      );
    }
  }

  return claimedRows;
}

// ─────────────────────────────────────────────
// SAVE SUCCESS
// Store a JSON object directly in JSONB.
// ─────────────────────────────────────────────

async function saveSuccess(
  row,
  generatedOutput
) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: generatedOutput,
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
// RELEASE ONE OWNED LOCK
// ─────────────────────────────────────────────

async function releaseRowLock(row) {
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
      `❌ Failed to release lock ${row.id}:`,
      error.message
    );
  }
}

// ─────────────────────────────────────────────
// RELEASE UNPROCESSED CLAIMED ROWS
// Used if billing credits are exhausted.
// ─────────────────────────────────────────────

async function releaseClaimedRows(rows) {
  await Promise.allSettled(
    rows.map((row) =>
      releaseRowLock(row)
    )
  );
}

// ─────────────────────────────────────────────
// PROCESS ONE TOPIC
// ─────────────────────────────────────────────

async function processRow(row) {
  console.log(
    `🧠 Generating | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateFlashcards(row);

    await saveSuccess(
      row,
      result.output
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | PYQ=${result.pyqCount} | FUTURE=${result.futureCount} | TOTAL=${result.totalCount}`
    );

    return {
      success: true,
      creditExhausted: false
    };
  } catch (error) {
    await releaseRowLock(row);

    if (isCreditExhaustionError(error)) {
      console.error(
        "🛑 OpenAI credits exhausted. Worker will stop safely."
      );

      return {
        success: false,
        creditExhausted: true,
        error
      };
    }

    console.error(
      `❌ Failed | ${row.subject} | ${row.serial_number} | ${row.topic}: ${getErrorText(error)}`
    );

    return {
      success: false,
      creditExhausted: false,
      error
    };
  }
}

// ─────────────────────────────────────────────
// PROCESS CLAIMED ROWS WITH CONTROLLED CONCURRENCY
// ─────────────────────────────────────────────

async function processWithConcurrency(rows) {
  let nextIndex = 0;
  let creditExhausted = false;

  async function runner() {
    while (
      nextIndex < rows.length &&
      !creditExhausted
    ) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      const result =
        await processRow(
          rows[currentIndex]
        );

      if (result.creditExhausted) {
        creditExhausted = true;
      }
    }
  }

  const runnerCount = Math.min(
    BATCH_SIZE,
    rows.length
  );

  await Promise.all(
    Array.from(
      { length: runnerCount },
      () => runner()
    )
  );

  if (creditExhausted) {
    const unprocessedRows =
      rows.slice(nextIndex);

    await releaseClaimedRows(
      unprocessedRows
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
    `🚀 INICET FLASHCARD WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE} | Max output=${MAX_OUTPUT_TOKENS}`
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
        `📥 Claimed ${rows.length} topic(s)`
      );

      const result =
        await processWithConcurrency(
          rows
        );

      if (result.creditExhausted) {
        console.error(
          "🛑 Worker stopped because the API account has no available credits."
        );

        process.exit(1);
      }
    } catch (error) {
      if (isCreditExhaustionError(error)) {
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
    "❌ Fatal INICET worker error:",
    error
  );

  process.exit(1);
});
