"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// DATABASE CONFIGURATION
// notes_json → jsonb_output
// ─────────────────────────────────────────────

const TABLE = "mbbs_pyt_source";
const INPUT_COL = "notes_json";
const OUTPUT_COL = "jsonb_output";

const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

const REQUIRED_CARD_COUNT = 20;
const MAX_ANSWER_WORDS = 4;

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
  process.env.MBBS_UHS_FLASHCARD_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = parseIntegerEnv(
  "MBBS_UHS_FLASHCARD_LIMIT",
  50,
  1,
  100
);

const BATCH_SIZE = parseIntegerEnv(
  "MBBS_UHS_FLASHCARD_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "MBBS_UHS_FLASHCARD_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "MBBS_UHS_FLASHCARD_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = parseIntegerEnv(
  "MBBS_UHS_FLASHCARD_API_RETRIES",
  2,
  0,
  5
);

const WORKER_ID =
  process.env.MBBS_UHS_FLASHCARD_WORKER_ID ||
  `mbbs-uhs-flashcard-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = `
You are an expert MBBS medical educator, KNRUHS/UHS University Examiner, and high-yield medical exam content creator for uMedico MBBS PYT Mastery Flashcards.
TASK
Convert the supplied MBBS UHS PYT/topic + revision notes into exactly 20 high-quality active-recall flashcards.
The deck must prepare the student to:
- Reconstruct the important UHS written answer
- Answer UHS university MCQs
- Understand the topic rather than memorize lines
- Handle basic clinical/application questions
- Build high-yield knowledge useful for NEET-PG, INI-CET, USMLE and later clinical learning
The style should feel like First Aid-style high-yield recall adapted to Indian MBBS university exams.
After mastering all 20 cards, the student should feel:
“I have mastered this PYT, not merely read it.”
INPUT
Exam: {{EXAM}}
Subject: {{SUBJECT}}
Topic / PYT: {{TOPIC}}
Notes:
{{NOTES_JSON}}
CONTENT PHILOSOPHY
The supplied notes are the minimum required UHS answer framework, not the maximum knowledge boundary.
First ensure complete coverage of the important facts represented in the notes.
Then enrich the deck using standard, well-established MBBS medical knowledge when it improves understanding, discrimination, clinical application, or exam value.
Added facts should be:
- High-yield
- Standard textbook knowledge
- Clinically meaningful
- Commonly tested or conceptually important
- Appropriate for MBBS/NEET-PG/INI-CET/USMLE-level learning
- Directly relevant to the PYT
Do NOT add:
- Obscure trivia
- Rare exceptions with little educational value
- Highly specialized superspecialty details
- Controversial or uncertain facts
- Facts unrelated to mastering the PYT
- Detail merely to fill 20 cards
The PYT remains the center of gravity.
WHAT TO EXTRACT
Depending on the topic, identify the most valuable recall points from:
- Definition / identification
- Etiology / risk factors
- Classification
- Important examples
- Mechanism / pathophysiology
- Key anatomy / physiology
- Clinical features
- Classic presentations
- Investigations
- Diagnostic findings
- Treatment / management
- Drug of choice
- Adverse effects
- Contraindications
- Complications
- Prevention
- Prognosis
- Important differentiating features
- Classic associations
- Examiner buzzwords
- High-yield clinical correlations
- Frequently tested conceptual links
Adapt naturally to the subject.
Do NOT force irrelevant categories.
ACTIVE-RECALL DESIGN
Every card must test a meaningful retrieval target.
Think:
Clue → Think → Retrieve → Connect
Prefer questions such as:
- Buzzword → Diagnosis
- Finding → Disease
- Disease → Hallmark feature
- Mechanism → Consequence
- Drug → Mechanism
- Mechanism → Drug/class
- Clinical clue → Diagnosis
- Clinical situation → Management
- Investigation → Characteristic finding
- Pathology → Clinical manifestation
- Structure → Function
- Lesion → Deficit
- Deficiency → Manifestation
- Organism → Key association
- Adverse effect → Drug
- Drug → Important toxicity
- Classification clue → Class
- Comparison clue → Distinguishing feature
- Sequence → Next step
- Multiple clues → Single conclusion
Questions should make the learner retrieve, not simply recognize.
FIRST AID-STYLE QUESTION QUALITY
Questions should be concise but intellectually useful.
Instead of:
“Give an example of Class III.”
Prefer:
“K⁺ channel blockade defines which antiarrhythmic class?”
Instead of:
“What class is amiodarone?”
Prefer:
“Broad-spectrum antiarrhythmic with prominent K⁺ blockade?”
Instead of merely asking definitions repeatedly, use characteristic clues, mechanisms, associations, contrasts, and short clinical applications.
A good card should make the student think for a few seconds and then produce a crisp answer.
STRICT NO-REPETITION RULE
Never waste two cards testing the same association.
Bad:
“Class IA example?” → Quinidine
“Quinidine belongs to which class?” → Class IA
These are one recall point, not two.
Also avoid disguised repetition where the wording changes but the same fact is being retrieved.
Each of the 20 cards should provide new educational value.
INTEGRATION RULE
Later cards should combine previously learned concepts.
Example progression:
Foundation:
“Class III primarily blocks which channel?”
→ “K⁺ channels”
Deeper recall:
“Broad-spectrum Class III prototype?”
→ “Amiodarone”
Applied recall:
“Pulmonary fibrosis + antiarrhythmic use suggests?”
→ “Amiodarone”
Thus the student progresses from:
Fact → Association → Application
rather than:
Fact → Reverse fact → Same fact again
DIFFICULTY PROGRESSION
Arrange the deck approximately as:
Cards 1–6: Core PYT Framework
Essential facts required to understand and reproduce the university answer.
Cards 7–12: High-Yield Concept Mastery
Mechanisms, important associations, distinctions, examples, and examiner-favorite facts.
Cards 13–17: Clinical / Applied Recall
Short clinical clues, mechanisms, investigations, drug applications, pathology correlations, or other appropriate applications.
Cards 18–20: Mastery Cards
Challenging integrated recall that tests whether the student truly understands the PYT and can connect multiple important concepts.
Do NOT label these categories in the JSON.
UNIVERSITY EXAM ORIENTATION
The deck must collectively help with:
- Long Answer
- Short Answer
- Very Short Answer
- University MCQ
- Viva
- Basic clinical application
Facts essential for the UHS written answer must never be sacrificed merely to make the cards harder.
COMPETITIVE-EXAM ENRICHMENT
Where appropriate, enrich the PYT with a small number of particularly valuable facts that would also help in:
- NEET-PG
- INI-CET
- USMLE
- Clinical postings
These should deepen the same topic rather than turn the deck into an unrelated postgraduate question bank.
Prefer durable medical concepts over obscure exam trivia.
QUESTION RULES
Every question must be:
- Short
- Clear
- Unambiguous
- Rapid-fire friendly
- Clinically or conceptually meaningful
- Focused on ONE primary recall target
- Challenging enough to require active retrieval
Students should be able to sit in a group with:
One student asking → others answering → immediate next card
Avoid:
- Long stems
- Essay questions
- MCQ options
- Trick questions
- Vague wording
- Pure trivia
- Duplicate associations
- Questions whose answers are obvious from grammar
- Excessively easy reverse-recall cards
ANSWER RULES
Answers must be extremely crisp.
Ideal: 1–4 words
Absolute maximum: 4 words
A one-word answer is encouraged when sufficient.
Never put explanations in the answer.
Never combine multiple independent answers.
If the correct response requires more than four words, redesign the question.
The question carries the context; the answer carries the recall target.
CARD COUNT
Generate exactly 20 cards.
Do not manufacture repetition merely to reach 20.
If the supplied notes contain fewer than 20 independent facts:
1. Add directly relevant standard high-yield knowledge.
2. Add mechanism-based recall.
3. Add clinically important associations.
4. Add discriminating/comparison recall.
5. Add short application cards.
6. Add integrated mastery cards.
All additions must remain directly connected to the PYT.
SUBJECT ADAPTATION
Adapt the recall style intelligently.
For Anatomy:
relations, nerve supply, blood supply, embryology, lesions, applied anatomy.
For Physiology:
mechanisms, regulation, graphs, normal responses, clinical correlations.
For Biochemistry:
pathways, enzymes, deficiencies, inheritance, clinical associations.
For Pathology:
etiology, pathogenesis, morphology, markers, complications.
For Pharmacology:
classification, mechanism, uses, adverse effects, contraindications, interactions.
For Microbiology:
organism, morphology, virulence, diagnosis, treatment, prevention.
For Community Medicine:
definitions, indicators, formulas, programs, screening, epidemiology.
For Forensic Medicine:
definitions, findings, interpretation, medicolegal significance.
For Medicine/Pediatrics:
clinical clues, diagnosis, investigations, management, complications.
For Surgery:
clinical presentation, diagnosis, investigation, management, complications.
For OBG:
diagnosis, clinical features, investigations, management, complications.
For Ophthalmology/ENT:
clinical findings, diagnosis, investigations, treatment, complications.
FINAL QUALITY CHECK
Before returning the answer, silently verify:
1. Exactly 20 cards.
2. Every answer is ≤4 words.
3. No duplicate or reverse-duplicate cards.
4. Every card adds new educational value.
5. Core UHS PYT content is fully represented.
6. Added knowledge is standard and directly relevant.
7. Questions progress from foundation to mastery.
8. Several cards require conceptual connections.
9. Appropriate clinical/application recall is included.
10. No obscure postgraduate trivia.
11. Questions work for rapid-fire group revision.
12. A student mastering all 20 can reconstruct the PYT.
13. The deck also strengthens NEET-PG/INI-CET/USMLE foundations.
14. No unsupported or questionable medical claims.
15. Weak or repetitive cards have been replaced before output.
OUTPUT — STRICT JSON ONLY
Return exactly:
{
"topic": "Exact topic",
"cards": [
{
"serial_number": 1,
"question": "Question",
"answer": "Answer"
}
]
}
STRICT OUTPUT RULES
- Valid JSON only.
- Exactly 20 cards.
- serial_number must be 1 through 20.
- Every card must contain exactly 3 fields:
  - serial_number
  - question
  - answer
- Every answer must contain no more than 4 words.
- Do not include year_or_recent_question.
- Do not include UHS_HIGH_YIELD.
- Do not include UHS_APPLIED.
- Do not include difficulty labels.
- Do not include card-type labels.
- No Markdown.
- No explanations outside JSON.
- No additional fields.
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
    "cards"
  ],
  properties: {
    topic: {
      type: "string",
      minLength: 1
    },
    cards: {
      type: "array",
      minItems: REQUIRED_CARD_COUNT,
      maxItems: REQUIRED_CARD_COUNT,
      items: {
        type: "object",
        additionalProperties: false,
        required: [
          "serial_number",
          "question",
          "answer"
        ],
        properties: {
          serial_number: {
            type: "integer",
            minimum: 1,
            maximum: REQUIRED_CARD_COUNT
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

function countWords(value) {
  return String(value)
    .trim()
    .split(/\s+/u)
    .filter(Boolean)
    .length;
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
    `EXAM: ${
      row.exam ||
      "KNRUHS/UHS MBBS University Examination"
    }`,
    `SUBJECT: ${row.subject}`,
    `TOPIC / PYT: ${row.topic}`,
    "",
    "REVISION NOTES:",
    serializeJson(row[INPUT_COL]),
    "",
    "Generate exactly 20 database-ready flashcards now."
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
// VALIDATION
// ─────────────────────────────────────────────

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
      "Generated flashcards must be one JSON object"
    );
  }

  if (
    !Array.isArray(parsed.cards) ||
    parsed.cards.length !==
      REQUIRED_CARD_COUNT
  ) {
    throw new Error(
      `Generated output must contain exactly ${REQUIRED_CARD_COUNT} cards`
    );
  }

  const seenQuestions =
    new Set();

  const seenAnswers =
    new Map();

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

        const answerWordCount =
          countWords(answer);

        if (
          answerWordCount >
          MAX_ANSWER_WORDS
        ) {
          throw new Error(
            `Card ${position} answer has ${answerWordCount} words; maximum is ${MAX_ANSWER_WORDS}`
          );
        }

        const questionKey =
          question
            .replace(/\s+/g, " ")
            .trim()
            .toLowerCase();

        if (
          seenQuestions.has(questionKey)
        ) {
          throw new Error(
            `Card ${position} duplicates another question`
          );
        }

        seenQuestions.add(questionKey);

        const answerKey =
          answer
            .replace(/\s+/g, " ")
            .trim()
            .toLowerCase();

        if (
          !seenAnswers.has(answerKey)
        ) {
          seenAnswers.set(
            answerKey,
            0
          );
        }

        seenAnswers.set(
          answerKey,
          seenAnswers.get(answerKey) + 1
        );

        return {
          serial_number: position,
          question,
          answer
        };
      }
    );

  return {
    output: {
      topic: expectedTopic,
      cards
    },
    cardCount: cards.length,
    uniqueQuestionCount:
      seenQuestions.size,
    uniqueAnswerCount:
      seenAnswers.size
  };
}

// ─────────────────────────────────────────────
// OPENAI GENERATION
// No max_output_tokens supplied
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

          instructions:
            SYSTEM_PROMPT,

          input:
            buildUserInput(row),

          text: {
            format: {
              type: "json_schema",
              name:
                "mbbs_uhs_flashcards",
              strict: true,
              schema:
                FLASHCARD_SCHEMA
            }
          }
        });

      return validateAndNormalize(
        extractResponseText(response),
        row.topic
      );
    } catch (error) {
      lastError = error;

      if (
        isCreditExhaustionError(error)
      ) {
        throw error;
      }

      const validationError =
        /invalid JSON|exactly 20 cards|not an object|serial_number|non-empty string|answer has|duplicates another question/i.test(
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
      `Failed to release expired flashcard locks: ${error.message}`
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
      `Failed to find pending MBBS flashcard rows: ${error.message}`
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
      `Failed to save MBBS flashcards: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the row lock changed or flashcards already exist"
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
    `🧠 Generating UHS flashcards | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateFlashcards(row);

    await saveSuccess(
      row,
      result.output
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | CARDS=${result.cardCount} | UNIQUE QUESTIONS=${result.uniqueQuestionCount} | UNIQUE ANSWERS=${result.uniqueAnswerCount}`
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
    `🚀 MBBS UHS FLASHCARD WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE} | Cards=${REQUIRED_CARD_COUNT}`
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
    "❌ Fatal MBBS UHS flashcard worker error:",
    error
  );

  process.exit(1);
});
