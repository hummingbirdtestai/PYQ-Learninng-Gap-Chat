"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// DATABASE CONFIGURATION
// topic → jsonb_output
// ─────────────────────────────────────────────

const TABLE = "neet_ss_pediatrics_pyt_source";

const INPUT_COL = "topic";
const OUTPUT_COL = "jsonb_output";

const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

const REQUIRED_CARD_COUNT = 20;
const MIN_ANSWER_WORDS = 3;
const MAX_ANSWER_WORDS = 6;

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
  process.env.NEET_SS_PEDS_FLASHCARD_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = parseIntegerEnv(
  "NEET_SS_PEDS_FLASHCARD_LIMIT",
  50,
  1,
  100
);

const BATCH_SIZE = parseIntegerEnv(
  "NEET_SS_PEDS_FLASHCARD_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "NEET_SS_PEDS_FLASHCARD_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "NEET_SS_PEDS_FLASHCARD_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = parseIntegerEnv(
  "NEET_SS_PEDS_FLASHCARD_API_RETRIES",
  2,
  0,
  5
);

const WORKER_ID =
  process.env.NEET_SS_PEDS_FLASHCARD_WORKER_ID ||
  `neet-ss-peds-flashcard-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// Paste your complete prompt between the backticks.
// Do not insert ${...} inside the prompt.
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = String.raw`
You are a Senior NEET SS Pediatrics / American Board of Pediatrics / NBME / AMBOSS examiner creating elite superspecialty pediatric clinical-decision flashcards.

GOAL
Convert the supplied pediatric topic/source into exactly 20 high-stakes flashcards at AMBOSS 3-step / NEET SS level.

The deck must test what an experienced pediatrician should DO NEXT, not merely what they should recognize.

INPUT
Topic: {{TOPIC}}
Source/Notes: {{SOURCE}}

SOURCE USE
Treat the supplied source as the minimum factual framework, not the maximum difficulty ceiling.
Preserve and test its important concepts. When the source is simple recall or diagnostic material, use Clinical Context Projection: assume the basic diagnosis/fact is Phase 1 and construct Phase 2/3 decisions involving treatment selection, response assessment, escalation, treatment failure, rescue, complications, or prevention.
You may enrich with well-established pediatric standards consistent with Nelson Pediatrics and major specialty guidelines. Never invent uncertain doses, thresholds, classifications, or recommendations.

CORE CARD ARCHITECTURE
Every card must require:
raw findings/data -> interpretation -> competing-pathway discrimination -> precise next action.

Diagnosis recognition alone must NEVER answer a card.

1. TRUE 3-STEP REASONING
At least 14/20 cards must require >=3 linked reasoning steps.
At least 6/20 should require 4-step reasoning.
A candidate who recognizes the diagnosis but ignores severity, physiology, treatment response, timing, comorbidity, or a threshold should get the card wrong.

2. TWO-LOCK MINIMUM
Every vignette must contain >=2 independent management-changing variables.
Prefer 3 when natural:
age + physiology + severity
treatment already given + response + new finding
laboratory value + imaging + clinical stability
drug exposure + adverse effect + competing disease
timing + organ dysfunction + microbiology

Decorative variables do not count.

3. COMPETING-PATHWAY LOCK
Every card must contain >=2 genuinely plausible actions.
Include one decisive discriminator that makes ONE action best.
Do not state the competing pathways explicitly.
If an informed candidate can answer from one buzzword, rewrite the card.

4. VARIABLE-FLIP RULE
Every card must contain at least one variable which, if changed, would change management.
Build cards near meaningful clinical boundaries whenever established guidance permits.

5. RAW-DATA RULE
Do not leak the interpretation.
Prefer raw:
vital signs
age/weight
percentiles or trajectory
laboratory values
drug dose/interval
timing
imaging findings
microbiology
oxygen requirement
fluid balance
organ-function data

Do not replace derivable findings with labels such as “unstable,” “severe,” “poor growth,” “prolonged QT,” “adequate trough,” or “treatment failure” when raw information can demonstrate them.

6. THRESHOLD-PAIR RULE
When an established threshold changes management, construct near-boundary cases.
Across the deck, include paired concepts where changing ONE value would flip:
observe -> intervene
standard therapy -> escalation
continue -> hold/withdraw
medical -> procedural/surgical
ward -> intensive support
empiric -> targeted therapy

Use numerical thresholds ONLY when authoritative and exam-relevant.

7. TREATMENT-FAILURE / BAILOUT DOMINANCE
At least 7/20 cards must begin AFTER a reasonable treatment has already been attempted.
Test:
inadequate response
breakthrough disease
toxicity
contraindication
new organ dysfunction
unexpected imaging/microbiology
recurrence
iatrogenic complication
need for rescue or alternate pathway

Do not merely ask for another diagnosis after treatment failure; ask for the tactical consequence.

8. TEST-RESULT -> ACTION
At least 4/20 cards must provide a completed investigation and require the immediate next management action.
Do not ask “what test next?” when the more advanced decision is what to DO with the result.

9. CLASSIFICATION -> ACTION
If staging/classification/risk category changes treatment, provide its defining findings and ask for the resulting action.
Never ask only for the classification name.

10. SEQUENCING
Prefer “What should be done next?”
Respect what has already been attempted, excluded, or failed.
In emergencies, test the first action whose delay changes outcome before secondary diagnostics.

11. PHARMACOLOGY PRECISION
When medication is the target, give the specific drug/class + route when relevant.
Give dose only when standardized, authoritative, and exam-relevant.
Test dose/interval escalation, withdrawal, substitution, toxicity rescue, or contraindication when appropriate.

12. PEDIATRIC-SPECIFIC DECISION VARIABLES
Age must change interpretation or action whenever included.
Use developmental trajectory rather than milestone trivia.
For neonates integrate gestational age, birth weight, postnatal age, feeding, glucose/bilirubin and respiratory support where relevant.
For respiratory disease test escalation across supportive care -> oxygen -> HFNC/NIV -> intubation using physiology.
For fluids/electrolytes distinguish resuscitation vs maintenance vs deficit and rapid vs controlled correction.
For infection integrate host risk + focus + microbiology + organ dysfunction.
For genetic/metabolic disease prioritize immediate pathway stabilization over syndrome naming.
For cardiac disease integrate cyanosis, pulses, ductal physiology, rhythm/QTc and hemodynamics.
For neurology preserve stabilization -> correction -> seizure termination -> second-line therapy -> definitive diagnostics.
For endocrine disease use paired/dynamic biochemical data to dictate treatment.

13. COMPLICATION PREVENTION
At least 3/20 cards must test prevention of irreversible morbidity after the diagnosis is already known.
Prevention must require a clinical decision, not generic counseling.

14. NO ANSWER LEAKAGE
Never state in the question:
the diagnosis being inferred if that gives away management
the severity/stage the learner should derive
the management principle being tested
that treatment has “failed” if raw data can show it
that a value is abnormal when the candidate should interpret it.

15. TACTICAL ANSWERS
Every answer must be exactly 3-6 words.
Architecture:
ACTION VERB + TARGET + DECISIVE TECHNICAL MODIFIER.

Answers must be executable and specific.

BANNED answer verbs:
consider
evaluate
investigate
assess
verify
monitor
reassess
ensure
check
rule out
arrange
manage

Prefer:
initiate
administer
stop
hold
increase
decrease
switch
shorten
intubate
drain
excise
refer urgently
repeat
replace
correct
start
continue
remove
repair

Do not use vague umbrella answers such as:
“Treat infection”
“Optimize therapy”
“Further workup”
“Supportive management”
“Escalate care”

16. NON-DUPLICATION
Each card must test a different decision boundary.
Two cards may involve the same disease feature only if the variable flip produces a different action.

17. DIFFICULTY MIX
Maximum 2/20 cards may be primarily diagnosis/staging.
The remaining >=18 must test management, treatment extent, escalation, sequencing, rescue, complication response, or prevention.

TARGET DISTRIBUTION
2 cards: diagnostic/risk/staging decisions that directly change management
3 cards: indication or treatment-initiation thresholds
4 cards: treatment selection/intensity/extent
4 cards: test-result -> action / pharmacologic or physiologic adjustment
5 cards: treatment failure, bailout, rescue, or complication management
2 cards: long-term complication prevention

Overlap between categories is allowed, but all 20 must remain distinct.

ANTI-EASY-CARD FILTER
REJECT and rewrite any card if:
- one buzzword directly reveals the answer
- diagnosis recognition alone solves it
- only one plausible action exists before reading the discriminator
- the answer is generic
- age/labs/imaging are decorative
- the question asks textbook recall rather than a decision
- the same principle was already tested
- an answer can be given safely without using at least two stem variables
- the case says “refractory,” “severe,” “unstable,” “contraindicated,” or “treatment failure” instead of showing why
- a specialist would regard the answer as obvious from a single clue

AMBOSS 3-STEP STRESS TEST
Before accepting each card silently ask:
A. What is reasoning step 1?
B. What is reasoning step 2?
C. What is reasoning step 3?
D. What competing action is plausible?
E. Which exact stem variable defeats that competing action?
F. Which single variable could flip the final answer?

If A-F cannot all be answered clearly, REWRITE THE CARD.

SILENT FINAL AUDIT
Before output, verify:
- exactly 20 cards
- >=14 cards require >=3 reasoning steps
- >=6 require 4-step reasoning
- >=7 begin after treatment has been attempted
- >=4 are test-result -> action
- >=3 test complication prevention
- every card has >=2 management-changing variables
- every card has a competing pathway
- every card has a variable flip
- raw data replace diagnostic/adjectival leakage whenever possible
- every answer contains exactly 3-6 words
- count answer words individually; silently rewrite any answer outside the 3-6-word limit
- no banned soft verbs appear in answers
- no duplicate decision boundaries
- no invented numerical thresholds
- no answer is obtainable from a single buzzword

If any criterion fails, silently rewrite failing cards before returning the deck.

OUTPUT
Return ONLY valid JSON. No markdown, commentary, headings, explanations, or code fences.

{
  "topic": "Exact topic",
  "cards": [
    {
      "question": "Clinical decision vignette",
      "answer": "Precise 3-6 word tactical action"
    }
  ]
}
`.trim();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT ===
    "PASTE YOUR COMPLETE SYSTEM PROMPT HERE"
) {
  throw new Error(
    "Paste your complete SYSTEM_PROMPT before starting the worker"
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
          "question",
          "answer"
        ],
        properties: {
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

function normalizeForComparison(value) {
  return String(value)
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .toLowerCase();
}

// ─────────────────────────────────────────────
// BUILD MODEL INPUT
// Only topic is supplied to the model.
// ─────────────────────────────────────────────

function buildUserInput(row) {
  return requireString(
    row[INPUT_COL],
    "Topic"
  );
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
// OUTPUT VALIDATION
// ─────────────────────────────────────────────

const BANNED_ANSWER_VERBS = [
  "consider",
  "evaluate",
  "investigate",
  "assess",
  "verify",
  "monitor",
  "reassess",
  "ensure",
  "check",
  "rule out",
  "arrange",
  "manage"
];

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

  const rootKeys =
    Object.keys(parsed).sort();

  if (
    rootKeys.join(",") !==
    "cards,topic"
  ) {
    throw new Error(
      "Generated output must contain exactly topic and cards"
    );
  }

  if (
    !Array.isArray(parsed.cards) ||
    parsed.cards.length !== REQUIRED_CARD_COUNT
  ) {
    throw new Error(
      `Generated output must contain exactly ${REQUIRED_CARD_COUNT} cards`
    );
  }

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

        const cardKeys =
          Object.keys(card).sort();

        if (
          cardKeys.join(",") !==
          "answer,question"
        ) {
          throw new Error(
            `Card ${position} must contain exactly question and answer`
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
          answerWordCount < MIN_ANSWER_WORDS ||
          answerWordCount > MAX_ANSWER_WORDS
        ) {
          throw new Error(
            `Card ${position} answer has ${answerWordCount} words; required range is ${MIN_ANSWER_WORDS}-${MAX_ANSWER_WORDS}`
          );
        }

        for (
          const bannedVerb of
          BANNED_ANSWER_VERBS
        ) {
          const escapedVerb =
            bannedVerb.replace(
              /[.*+?^${}()|[\]\\]/g,
              "\\$&"
            );

          const pattern =
            new RegExp(
              `\\b${escapedVerb.replace(
                /\s+/g,
                "\\s+"
              )}\\b`,
              "i"
            );

          if (pattern.test(answer)) {
            throw new Error(
              `Card ${position} answer uses banned wording: ${bannedVerb}`
            );
          }
        }

        const questionKey =
          normalizeForComparison(
            question
          );

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
    cardCount:
      cards.length,
    uniqueQuestionCount:
      seenQuestions.size
  };
}

// ─────────────────────────────────────────────
// OPENAI GENERATION
// Only the topic is passed as input.
// No max_output_tokens supplied.
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
                "neet_ss_pediatrics_flashcards",
              strict: true,
              schema:
                FLASHCARD_SCHEMA
            }
          }
        });

      return validateAndNormalize(
        extractResponseText(response),
        row[INPUT_COL]
      );
    } catch (error) {
      lastError = error;

      if (
        isCreditExhaustionError(error)
      ) {
        throw error;
      }

      const validationError =
        /invalid JSON|one JSON object|exactly topic and cards|exactly 20 cards|not an object|exactly question and answer|non-empty string|required range|banned wording|duplicates another question/i.test(
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
      "Flashcard generation failed"
    )
  );
}

// ─────────────────────────────────────────────
// RELEASE EXPIRED LOCKS
// Only rows without jsonb_output are unlocked.
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
      `Failed to release expired Pediatrics flashcard locks: ${error.message}`
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
// topic IS NOT NULL
// jsonb_output IS NULL
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
        "serial_number",
        INPUT_COL
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
      `Failed to find pending Pediatrics flashcard rows: ${error.message}`
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
      result.status === "fulfilled" &&
      result.value
    ) {
      claimedRows.push(
        result.value
      );
    } else if (
      result.status === "rejected"
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
// Prevents overwriting existing jsonb_output.
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
      `Failed to save Pediatrics flashcards: ${error.message}`
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
    `👶 Generating Pediatrics flashcards | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateFlashcards(row);

    await saveSuccess(
      row,
      result.output
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | CARDS=${result.cardCount} | UNIQUE=${result.uniqueQuestionCount}`
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

async function processWithConcurrency(rows) {
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
    `🚀 NEET SS PEDIATRICS FLASHCARD WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Input=${INPUT_COL} only | Output=${OUTPUT_COL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE} | Cards=${REQUIRED_CARD_COUNT}`
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
        `📥 Claimed ${rows.length} Pediatrics topic(s)`
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
    "❌ Fatal Pediatrics flashcard worker error:",
    error
  );

  process.exit(1);
});
