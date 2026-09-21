require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// TABLE AND COLUMNS
// ─────────────────────────────────────────────

const TABLE = "neetss_surgery_pyt_source";

const INPUT_COL = "topic";
const OUTPUT_COL = "jsonb_output";

const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

// ─────────────────────────────────────────────
// ENVIRONMENT SETTINGS
// ─────────────────────────────────────────────

function parseIntegerEnv(name, fallback, min, max) {
  const value = Number.parseInt(
    process.env[name] ?? String(fallback),
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
  process.env.NEETSS_SURGERY_FLASHCARD_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = parseIntegerEnv(
  "NEETSS_SURGERY_FLASHCARD_LIMIT",
  50,
  1,
  100
);

const BATCH_SIZE = parseIntegerEnv(
  "NEETSS_SURGERY_FLASHCARD_BATCH_SIZE",
  10,
  1,
  20
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "NEETSS_SURGERY_FLASHCARD_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "NEETSS_SURGERY_FLASHCARD_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = parseIntegerEnv(
  "NEETSS_SURGERY_FLASHCARD_API_RETRIES",
  2,
  0,
  5
);

const CARD_COUNT = parseIntegerEnv(
  "NEETSS_SURGERY_FLASHCARD_COUNT",
  20,
  1,
  50
);

const WORKER_ID =
  process.env.NEETSS_SURGERY_FLASHCARD_WORKER_ID ||
  `neetss-surgery-flashcard-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// Paste the complete supplied prompt here.
// Keep [NUMBER] because it is replaced automatically.
// ─────────────────────────────────────────────

const PROMPT_TEMPLATE = String.raw`
You are a Senior NEET SS Surgery / ABSITE / NBME / AMBOSS surgical examiner.

Using the supplied topic/source, create [NUMBER] high-stakes clinical decision flashcards at superspecialty surgery level.

Prioritize indications, differential diagnosis, investigations, operative approach, surgical anatomy, extent of surgery, classification-driven management, intraoperative decisions, treatment, complications, bailout strategies and postoperative rescue.

Return ONLY valid JSON:

{
"topic": "Exact topic",
"cards": [
{
"question": "Clinical vignette",
"answer": "Precise tactical action"
}
]
}

CORE STANDARD

Every card must test what an experienced surgeon should DO, not merely what they should recognize.

1. DECISION > RECALL

Diagnosis recognition alone must NEVER answer the card.

The candidate should need to move from:
findings → interpretation → management decision.

At least 40% of cards must require ≥3 reasoning steps.

2. TWO-LOCK RULE

Every card must contain ≥2 independent management-changing variables.

Examples:

location + stage → operative extent

imaging + physiology → intervention

anatomy + tissue viability → reconstruction

contamination + stability → repair/diversion

classification-defining finding + anatomy → operation

failed treatment + new finding → rescue

3. COMPETING-PATHWAY RULE

Every vignette must contain ≥2 genuinely plausible management pathways.

Include the discriminator that makes ONE pathway the best answer.

If an informed candidate could answer without considering the competing pathway, rewrite the card.

4. RAW-DATA RULE

Never reveal the reasoning tool that the candidate should derive.

Do NOT write:
“Using the delta pressure…”
“Csendes type III…”
“Hinchey III…”
“Grade III injury…”

Instead provide the raw clinical, imaging, laboratory or operative findings and require the candidate to derive the consequence.

5. CLASSIFICATION → ACTION RULE

Whenever an established classification changes management:

Do not ask for the classification name alone.

Provide its defining raw findings and test the resulting:

observation vs intervention

operative approach

extent of resection

repair vs reconstruction

drainage strategy

salvage/bailout procedure.

Include classification names/numbers in the answer ONLY when they materially reinforce exam learning.

6. VARIABLE-FLIP RULE

Every card must contain at least ONE variable that would change the answer if altered.

Examples:

stable → unstable

<1/3 duct defect → >2/3 defect

viable bowel → necrotic bowel

contained leak → conduit necrosis

N0 superficial tumor → deeper/node-positive tumor

If changing no single variable changes management, rewrite the case.

7. NO ANSWER LEAKAGE

Never state:

the diagnosis being inferred

the management principle being tested

the classification being derived

the anatomical hazard being tested

the calculation/threshold being applied.

Give findings, not conclusions.

8. TACTICAL ANSWER RULE

Answers should usually be 5–20 words:

action verb + target + decisive technical modifier.

BAD:
“D2 lymphadenectomy.”

BETTER:
“Perform D2 clearance including the required extraperigastric nodal stations.”

BAD:
“Control bile leak.”

BETTER:
“Perform ERCP with transpapillary stenting for persistent controlled postoperative bile leak.”

BAD:
“Subtotal cholecystectomy.”

BETTER:
“Abandon unsafe Calot dissection and complete the appropriate subtotal bailout.”

9. TEST WHAT THE LABEL MEANS

Never stop at a named operation when its technical content is examinable.

Where relevant, test:

exact operative extent

nodal stations

structures removed

structures preserved

vascular pedicle

dissection plane

anatomical boundary

margin

reconstruction

drainage

criterion for completeness

criterion for abandoning the planned operation.

10. FUNCTIONAL SURGICAL ANATOMY

Never ask static anatomy merely for recall.

Anatomy must change what the surgeon does.

Test:
incision → structure at risk
traction → injury
plane → safe dissection
vascular supply → preserve/divide
location → lymphadenectomy
defect anatomy → reconstruction
anatomical distortion → bailout

11. BORDERLINE-THRESHOLD RULE

When an established threshold changes management, preferentially place the patient near that boundary.

Give raw values and make the candidate derive the consequence.

Use numerical thresholds ONLY when they are well established and relevant.

Never invent a number merely to make the card appear sophisticated.

12. CLASSIFICATION-BOUNDARY CARDS

For classification-dependent topics, include cards near adjacent management-changing categories.

The candidate should learn:

What single anatomical/physiological change converts treatment A into treatment B?

13. BAILOUT / PLAN-FAILURE RULE

At least 25% of cards must begin with a reasonable intended strategy that becomes unsafe or fails.

Test:

conversion

subtotal procedure

alternate exposure

damage-control strategy

alternative reconstruction

abandon vs continue

salvage vs resection.

14. IATROGENIC RESCUE RULE

Where relevant, include recognized intraoperative injuries.

Do NOT ask merely:
“What was injured?”

Ask:
What should the surgeon do now?

Specify enough anatomy and physiology to distinguish:
repair vs ligation vs reconstruction vs drainage vs referral.

15. POSTOPERATIVE RESCUE RULE

Include complications where timing + physiology + imaging + extent of failure determine management.

Examples:

controlled leak vs uncontrolled sepsis

localized collection vs generalized contamination

ischemia vs necrosis

early technical failure vs late stricture

stable patient vs shock.

16. SEQUENCING RULE

Test the next chronological action, not a vague definitive endpoint.

Account for what has already been:

attempted

ruled out

drained

imaged

resuscitated

treated

failed.

Prefer:
“What should be done next?”

17. FIRST-15-MINUTES RULE

For trauma, shock, airway, hemorrhage, sepsis and surgical emergencies, test the action that must occur before secondary diagnostics when delay changes outcome.

18. PHARMACOLOGY PRECISION RULE

When a drug decision is the actual surgical-management target, specify the correct drug + route, and dose only when the dose is standardized, established and genuinely exam-relevant.

Do NOT force doses into cards whose real learning objective is surgical.

19. PRECISION-WITHOUT-HALLUCINATION RULE

Never invent specificity.

A number, margin, station, dose, duration, pressure, classification threshold, device setting or technical distance may be used ONLY when supported by:

supplied source, OR

established authoritative surgical guidance.

If uncertain, use the correct qualitative decision rather than fabricated precision.

20. CONTROVERSY RULE

Never present a debated technique as universally mandatory.

When acceptable strategies vary with:

institution

surgeon expertise

guideline

disease extent

available technology,

construct the vignette so ONE option becomes clearly preferred, or avoid the disputed detail.

21. NO COMMON-SENSE CARDS

Reject cards answerable without specialist knowledge.

BAD:
“Major bleeding occurs. What next?”
→ “Control bleeding.”

GOOD:
Give the injured vessel/region, hemodynamics, accessibility and operative context, then test the specific exposure/control/repair strategy.

22. NO UMBRELLA ANSWERS

Reject vague answers such as:

“Treat infection.”
“Control bleeding.”
“Reduce immunosuppression.”
“Optimize patient.”
“Perform surgery.”
“Further evaluation.”
“Manage conservatively.”
“Perform D2.”
“Drain collection.”

Replace them with the actual management-changing maneuver.

23. SPECIFICITY CEILING

More detail is NOT automatically better.

Do not turn every answer into a miniature textbook paragraph.

Include only details that:

determine the decision,

distinguish competing options, or

are independently testable.

24. EXAM-YIELD FILTER

Prioritize decisions likely to distinguish senior candidates:

operate vs observe
test vs intervene
open vs minimally invasive
extent of resection
extent of lymphadenectomy
preserve vs divide
repair vs resect
primary repair vs reconstruction
drain vs reoperate
salvage vs abandon
definitive vs damage-control
complication rescue

Avoid decorative complexity and obscure trivia.

25. NON-DUPLICATION RULE

Two cards must not test essentially the same management decision with merely different ages, laboratory values or wording.

Each card must teach a distinct decision boundary, operative principle, anatomical hazard, complication or rescue pathway.

26. COVERAGE BLUEPRINT

Across the complete set, distribute cards across the topic where applicable:

10–15% diagnostic/staging decisions

15–20% indications/thresholds

20–25% operative approach/extent

15–20% functional surgical anatomy/technical execution

≥25% bailout, complication or rescue management

Adapt percentages when the topic itself makes a category irrelevant.

SILENT SENIOR-EXAMINER AUDIT

Before outputting EACH card, silently test:

Can diagnosis recognition alone answer it?
→ REWRITE.

Are ≥2 independent decision-changing variables required?
→ If no, REWRITE.

Are ≥2 plausible actions competing?
→ If no, REWRITE.

Does the stem reveal the rule/classification/answer?
→ REWRITE.

Is the answer merely a label?
→ Test its operative consequence.

Is the answer an umbrella goal?
→ Replace with the tactical maneuver.

Is anatomy functional?
→ If not, rewrite or replace.

Would changing one key variable change management?
→ If no, deepen the case.

Is a numerical value genuinely established?
→ If uncertain, remove it.

Is the recommendation controversial?
→ Add discriminating context or avoid absolute wording.

Could a junior answer through common sense?
→ Increase specialist discrimination.

Does another card test the same decision?
→ Replace it.

Is every sentence decision-relevant?
→ Remove decorative information.

Is there ONE defensible best next action?
→ If not, add the missing discriminator.

Does the answer teach something useful even when reviewed without the question?
→ If no, sharpen it.

FINAL STANDARD

Every question must be ≥40 words, clinically realistic and information-dense.

Difficulty must arise from choosing between plausible surgical pathways, not from obscure trivia, excessive length or invented precision.

The final deck should function as active-recall preparation for NEET SS Surgery while approaching ABSITE/NBME/AMBOSS clinical decision depth.
27. GUIDELINE-DECISION MATRIX RULE

When a disease has an established management algorithm, first internally identify the management-changing variables before writing cards.



Examples:



size

location

stage

duct/vessel involvement

enhancement

margin status

physiological stability

symptoms

biochemical marker

growth/progression

operative fitness.



Cards must test the transition between management pathways using these raw variables.



Do NOT write:
“Patient has high-risk features. What next?”



Instead give the actual findings and require the candidate to derive:



surveillance → further test → intervention → specific operation.



Never name the risk category in the stem if deriving that category is part of the reasoning.

THRESHOLD-PAIR RULE

When a validated numerical threshold changes management, do not merely create a card clearly above or below it.



Whenever useful, create paired or boundary cases around the threshold so the learner understands what changes management.



Example architecture:



Case A: value below threshold + no additional adverse feature
→ surveillance / further evaluation.



Case B: value crosses threshold OR acquires another decisive feature
→ intervention / surgery.



The cards must teach:



“Which single changed variable flips the management?”



Use only well-established thresholds supported by the supplied source or authoritative guideline.

MULTIVARIABLE RISK-INTEGRATION RULE

For diseases where management depends on several risk features, do NOT allow a single obvious clue to answer every card.



At least some cards must combine:



one major feature + one modifying feature + patient fitness



or



two intermediate features + progression over time



or



imaging + biochemical/cytological result + anatomy.



The candidate must determine whether the combined evidence warrants:



observe → repeat imaging → targeted diagnostic test → multidisciplinary review → operation.



Do not artificially combine variables when one established finding independently determines management.

TEST-RESULT → ACTION RULE

Do not stop at asking which investigation to order.



When the topic permits, include sequential cards in which the investigation has already been performed and the candidate must interpret the result and choose the next action.



Preferred architecture:



clinical problem → best test



then separately:



test result → interpretation → management



then, where appropriate:



failed/equivocal test → next diagnostic or operative decision.



Examples:



MRI finding → EUS



EUS morphology + fluid analysis → surveillance/resection



frozen section → extend/stop resection



CTA → embolization/reoperation



This prevents a deck from becoming a collection of “order CT / order MRI / perform EUS” questions.

COMPLICATION-SEVERITY ESCALATION RULE

For postoperative complications, never use the complication name alone to determine treatment.



Build the decision from:



timing + physiology + biochemical/imaging finding + source control + organ dysfunction + response to initial treatment.



Whenever applicable, create escalation boundaries such as:



abnormal laboratory/drain finding without clinical consequence
→ observe/support.



clinically relevant complication requiring changed management
→ targeted intervention.



organ failure, uncontrolled sepsis, hemorrhage or failed minimally invasive control
→ operative rescue.



Do NOT state the complication grade/classification in the stem when the candidate should derive it.



The candidate should infer the severity category from raw findings and select the corresponding intervention.

ADD TO SILENT EXAMINER AUDIT

Before accepting each card, additionally ask:



Is there an established management algorithm for this topic?
→ If yes, does the card test one of its real decision boundaries?

If a numerical threshold matters, did I give the raw value rather than the risk-category label?

Would moving that value across the threshold actually change management?
→ If no, the number is decorative; remove it.

Am I testing only “which test?”
→ If yes, consider a higher-value card testing interpretation of the result and subsequent action.

For a postoperative complication, have I supplied enough information to distinguish:
observe vs drain/intervene vs reoperate?

Am I forcing an exact number merely because specificity sounds sophisticated?
→ If yes, remove it unless authoritative and management-changing.

Could a guideline update change this exact threshold?
→ If yes, avoid unsupported certainty and follow the supplied/current authoritative source.
Output ONLY cards that pass ALL audits.
`.trim();

if (
  !PROMPT_TEMPLATE ||
  PROMPT_TEMPLATE.includes(
    "PASTE THE COMPLETE PROMPT"
  )
) {
  throw new Error(
    "Paste the complete prompt inside PROMPT_TEMPLATE"
  );
}

const SYSTEM_PROMPT =
  PROMPT_TEMPLATE.replaceAll(
    "[NUMBER]",
    String(CARD_COUNT)
  );

// ─────────────────────────────────────────────
// STRUCTURED OUTPUT SCHEMA
// ─────────────────────────────────────────────

const FLASHCARD_JSON_SCHEMA = {
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
      minItems: CARD_COUNT,
      maxItems: CARD_COUNT,
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
  return /no credits remaining|insufficient_quota|billing_hard_limit|credit balance/i.test(
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

  const message = getErrorText(error);

  return (
    status === 408 ||
    status === 409 ||
    status === 429 ||
    status >= 500 ||
    /timeout|temporar|unavailable|rate limit|ECONNRESET|ETIMEDOUT|socket hang up/i.test(
      message
    ) ||
    /invalid JSON|empty output|exactly .* cards|required at least 40 words|required 5–20 words|duplicate question|topic mismatch|is missing|is invalid/i.test(
      message
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

function normalizeComparable(value) {
  return String(value)
    .normalize("NFKC")
    .trim()
    .replace(/\s+/gu, " ")
    .toLocaleLowerCase("en");
}

function requireNonEmptyString(
  value,
  description
) {
  if (
    typeof value !== "string" ||
    !value.trim()
  ) {
    throw new Error(
      `${description} is missing`
    );
  }

  return value.trim();
}

// ─────────────────────────────────────────────
// BUILD INPUT FOR ONE TOPIC
// ─────────────────────────────────────────────

function buildUserInput(row) {
  return [
    "Return exactly one valid JSON object matching the required schema.",
    `Create exactly ${CARD_COUNT} flashcards.`,
    `Use this exact supplied topic: ${row.topic}`,
    "",
    "TOPIC:",
    row.topic
  ].join("\n");
}

// ─────────────────────────────────────────────
// EXTRACT RESPONSE
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

  const output =
    collected.join("\n").trim();

  if (!output) {
    throw new Error(
      "OpenAI returned empty output"
    );
  }

  return output;
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
// VALIDATE GENERATED FLASHCARDS
// ─────────────────────────────────────────────

function validateGeneratedOutput(
  rawOutput,
  row
) {
  const cleaned =
    cleanJsonText(rawOutput);

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

  const topic =
    requireNonEmptyString(
      parsed.topic,
      "Generated topic"
    );

  if (
    normalizeComparable(topic) !==
    normalizeComparable(row.topic)
  ) {
    throw new Error(
      `Generated topic mismatch: expected "${row.topic}", received "${topic}"`
    );
  }

  if (
    !Array.isArray(parsed.cards) ||
    parsed.cards.length !== CARD_COUNT
  ) {
    throw new Error(
      `Generated output contains ${
        Array.isArray(parsed.cards)
          ? parsed.cards.length
          : 0
      } cards; exactly ${CARD_COUNT} cards required`
    );
  }

  const seenQuestions =
    new Set();

  const cards = parsed.cards.map(
    (card, index) => {
      const cardNumber =
        index + 1;

      if (
        !card ||
        typeof card !== "object" ||
        Array.isArray(card)
      ) {
        throw new Error(
          `Card ${cardNumber} is invalid`
        );
      }

      const question =
        requireNonEmptyString(
          card.question,
          `Card ${cardNumber} question`
        );

      const answer =
        requireNonEmptyString(
          card.answer,
          `Card ${cardNumber} answer`
        );

      const questionWords =
        countWords(question);

      if (questionWords < 40) {
        throw new Error(
          `Card ${cardNumber} question has ${questionWords} words; required at least 40 words`
        );
      }

      const answerWords =
        countWords(answer);

      if (
        answerWords < 5 ||
        answerWords > 20
      ) {
        throw new Error(
          `Card ${cardNumber} answer has ${answerWords} words; required 5–20 words`
        );
      }

      const questionKey =
        normalizeComparable(
          question
        );

      if (
        seenQuestions.has(
          questionKey
        )
      ) {
        throw new Error(
          `Card ${cardNumber} contains a duplicate question`
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
    topic: row.topic,
    cards
  };
}

// ─────────────────────────────────────────────
// OPENAI GENERATION
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
                "neetss_surgery_flashcards",
              strict: true,
              schema:
                FLASHCARD_JSON_SCHEMA
            }
          }
        });

      const rawOutput =
        extractResponseText(
          response
        );

      return validateGeneratedOutput(
        rawOutput,
        row
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
        Math.floor(
          Math.random() * 300
        );

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
// Only rows with jsonb_output IS NULL.
// ─────────────────────────────────────────────

async function releaseExpiredLocks() {
  const cutoff = new Date(
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
      .eq(LOCK_COL, true)
      .is(OUTPUT_COL, null)
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
        [LOCK_AT_COL]:
          lockedAt
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
    .select("id")
    .not(INPUT_COL, "is", null)
    .neq(INPUT_COL, "")
    .is(OUTPUT_COL, null)
    .eq(LOCK_COL, false)
    .order(
      "serial_number",
      {
        ascending: true
      }
    )
    .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending flashcard rows: ${error.message}`
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
        [LOCK_COL]: false,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id)
      .eq(LOCK_COL, true)
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
  const { error } =
    await supabase
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
      .is(
        OUTPUT_COL,
        null
      );

  if (error) {
    console.error(
      `❌ Failed to release lock ${row.id}:`,
      error.message
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
// PROCESS ONE TOPIC
// ─────────────────────────────────────────────

async function processRow(row) {
  console.log(
    `🧠 Generating | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const generatedOutput =
      await generateFlashcards(
        row
      );

    await saveSuccess(
      row,
      generatedOutput
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | Cards=${generatedOutput.cards.length}`
    );

    return {
      success: true,
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
        "🛑 API credits exhausted. Worker will stop safely."
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
          rows[currentIndex]
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
        length: runnerCount
      },
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
    `🚀 NEET-SS SURGERY FLASHCARD WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE} | Cards/topic=${CARD_COUNT}`
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

      if (
        result.creditExhausted
      ) {
        console.error(
          "🛑 Worker stopped because the API account has no available credits."
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
          "🛑 Worker stopped: API credits exhausted."
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
    "❌ Fatal NEET-SS Surgery flashcard worker error:",
    error
  );

  process.exit(1);
});
