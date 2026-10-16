"use strict";


require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

const TABLE = "usmle_step1_topic_grouped";
const INPUT_COL = "notes_json";
const OUTPUT_COL = "flashcard";
const LOCK_COL = "flashcard_generation_lock";
const LOCK_AT_COL = "flashcard_generation_locked_at";

const EXPECTED_CARD_COUNT = 10;

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
  process.env.USMLE_STEP1_FLASHCARD_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "USMLE_STEP1_FLASHCARD_LIMIT",
  50,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "USMLE_STEP1_FLASHCARD_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "USMLE_STEP1_FLASHCARD_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "USMLE_STEP1_FLASHCARD_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "USMLE_STEP1_FLASHCARD_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = integerEnv(
  "USMLE_STEP1_FLASHCARD_MAX_OUTPUT_TOKENS",
  16000,
  2000,
  50000
);

const WORKER_ID =
  process.env.USMLE_STEP1_FLASHCARD_WORKER_ID ||
  `usmle-step1-flashcard-${process.pid}-${Math.random()
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
USMLE STEP 1 — HIGH-YIELD NOTES → ADVANCED CLINICAL REASONING FLASHCARDS
MASTER GENERATION PROMPT | ORIGINAL NBME-STYLE / AMBOSS-STYLE / UWORLD-STYLE DEPTH

ROLE
You are a senior USMLE Step 1 question writer, basic-science faculty examiner, and clinical reasoning editor. Generate ORIGINAL high-discrimination active-recall flashcards inspired by the reasoning depth of NBME, AMBOSS, UWorld, and USMLE-Rx. Do not reproduce proprietary questions or claim endorsement by those organizations.

INPUT


NUMBER_OF_FLASHCARDS:10

MISSION
Transform the supplied high-yield notes into exactly NUMBER_OF_FLASHCARDS medically accurate, clinically integrated, high-value flashcards. The notes are the primary content boundary: preserve their examinable concepts and distinctions; do not introduce unrelated obscure material. Apply established USMLE Step 1 science to contextualize them, but do not invent facts, doses, lab cutoffs, genetics, or pathways. If source notes conflict with well-established science, do not propagate an unsafe or false statement; prefer a different supported concept and flag uncertainty internally.

OUTPUT CONTRACT — STRICT
Return ONLY valid JSON, with precisely this structure:
{
  "topic": "Exact topic name",
  "cards": [
    {
      "question": "Clinical or experimental vignette ending in one focused question?",
      "answer": "Precise mechanism, inference, predicted change, or discriminating conclusion."
    }
  ]
}
Exactly NUMBER_OF_FLASHCARDS card objects. No additional keys, no Markdown fences, no comments, no nulls, no prefatory text. Use valid escaped JSON strings. Do not include explanations as extra keys; make the answer self-contained and specific. Do not insert numbering into question/answer text.

NONNEGOTIABLE REASONING STANDARD
1. THREE-STEP CHAIN: At least 60% of cards must require THREE meaningful steps: (1) infer an unlabelled clinical/experimental state from raw findings; (2) interpret a second independent modifier (physiology, location, enzyme function, drug exposure, timing, genetics, lab trend, receptor effect, immune state); (3) predict the downstream mechanism, lab pattern, pathology, complication, pharmacologic response, or diagnostic interpretation. Remaining cards must require at least TWO genuine steps.
2. TWO-LOCK RULE: Every card includes at least TWO independent, necessary clues. The answer must change if an important modifier changes. Never pad the stem with irrelevant demographic or laboratory details.
3. POST-DIAGNOSIS COMPETITION: Even after recognizing the disease, at least TWO mechanistic possibilities should remain plausible until the modifier is interpreted. If a disease name alone gives the answer, REWRITE.
4. VARIABLE-FLIP RULE: Internally identify a single clinically meaningful change (e.g., competitive vs noncompetitive inhibition, low vs high ACTH, direct vs indirect bilirubin, pre- vs post-synaptic lesion, high vs low anion gap) that would change the answer. Put the discriminating raw observation in the question, not the named rule.
5. RAW-DATA RULE: Give relevant history, physical findings, labs, histology, inheritance, enzyme activity, imaging, pharmacodynamics, or experimental results. Do not reveal the syndrome, diagnosis, mechanism, transporter, receptor, enzyme, or pathway in the stem when identifying it is part of the reasoning.
6. FIRST-PRINCIPLES OVER BUZZWORDS: Reject simple disease→gene, organism→toxin, drug→side effect, vitamin→deficiency, receptor→second messenger, or histology→diagnosis association cards unless a second inference is required. A named association can be STEP 1, never the whole question.
7. STEP 1 SCOPE: Emphasize mechanisms, not advanced specialty treatment algorithms. High-yield targets include molecular/cellular mechanisms, enzyme or transport defects, inheritance and penetrance, signaling pathways, biochemical flux, metabolic consequences, pharmacologic targets, resistance mechanisms, microbial virulence, immunopathology, histopathology, endocrine feedback, renal and cardiopulmonary physiology, neuroanatomical localization, and interpretation of diagnostic tests. Ask clinical next steps only when they genuinely test Step 1 mechanisms or safety priorities.
8. PHYSIOLOGIC CAUSALITY: Prefer prediction: if X is altered, what happens to Y and why? Require directional accuracy (increase/decrease), tissue/site specificity, feedback behavior, and time course when meaningful.
9. INTEGRATED ORGAN-SYSTEM REASONING: When justified by the notes, bridge two domains (e.g., renal physiology + pharmacology, genetics + pathology, microbiology + immunology, endocrine physiology + biochemistry). Do not combine domains artificially.
10. COUNTERFACTUAL BOUNDARY: Where notes support adjacent categories, construct contrasting scenarios across a decision boundary (e.g., primary vs secondary endocrine disorder, nephritic vs nephrotic physiology, receptor agonism vs blockade, enzyme deficiency vs cofactor deficiency). Avoid duplicate cards testing the same fact with cosmetic changes.
11. MECHANISM BEFORE MANAGEMENT: If a clinical treatment appears, test WHY it works, WHEN its physiology changes, or WHAT laboratory consequence follows, not a superficial drug-selection prompt.
12. EXPERIMENTAL INTERPRETATION: Include appropriately scoped graphs, knockout models, enzyme kinetics, Western blot/immunostaining, blood-gas results, pedigree analysis, or challenge tests when they are genuinely present in or strongly supported by the notes. Describe raw results clearly; no invented data.
13. REALISTIC CLINICAL DATA: Use coherent age, symptom chronology, vital signs, CBC/chemistry/ABG values, imaging, and drug exposures. Never invent a clinically significant threshold, mutation effect, dose, or normal range. Avoid numbers unless needed for discrimination.
14. ANSWER PRECISION: Usually 8–25 words, extending to 35 only when required to state the mechanistic cause-and-effect. Give the specific mechanism or predicted change, not a vague umbrella phrase. The answer should teach the key distinction when reviewed independently.
15. QUESTION QUALITY: Normally 35–75 words for clinical vignettes, concise but information-dense. Shorter experimental prompts are acceptable when fully discriminating. End with a neutral, focused question. Avoid leading questions that telegraph the answer.
16. NO ARTIFICIAL TRICKERY: One defensible answer per card. Difficulty comes from competing causal explanations, not missing facts, ambiguous wording, contrived exceptions, or obscure trivia.
17. HIGH-YIELD COVERAGE: Map source notes into distinct examinable learning objectives BEFORE writing. Distribute cards across important subtopics in proportion to their conceptual importance, not simply their source length. Cover multiple subtopics when possible; do not fabricate content to reach a quota.
18. NO CROSS-CARD BLEED: Each question and answer must refer to the SAME patient/experiment and EXACT target concept. Every card must stand alone, without referencing prior cards.
19. NO NEGATIVE-STEM SHORTCUTS: Prefer positive prediction and mechanism prompts. Avoid 'EXCEPT', 'NOT', and 'least likely' unless absolutely necessary.
20. REFERENCE-INDEPENDENT ANSWERS: Never answer 'the first option', 'the above enzyme', 'as described', or 'see explanation'. Write the named, specific answer.

QUALITY TARGETS FOR THE COMPLETE DECK
- >=60% true 3-step causal reasoning.
- 100% require at least two independent discriminating clues.
- >=30% should use a meaningful mechanistic modifier or counterfactual where source coverage permits.
- >=20% should test a predicted downstream change (lab, physiologic, cellular, or pharmacologic) where relevant.
- Avoid overrepresenting diagnosis-only recognition, memorized eponyms, and gene-name recall.
- Across a large deck, mix clinical vignettes, lab interpretation, mechanism prediction, pharmacology, genetics, microbiology/immunology, and experiment interpretation as the source permits. Do NOT force irrelevant quotas on a small or narrow topic.

ILLUSTRATIVE EXAMPLES — MODEL THE REASONING, NOT THE SUBJECT MATTER
These examples are demonstrations ONLY. Do not include them in generated output unless they are specifically part of the input notes. The required production output remains only {"topic": ..., "cards": ...}.

EXAMPLE 1 — RENAL PHYSIOLOGY + PHARMACOLOGY
QUESTION: A patient with hypertension develops weakness after starting a diuretic. Potassium is 2.8 mEq/L and bicarbonate is elevated. Urinary chloride remains high. A second medication blocks mineralocorticoid receptors. Which collecting-duct transport change explains the expected correction of potassium loss?
ANSWER: Reduced ENaC-mediated sodium reabsorption lowers lumen-negative potential, decreasing principal-cell potassium secretion.
WHY IT IS ADVANCED: Diuretic physiology -> distal sodium delivery and voltage -> predict receptor blockade effect; not merely 'name the drug'.

EXAMPLE 2 — ENDOCRINE FEEDBACK + RECEPTOR BIOLOGY
QUESTION: A patient has weight loss, tremor, diffuse thyroid enlargement, suppressed TSH, and increased free T4. After administration of a TSH-receptor antagonist, thyroid hormone production falls despite persistently low pituitary TSH. Which intracellular signaling change directly accounts for the drug's effect?
ANSWER: Reduced TSH-receptor Gs activation decreases adenylyl cyclase activity and intracellular cAMP in thyroid follicular cells.
WHY IT IS ADVANCED: Recognize primary thyroid stimulation -> separate pituitary feedback from receptor signaling -> predict intracellular effect.

EXAMPLE 3 — MICROBIOLOGY + IMMUNOLOGY
QUESTION: An unvaccinated child develops a gray pharyngeal membrane and myocarditis. The organism's toxin enters host cells and inhibits elongation factor 2. A mutant toxin still binds its receptor but cannot catalyze ADP-ribosylation. Which cellular process is preserved despite toxin uptake?
ANSWER: Ribosomal protein-chain elongation remains functional because EF-2 is not inactivated by ADP-ribosylation.
WHY IT IS ADVANCED: Clinical inference -> distinguish toxin binding from catalytic activity -> predict downstream host-cell function.

EXAMPLE 4 — BIOCHEMISTRY + METABOLIC PREDICTION
QUESTION: A fasting infant develops hypoketotic hypoglycemia and elevated long-chain acylcarnitines. During illness, hepatic beta-oxidation is inadequate despite mobilization of adipose triglycerides. Which paired metabolic change explains the impaired maintenance of blood glucose?
ANSWER: Reduced hepatic acetyl-CoA limits pyruvate carboxylase activation, while diminished beta-oxidation reduces ATP for gluconeogenesis.
WHY IT IS ADVANCED: Interpret metabolic phenotype -> trace energy and allosteric consequences -> explain impaired gluconeogenesis.

EXAMPLE 5 — GENETICS + MOLECULAR PATHOLOGY
QUESTION: A patient has early-onset colon tumors and a family history spanning successive generations. Tumor testing shows microsatellite instability, but immunostaining preserves one mismatch-repair protein and loses its binding partner. What molecular event best explains the selective loss of the second protein?
ANSWER: Destabilization of the partner protein in a defective mismatch-repair heterodimer causes loss of its detectable expression.
WHY IT IS ADVANCED: Integrate hereditary phenotype -> protein partnership -> interpret immunostaining rather than simply naming a syndrome.

REJECTED FLASHCARD EXAMPLES — NEVER GENERATE
BAD: 'Which gene causes cystic fibrosis?' -> 'CFTR.' [One-step association]
BETTER ARCHITECTURE: Give pancreatic insufficiency, sweat chloride, and a drug-response modifier; ask how an ion-transport change produces the observed secretory defect.
BAD: 'What is the mechanism of beta blockers?' -> 'Block beta receptors.' [Definition]
BETTER ARCHITECTURE: Give an adrenergic challenge and altered heart rate/renin response; ask which tissue-specific second messenger changes.
BAD: 'Which organism causes pseudomembranous pharyngitis?' -> 'Corynebacterium diphtheriae.' [Pattern matching]
BETTER ARCHITECTURE: Provide clinical context plus a toxin functional mutation; ask the consequence for translation.

SILENT 10/10 SENIOR-EXAMINER AUDIT — APPLY TO EVERY CARD
A. Is the answer impossible to retrieve by a single buzzword or memorized association? If not, REWRITE.
B. Are two independent clues genuinely required? If not, REWRITE.
C. Does the card demand >=2 steps, and do >=60% of the deck demand 3? If not, REWRITE.
D. Does a specific modifier discriminate between at least two plausible mechanistic interpretations? If not, REWRITE.
E. Would changing a relevant variable change the answer? If not, REWRITE.
F. Are the diagnosis and tested mechanism hidden when they should be inferred? If not, REWRITE.
G. Is the causal chain biochemically, physiologically, genetically, and clinically accurate? If not, REWRITE.
H. Is the target squarely within Step 1 scope? If not, REWRITE.
I. Is the question answerable from supplied high-yield notes plus established Step 1 fundamentals? If not, REPLACE.
J. Is the answer precise, short, standalone, and mechanistically explanatory? If not, REWRITE.
K. Are the question and answer unambiguously matched? If not, REWRITE.
L. Is the card meaningfully different from every other card? If not, REPLACE.
M. Are numerical cutoffs, drug effects, and genetic associations verified rather than invented? If not, REMOVE or REWRITE.
N. Does each clinical or experimental detail actually help discriminate the answer? If not, DELETE.
O. Does the deck cover the source's major high-yield concepts rather than only its easiest associations? If not, REBALANCE.
P. Would a well-prepared Step 1 candidate have to reason rather than merely recognize a phrase? If not, REWRITE.

FINAL COMMAND
Internally draft, challenge, revise, and validate the complete deck. Do not print internal reasoning, audits, examples, or explanations. Return ONLY syntactically valid JSON with the exact topic and exactly NUMBER_OF_FLASHCARDS question/answer objects. Do not promise or self-report a score; enforce the rules above to target top-tier reasoning quality.
  */ };

  const source = promptContainer.toString();
  const start = source.indexOf("/*") + 2;
  const end = source.lastIndexOf("*/");

  return source.slice(start, end).trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE USMLE STEP 1 FLASHCARD PROMPT HERE"
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

function requiredText(value, label) {
  const text = String(value ?? "").trim();

  if (!text) {
    throw new Error(`${label} must be a non-empty string`);
  }

  return text;
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
      return text;
    }
  }

  return JSON.stringify(value, null, 2);
}

function buildInput(row) {
  return [
    `TOPIC: ${requiredText(row.topic, "Topic")}`,
    "",
    "INPUT NOTES:",
    serializeNotes(row[INPUT_COL]),
    "",
    `Create exactly ${EXPECTED_CARD_COUNT} USMLE Step 1 flashcards.`,
    "Return only the required valid JSON object.",
    "Do not use Markdown code fences.",
    "Do not add text before or after the JSON."
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
        "Output does not contain a valid JSON object"
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

function validateString(value, label, minimumLength = 1) {
  if (
    typeof value !== "string" ||
    value.trim().length < minimumLength
  ) {
    throw new Error(
      `${label} must be a non-empty string`
    );
  }

  return value.trim();
}

function normalizeComparableText(value) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function validateFlashcardOutput(value, expectedTopic) {
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value)
  ) {
    throw new Error(
      "Flashcard output must be one JSON object"
    );
  }

  const topLevelKeys = Object.keys(value).sort();
  const expectedTopLevelKeys = ["cards", "topic"].sort();

  if (
    JSON.stringify(topLevelKeys) !==
    JSON.stringify(expectedTopLevelKeys)
  ) {
    throw new Error(
      'Output must contain exactly the keys "topic" and "cards"'
    );
  }

  const topic = validateString(
    value.topic,
    "Output topic"
  );

  if (
    normalizeComparableText(topic) !==
    normalizeComparableText(expectedTopic)
  ) {
    throw new Error(
      `Output topic does not match source topic. Expected "${expectedTopic}", received "${topic}"`
    );
  }

  if (!Array.isArray(value.cards)) {
    throw new Error(
      '"cards" must be an array'
    );
  }

  if (value.cards.length !== EXPECTED_CARD_COUNT) {
    throw new Error(
      `Expected exactly ${EXPECTED_CARD_COUNT} cards, received ${value.cards.length}`
    );
  }

  const questionSet = new Set();
  const answerSet = new Set();

  value.cards.forEach((card, index) => {
    const cardNumber = index + 1;

    if (
      !card ||
      typeof card !== "object" ||
      Array.isArray(card)
    ) {
      throw new Error(
        `Card ${cardNumber} must be an object`
      );
    }

    const cardKeys = Object.keys(card).sort();
    const expectedCardKeys = ["answer", "question"].sort();

    if (
      JSON.stringify(cardKeys) !==
      JSON.stringify(expectedCardKeys)
    ) {
      throw new Error(
        `Card ${cardNumber} must contain exactly "question" and "answer"`
      );
    }

    const question = validateString(
      card.question,
      `Card ${cardNumber} question`,
      20
    );

    const answer = validateString(
      card.answer,
      `Card ${cardNumber} answer`,
      3
    );

    if (!question.endsWith("?")) {
      throw new Error(
        `Card ${cardNumber} question must end with a question mark`
      );
    }

    const normalizedQuestion =
      normalizeComparableText(question);

    const normalizedAnswer =
      normalizeComparableText(answer);

    if (questionSet.has(normalizedQuestion)) {
      throw new Error(
        `Card ${cardNumber} duplicates another question`
      );
    }

    if (answerSet.has(normalizedAnswer)) {
      throw new Error(
        `Card ${cardNumber} duplicates another answer`
      );
    }

    questionSet.add(normalizedQuestion);
    answerSet.add(normalizedAnswer);

    card.question = question;
    card.answer = answer;
  });

  value.topic = expectedTopic;

  return value;
}

async function generateFlashcards(row) {
  let lastError;

  for (
    let attempt = 0;
    attempt <= API_RETRIES;
    attempt += 1
  ) {
    try {
      const response = await openai.responses.create({
        model: MODEL,
        instructions: SYSTEM_PROMPT,
        input: buildInput(row),
        max_output_tokens: MAX_OUTPUT_TOKENS
      });

      const rawText = extractText(response);
      const parsed = parseJsonOutput(rawText);

      const validated = validateFlashcardOutput(
        parsed,
        requiredText(row.topic, "Topic")
      );

      return {
        json: validated,
        characters: rawText.length,
        cards: validated.cards.length
      };
    } catch (error) {
      lastError = error;

      if (isCreditError(error)) {
        throw error;
      }

      const validationFailure =
        /empty output|JSON|Flashcard|cards|Card|question|answer|topic|notes_json|duplicate|question mark|non-empty/i.test(
          errorText(error)
        );

      if (
        attempt === API_RETRIES ||
        (!isRetryable(error) && !validationFailure)
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
    new Error("USMLE Step 1 flashcard generation failed")
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
      `Schema check failed. Run the SQL migration first: ${error.message}`
    );
  }
}

async function releaseExpiredLocks() {
  const cutoff = new Date(
    Date.now() - LOCK_TTL_MIN * 60 * 1000
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
      `Failed to release expired flashcard locks: ${error.message}`
    );
  }
}

async function lockRow(candidate) {
  const lockedAt = new Date().toISOString();

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
        "topic",
        "source_row_count",
        "topic_subjectwise_serial_number",
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

  const { data, error } = await supabase
    .from(TABLE)
    .select(
      "id,subject,topic,topic_subjectwise_serial_number"
    )
    .eq(LOCK_COL, false)
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .order("subject", {
      ascending: true
    })
    .order("topic_subjectwise_serial_number", {
      ascending: true,
      nullsFirst: false
    })
    .order("topic", {
      ascending: true
    })
    .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending flashcard rows: ${error.message}`
    );
  }

  if (!data?.length) {
    return [];
  }

  const results = await Promise.allSettled(
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

async function saveSuccess(row, flashcardJson) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: flashcardJson,
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null
    })
    .eq("id", row.id)
    .eq(LOCK_COL, true)
    .eq(LOCK_AT_COL, row[LOCK_AT_COL])
    .is(OUTPUT_COL, null)
    .select("id");

  if (error) {
    throw new Error(
      `Failed to save flashcards: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or flashcard output already exists"
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
    .eq(LOCK_AT_COL, row[LOCK_AT_COL])
    .is(OUTPUT_COL, null);

  if (error) {
    console.error(
      `Failed to release flashcard lock ${row.id}: ${error.message}`
    );
  }
}

async function processRow(row) {
  console.log(
    `Generating USMLE Step 1 flashcards | ${row.subject} | ${
      row.topic_subjectwise_serial_number ?? "-"
    } | ${row.topic}`
  );

  try {
    const result = await generateFlashcards(row);

    await saveSuccess(row, result.json);

    console.log(
      `Completed | ${row.subject} | ${
        row.topic_subjectwise_serial_number ?? "-"
      } | ${row.topic} | cards=${result.cards} | characters=${result.characters}`
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
      `Failed | ${row.subject} | ${
        row.topic_subjectwise_serial_number ?? "-"
      } | ${row.topic}: ${errorText(error)}`
    );

    return {
      creditExhausted: false
    };
  }
}

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

      const result = await processRow(rows[index]);

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
      rows.slice(next).map(releaseLock)
    );
  }

  return {
    creditExhausted
  };
}

async function main() {
  await verifySchema();

  console.log(
    `USMLE STEP 1 NOTES -> FLASHCARD WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `Model=${MODEL} | Table=${TABLE} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | Lock=${LOCK_COL} | Cards=${EXPECTED_CARD_COUNT} | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY} | MaxOutputTokens=${MAX_OUTPUT_TOKENS}`
  );

  while (true) {
    try {
      const rows = await claimRows(PICKUP_LIMIT);

      if (!rows.length) {
        await sleep(LOOP_SLEEP_MS);
        continue;
      }

      console.log(
        `Claimed ${rows.length} pending flashcard row(s)`
      );

      const result = await processBatch(rows);

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
        Math.max(LOOP_SLEEP_MS, 2000)
      );
    }
  }
}

main().catch((error) => {
  console.error(
    "Fatal USMLE Step 1 flashcard worker error:",
    error
  );

  process.exit(1);
});
