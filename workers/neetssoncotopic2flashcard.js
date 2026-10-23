"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

const TABLE = "neet_ss_oncology_pyt_source";
const OUTPUT_COL = "jsonb_output";
const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

const EXPECTED_CARD_COUNT = 20;
const MIN_ANSWER_WORDS = 3;
const MAX_ANSWER_WORDS = 6;

const BANNED_ANSWER_VERBS = new Set([
  "consider",
  "evaluate",
  "investigate",
  "assess",
  "verify",
  "monitor",
  "reassess",
  "ensure",
  "check",
  "rule",
  "arrange",
  "manage"
]);

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
  process.env.NEETSS_ONCOLOGY_FLASHCARD_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "NEETSS_ONCOLOGY_FLASHCARD_LIMIT",
  50,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "NEETSS_ONCOLOGY_FLASHCARD_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "NEETSS_ONCOLOGY_FLASHCARD_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "NEETSS_ONCOLOGY_FLASHCARD_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "NEETSS_ONCOLOGY_FLASHCARD_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = integerEnv(
  "NEETSS_ONCOLOGY_FLASHCARD_MAX_OUTPUT_TOKENS",
  16000,
  2000,
  50000
);

const WORKER_ID =
  process.env.NEETSS_ONCOLOGY_FLASHCARD_WORKER_ID ||
  `neetss-oncology-flashcard-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

/*
PASTE YOUR COMPLETE PROMPT INSIDE THE COMMENT BELOW.

Markdown, JSON examples, quotation marks and backticks are safe here.

IMPORTANT:
The pasted prompt must not contain the comment-closing characters:
* followed immediately by /
*/
const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
ROLE
You are a senior NEET SS Medical Oncology / DM Medical Oncology / ABIM Medical Oncology / ESMO / ASCO / NCCN-style clinical examiner. Create elite, specialist-level, evidence-aligned clinical-decision flashcards, not elementary oncology recall.

GOAL
Convert the supplied oncology topic and subtopics into EXACTLY 20 high-stakes flashcards requiring multi-step reasoning about what the treating oncologist should DO NEXT. Preserve every important supplied concept while projecting foundational molecular/pathology knowledge into genuine clinical decisions.

INPUT
Topic: {{TOPIC}}
Source/Notes: {{SOURCE}}
Disease setting (if supplied): {{DISEASE_SETTING}}
Guideline/reference date (if supplied): {{REFERENCE_DATE}}

SOURCE AND EVIDENCE RULES
- The source is the minimum content framework, not the difficulty ceiling. Cover its important distinct concepts; do not fabricate relevance to treatment when none exists.
- Enrich only with well-established principles from DeVita, WHO/ICC classifications, AJCC where applicable, NCCN/ESMO/ASCO and validated molecular pathology standards (ACMG/AMP, AMP/ASCO/CAP, ClinGen/CGC/VICC as relevant). Match recommendations to disease, stage, biomarker, line of therapy and the specified guideline date.
- Distinguish germline pathogenicity classification from somatic oncogenicity/actionability and drug eligibility. Do not treat a VUS as actionable; do not infer biallelic loss from a single variant, or pathogenicity from a hotspot alone.
- If a threshold, regimen, dose, approval, companion diagnostic, or biomarker-treatment link is not securely established, avoid inventing it. Use a defensible alternative clinical decision.
- If source concepts are mechanistic (e.g., oncogenes vs tumor suppressors), test clinical consequences: appropriate tissue/germline testing, confirmatory molecular assay, loss-of-heterozygosity interpretation, hereditary-risk referral, therapy eligibility, resistance assessment, trial selection, or avoiding an unjustified treatment. Never force a drug indication from a gene name alone.

CORE ARCHITECTURE: EVERY CARD
RAW DATA -> BIOLOGICAL/CLINICAL INTERPRETATION -> DISCRIMINATION BETWEEN AT LEAST TWO PLAUSIBLE MANAGEMENT PATHWAYS -> ONE PRECISE NEXT ACTION.
Recognition or naming a gene, syndrome, mutation class, diagnosis, or stage must NEVER be sufficient to answer.

1. THREE- AND FOUR-STEP REASONING
At least 14/20 cards must require >=3 linked reasoning steps; at least 6/20 require >=4. The correct action must depend on the interaction of multiple data points, not a lone buzzword.

2. TWO-LOCK MINIMUM
Every stem must contain >=2 INDEPENDENT action-changing variables, preferably 3: histology + stage + performance status; somatic variant + variant allele fraction + matched normal result; line of therapy + prior exposure + progression pattern; germline finding + family history + pathogenicity classification; organ function + toxicity grade + timing; response imaging + clinical status + molecular residual disease. No decorative variables.

3. COMPETING PATHWAYS AND VARIABLE FLIP
At least two actions must initially be plausible. One decisive discriminator selects the best action. For EVERY card, silently identify ONE variable that, if changed, would flip management. Do not explicitly reveal alternatives or the flip in the question.

4. RAW FINDINGS, NOT INTERPRETIVE LABELS
Use actual histology, stage-defining findings, mutation notation where relevant, variant allele fraction, tumor purity, copy-number changes, LOH, IHC pattern, tumor-normal concordance, imaging measurements, treatment dates, blood counts, renal/hepatic values, symptoms, response trajectory, and prior regimens. Do not call a variant 'actionable,' a tumor 'refractory,' a patient 'unfit,' or toxicity 'severe' when raw findings should establish it.

5. ONCOLOGY-SPECIFIC DECISION DOMAINS
Distribute across relevant source-driven domains:
- Oncogene gain-of-function vs tumor-suppressor loss-of-function and therapeutic consequences.
- Dominant activating hotspots vs truncating/splice/deletion variants, and functional/assay limitations.
- Monoallelic vs biallelic loss, copy-neutral LOH, deletion plus mutation, promoter silencing, and hereditary second hits.
- Somatic vs germline origin, tumor-only vs paired tumor-normal sequencing, clonal hematopoiesis confounding and referral for confirmatory germline testing.
- Pathogenic/likely pathogenic/VUS/benign classification, somatic oncogenicity, assay validation and actionability tiers.
- Histology-specific biomarker use, tissue-agnostic indications ONLY where established, and resistance alterations.
- Stage, resectability, intent of treatment, prior lines, performance status, comorbidities, organ function, treatment response and toxicity when genuinely pertinent.
- Surveillance and cascade testing to prevent irreversible morbidity when hereditary risk is confirmed.
Avoid substituting generic cytotoxic chemotherapy scenarios for the supplied molecular topic.

6. THRESHOLD-PAIR RULE
When a recognized threshold changes action, use near-boundary values and at least one paired decision concept across the deck (e.g., continue vs hold, standard vs intensified treatment, local vs systemic therapy, somatic-only vs germline follow-up). Use exact cutoffs ONLY when validated for the specific disease, assay, guideline and clinical setting. Do not invent VAF thresholds for germline status or universal tumor mutational burden cutoffs.

7. POST-TREATMENT / RESCUE DOMINANCE
At least 7/20 vignettes must BEGIN after an appropriate intervention has already occurred (systemic therapy, surgery, radiation, targeted therapy, molecular testing, genetic counseling, or surveillance). Test progression, acquired resistance, discordant molecular results, toxicity, recurrence, unexpected pathology, treatment ineligibility, or prevention of repeat harm. The task must ask for the tactical next step, not merely identify the cause.

8. COMPLETED TEST -> ACTION
At least 4/20 cards must present completed molecular/pathology/imaging tests and ask what to DO with the results. Do not ask for another test when a clinically indicated treatment, withholding of inappropriate therapy, referral, or safety action should follow immediately.

9. CLASSIFICATION -> ACTION
When WHO/ICC, AJCC, molecular-risk, variant pathogenicity, or somatic oncogenicity classification determines management, provide defining evidence and ask for the resulting action. Never ask only for a classification label.

10. SEQUENCING AND URGENCIES
Respect prior tests and treatments. In oncologic emergencies (e.g., spinal cord compression, febrile neutropenia, tumor lysis, hypercalcemia, immune toxicity), prioritize the time-critical first action over confirmatory workup when applicable. Do not create unrelated emergencies merely to inflate difficulty.

11. PHARMACOLOGY PRECISION
Specify drug or drug class and route if it changes the decision. Test initiation, hold, withdrawal, switch, sequencing, resistance-guided therapy, toxicity rescue, or contraindication. Give a dose only when standardized, authoritative, and critical to the question; avoid unsupported dosing or off-label assertions.

12. BIOMARKER CAUSALITY LOCK
A biomarker alone must not automatically dictate treatment. Require at least one other qualifying determinant: histology, stage, line, assay method, pathogenicity, biallelic status, prior exposure, regulatory/guideline eligibility, or organ function. Distinguish prognostic, predictive, diagnostic, hereditary and nonactionable findings.

13. COMPLICATION AND HEREDITARY-RISK PREVENTION
At least 3/20 cards must require a specific intervention preventing irreversible morbidity, including at least 2 long-term prevention decisions when relevant (e.g., syndrome-specific surveillance, risk-reducing intervention, cascade testing, avoidance of harmful targeted therapy). Generic 'counsel patient' is insufficient.

14. NO ANSWER LEAKAGE
Do not state in the stem the inference that makes the answer obvious. Avoid 'refractory,' 'actionable mutation,' 'biallelic inactivation,' 'pathogenic germline syndrome,' 'treatment failure,' or 'contraindicated' if these are the conclusions to derive. Avoid presenting explicit competing actions in the question.

15. TACTICAL ANSWERS: EXACTLY 3-6 WORDS
Each answer must be an executable ACTION VERB + TARGET + DECISIVE TECHNICAL MODIFIER, with 3-6 space-separated words. Examples of answer shape only: 'Refer for confirmatory germline testing'; 'Hold immunotherapy for suspected pneumonitis'; 'Initiate syndrome-specific colonoscopic surveillance'. Do not copy examples without scenario justification.
BANNED answer verbs: consider, evaluate, investigate, assess, verify, monitor, reassess, ensure, check, rule out, arrange, manage.
Prefer: initiate, administer, stop, hold, switch, repeat, biopsy, resect, irradiate, refer, obtain, confirm, continue, withhold, start, discontinue, replace, perform, offer.
Never use vague answers like 'Optimize oncology treatment,' 'Order further workup,' or 'Manage according to guidelines.'

16. NON-DUPLICATION
Every card tests a DIFFERENT clinical decision boundary. Similar biomarkers are allowed only if the decisive variable flips the action. Avoid 20 near-identical genetic referral vignettes.

17. DIFFICULTY AND TARGET MIX
Maximum 2/20 cards primarily diagnostic/staging, and even these must end in an action. At least 18/20 focus on management, selection, treatment extent, sequencing, rescue, complication response, or prevention.
Target primary distribution (sum = 20):
- 2 diagnostic/risk/classification -> action
- 3 treatment indication/initiation boundaries
- 4 selection/intensity/extent of treatment
- 4 completed result -> action / pharmacologic adjustment
- 5 treatment failure/bailout/rescue/complication decisions
- 2 long-term complication/hereditary-risk prevention
Categories may overlap for secondary attributes, but assign one primary category silently to each card.

18. TOPIC COVERAGE
Before writing, silently map ALL provided subtopics to cards and ensure none is omitted. For the supplied example 'Oncogenes vs Tumor Suppressors', cover oncogene activation/two-hit suppressor loss; gain vs loss of function; LOH/biallelic loss; hotspot vs truncating mutations; germline vs somatic; and clinical pathogenic-variant classification. Spread concepts throughout the deck rather than grouping trivial recall cards.

ANTI-EASY-CARD REJECTION
Rewrite any card if one buzzword yields the answer; recognition alone suffices; only one action is plausible before the discriminator; the answer is generic; raw data are replaced by interpretation; age, lab, imaging, or mutation details are decorative; the decision repeats; a candidate can answer without using >=2 stem variables; a mechanistic fact is asked without a clinical consequence; a variant of uncertain significance is treated as actionable; or the answer depends on an unverified guideline/approval.

SILENT AMBOSS 3-STEP STRESS TEST FOR EACH CARD
A. Identify reasoning step 1.
B. Identify reasoning step 2.
C. Identify reasoning step 3.
D. For >=6 cards, identify step 4.
E. Identify a genuinely plausible competing action.
F. Identify the precise stem variable that defeats it.
G. Identify one variable flip that changes the action.
If any part fails, rewrite silently.

SILENT FINAL AUDIT
- Exactly 20 distinct cards.
- >=14 cards with >=3 reasoning steps; >=6 with >=4.
- >=7 begin after an attempted intervention.
- >=4 completed investigation -> immediate action.
- >=3 specific complication-prevention decisions; >=2 long-term prevention.
- Every stem >=2 independent management-changing variables.
- Every card has plausible competing pathways and a variable flip.
- Important source subtopics all represented.
- No unsubstantiated biomarker-drug inference, thresholds, dosing, or approvals.
- Exactly 3-6 words in EVERY answer, counted individually.
- No banned verbs, generic answers, or duplicate boundaries.
- No diagnosis-only cards; no answer leakage.
Silently revise until all conditions hold.

OUTPUT
Return ONLY a valid JSON object, with no Markdown, commentary, headings, citations outside JSON, or code fences:
{
  "topic": "Exact topic",
  "cards": [
    {
      "question": "High-stakes oncology clinical decision vignette ending with a precise next-action question",
      "answer": "Precise 3-6 word tactical action"
    }
  ]
}
  */ };

  const source = promptContainer.toString();
  const start = source.indexOf("/*") + 2;
  const end = source.lastIndexOf("*/");

  return source.slice(start, end).trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE NEET SS MEDICAL ONCOLOGY FLASHCARD PROMPT HERE"
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

function serializeSubtopic(value) {
  if (value === null || value === undefined) {
    throw new Error("Subtopic/source is missing");
  }

  if (typeof value === "string") {
    const text = value.trim();

    if (!text) {
      throw new Error("Subtopic/source is empty");
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
    "SOURCE / SUBTOPICS:",
    serializeSubtopic(row.subtopic),
    "",
    `Create exactly ${EXPECTED_CARD_COUNT} clinical-decision flashcards.`,
    "Use the supplied topic and subtopics as the content framework.",
    "Each answer must contain exactly 3–6 space-separated words.",
    "Return only the required valid JSON object.",
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

function countWords(value) {
  return String(value)
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .length;
}

function getFirstWord(value) {
  return String(value)
    .trim()
    .split(/\s+/)[0]
    .toLowerCase()
    .replace(/[^a-z-]/g, "");
}

function validateFlashcards(value, expectedTopic) {
  validateExactKeys(
    value,
    ["topic", "cards"],
    "Flashcard output"
  );

  const outputTopic = requiredText(
    value.topic,
    "Output topic"
  );

  if (
    normalizeText(outputTopic) !==
    normalizeText(expectedTopic)
  ) {
    throw new Error(
      `Output topic mismatch. Expected "${expectedTopic}", received "${outputTopic}"`
    );
  }

  if (!Array.isArray(value.cards)) {
    throw new Error('"cards" must be an array');
  }

  if (value.cards.length !== EXPECTED_CARD_COUNT) {
    throw new Error(
      `Expected exactly ${EXPECTED_CARD_COUNT} cards, received ${value.cards.length}`
    );
  }

  const questions = new Set();
  const answers = new Set();

  value.cards.forEach((card, index) => {
    const cardNumber = index + 1;

    validateExactKeys(
      card,
      ["question", "answer"],
      `Card ${cardNumber}`
    );

    const question = requiredText(
      card.question,
      `Card ${cardNumber} question`,
      30
    );

    const answer = requiredText(
      card.answer,
      `Card ${cardNumber} answer`
    );

    if (!question.endsWith("?")) {
      throw new Error(
        `Card ${cardNumber} question must end with a question mark`
      );
    }

    const answerWords = countWords(answer);

    if (
      answerWords < MIN_ANSWER_WORDS ||
      answerWords > MAX_ANSWER_WORDS
    ) {
      throw new Error(
        `Card ${cardNumber} answer contains ${answerWords} words; required range is ${MIN_ANSWER_WORDS}–${MAX_ANSWER_WORDS}`
      );
    }

    const firstWord = getFirstWord(answer);

    if (BANNED_ANSWER_VERBS.has(firstWord)) {
      throw new Error(
        `Card ${cardNumber} begins with banned answer verb "${firstWord}"`
      );
    }

    const normalizedQuestion = normalizeText(question);
    const normalizedAnswer = normalizeText(answer);

    if (questions.has(normalizedQuestion)) {
      throw new Error(
        `Card ${cardNumber} duplicates another question`
      );
    }

    if (answers.has(normalizedAnswer)) {
      throw new Error(
        `Card ${cardNumber} duplicates another answer`
      );
    }

    questions.add(normalizedQuestion);
    answers.add(normalizedAnswer);

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

      const validated = validateFlashcards(
        parsed,
        requiredText(row.topic, "Topic")
      );

      return {
        json: validated,
        cards: validated.cards.length,
        characters: rawText.length
      };
    } catch (error) {
      lastError = error;

      if (isCreditError(error)) {
        throw error;
      }

      const validationFailure =
        /empty output|JSON|Flashcard|cards|Card|question|answer|topic|word|banned|duplicate|Subtopic|exactly/i.test(
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
    new Error(
      "NEET SS Medical Oncology flashcard generation failed"
    )
  );
}

async function verifySchema() {
  const { error } = await supabase
    .from(TABLE)
    .select(
      [
        "id",
        "topic",
        "subtopic",
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
    Date.now() - LOCK_TTL_MIN * 60 * 1000
  ).toISOString();

  const { error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null
    })
    .eq(LOCK_COL, true)
    .not("topic", "is", null)
    .not("subtopic", "is", null)
    .is(OUTPUT_COL, null)
    .lt(LOCK_AT_COL, cutoff);

  if (error) {
    throw new Error(
      `Failed to release expired locks: ${error.message}`
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
    .not("topic", "is", null)
    .not("subtopic", "is", null)
    .is(OUTPUT_COL, null)
    .select(
      [
        "id",
        "subject",
        "serial_number",
        "topic",
        "subtopic",
        "number_of_times_asked",
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
    .select("id,serial_number")
    .eq(LOCK_COL, false)
    .not("topic", "is", null)
    .not("subtopic", "is", null)
    .is(OUTPUT_COL, null)
    .order("serial_number", {
      ascending: true
    })
    .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending oncology rows: ${error.message}`
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
      `Failed to save oncology flashcards: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or jsonb_output already exists"
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
      `Failed to release lock ${row.id}: ${error.message}`
    );
  }
}

async function processRow(row) {
  console.log(
    `Generating oncology flashcards | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result = await generateFlashcards(row);

    await saveSuccess(row, result.json);

    console.log(
      `Completed | ${row.subject} | ${row.serial_number} | ${row.topic} | cards=${result.cards} | characters=${result.characters}`
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
      `Failed | ${row.subject} | ${row.serial_number} | ${row.topic}: ${errorText(error)}`
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
    `NEET SS MEDICAL ONCOLOGY TOPIC -> FLASHCARDS WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `Model=${MODEL} | Table=${TABLE} | Input=topic+subtopic | Output=${OUTPUT_COL} | Cards=${EXPECTED_CARD_COUNT} | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY} | MaxOutputTokens=${MAX_OUTPUT_TOKENS}`
  );

  while (true) {
    try {
      const rows = await claimRows(PICKUP_LIMIT);

      if (!rows.length) {
        await sleep(LOOP_SLEEP_MS);
        continue;
      }

      console.log(
        `Claimed ${rows.length} pending oncology row(s)`
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
    "Fatal oncology flashcard worker error:",
    error
  );

  process.exit(1);
});
