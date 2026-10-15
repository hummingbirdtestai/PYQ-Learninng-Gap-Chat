require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

const TABLE = "usmle_step1_topic_grouped";

const INPUT_COL = "notes_json";
const OUTPUT_COL = "mcq_json";

const LOCK_COL = "mcq_generation_lock";
const LOCK_AT_COL = "mcq_generation_locked_at";

function integerEnv(name, fallback, min, max) {
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
  process.env.USMLE_STEP1_MCQ_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "USMLE_STEP1_MCQ_LIMIT",
  50,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "USMLE_STEP1_MCQ_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "USMLE_STEP1_MCQ_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "USMLE_STEP1_MCQ_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "USMLE_STEP1_MCQ_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = integerEnv(
  "USMLE_STEP1_MCQ_MAX_OUTPUT_TOKENS",
  16000,
  2000,
  50000
);

const WORKER_ID =
  process.env.USMLE_STEP1_MCQ_WORKER_ID ||
  `usmle-step1-mcq-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

/*
PASTE THE COMPLETE PROMPT INSIDE THE COMMENT BELOW.

Backticks, Markdown, JSON examples and quotation marks are safe here.

IMPORTANT:
Do not place the characters * followed immediately by /
inside the prompt because that closes the JavaScript comment.
*/
const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
ROLE
You are a senior USMLE Step 1 item writer and rigorous medical editor, creating ORIGINAL questions at the clinical reasoning depth of high-quality NBME-style, AMBOSS, UWorld, and USMLE-Rx practice. Do not copy proprietary questions. Your task is to transform the supplied high-yield NOTES for ONE topic into EXACTLY FIVE high-quality, single-best-answer MCQs with FIVE options (A–E) each.

SOURCE AND SCOPE
- Treat the supplied NOTES as the sole coverage map: identify its testable concepts and use them faithfully. Do not invent claims or numerical thresholds. You may apply established Step 1 foundational science to construct medically valid clinical scenarios and explain causal mechanisms; never misrepresent unsupported additions as being in the NOTES.
- Cover five DISTINCT, important, supported learning objectives where available. Avoid duplicate testing of one association. If notes are narrow, vary the inference or mechanism without drifting outside the topic.
- If source notes are internally inconsistent, ambiguous, or clinically wrong, do not silently perpetuate errors: avoid the disputed point and use an undisputed supported concept; if five accurate questions cannot be produced, return the defined error JSON rather than fabricate.
- Step 1 priorities: physiology, pathophysiology, biochemistry, genetics, microbiology, immunology, pathology, pharmacology, anatomy, embryology, biostatistics/epidemiology and behavioral science, all linked to patient findings. Do NOT turn the set into Step 2/Step 3 treatment-selection or guideline memorization.

MANDATORY QUESTION DESIGN
1. EXACTLY FIVE MCQs per input topic, each with EXACTLY FIVE options A, B, C, D, E. One unequivocally best answer.
2. Aim for a compact, information-dense clinical/laboratory/experimental vignette of 45–85 words (extend only if needed for a table or multistage experiment). Neutral lead-in. No giveaway diagnosis, enzyme, gene, drug, organism, receptor or pathway when that is the target of the question.
3. Every item must require THREE LINKED INFERENCES:
   STEP 1: interpret raw clinical, histologic, laboratory, pharmacologic, genetic or experimental data to infer an underlying disease/process;
   STEP 2: integrate a DISCRIMINATING modifier (timing, tissue specificity, inheritance, organelle, receptor signaling, feedback, enzyme kinetics, drug exposure, compensatory response, lab pattern, mutation consequence, or test condition);
   STEP 3: predict the mechanism, physiologic change, downstream metabolite, response to intervention, cellular consequence, diagnostic interpretation, or experimental outcome.
   If recognizing a diagnosis or recalling one association directly gives the answer, REWRITE.
4. AFTER the disease/process is recognized, at least TWO options must remain plausible until the modifier is used. Prefer counterfactual alternatives that differ in ONE mechanistic dimension.
5. Vary cognitive tasks across the five: e.g., perturbation/feedback prediction; distinguish adjacent mechanisms; drug-target and downstream effect; molecular/genetic mechanism to phenotype; experiment or laboratory pattern interpretation. Match to actual note coverage; do not force an irrelevant category.
6. Include patient demographics, exposures, family history, labs, histology or experiments only if they discriminate answers. Include units and appropriate reference ranges when necessary. Maintain internally consistent physiology. Do not invent specific cutoffs, sensitivities, doses or recommendations.
7. One plausible red herring may be included if it creates legitimate competition; otherwise none. Avoid gratuitous rarity and obscure trivia. Difficulty comes from integrating concepts, not from withholding necessary information.
8. Options: EXACTLY FIVE, typically 2–8 words each; parallel grammar, similar length/specificity, same conceptual category, mutually distinct, medically plausible. Avoid 'all/none of the above', joke distractors, overlapping choices, and clues based on option length.
9. Do not include a second defensible correct answer. For any clinical action item, the question must remain within Step 1 mechanisms, pharmacodynamics, or physiologic consequences rather than detailed management algorithms.
10. Randomize the correct-answer positions without predictable cycles. Never sacrifice question validity to force a distribution.

USMLE STEP 1 EXEMPLARS — DESIGN PATTERNS (DO NOT COPY THESE AS OUTPUT)
EXAMPLE 1 — ENDOCRINE FEEDBACK / RECEPTOR (illustrative)
Stem: A 29-year-old woman has episodic palpitations and weight loss. Free thyroxine is elevated, yet TSH is not suppressed. MRI shows a pituitary mass. After administration of a somatostatin analog, TSH decreases. Which change best explains the resulting fall in thyroid hormone synthesis?
A: Reduced thyroidal cAMP signaling
B: Increased thyroidal cAMP signaling
C: Increased hypothalamic TRH release
D: Reduced thyroidal iodide excretion
E: Increased peripheral T4 conversion
Correct Answer: A
Three steps: infer inappropriate TSH secretion -> recognize intact TSH-receptor Gs signaling as the relevant modifier -> predict decreased cAMP when TSH stimulation falls. This is a STRUCTURAL illustration, not a template to repeat.

EXAMPLE 2 — METABOLIC PATHWAY / TISSUE-SPECIFIC EFFECT (illustrative)
Stem: A fasting child develops lethargy and hepatomegaly after a viral illness. Laboratory studies show hypoglycemia, elevated lactate, and hyperuricemia. Liver biopsy demonstrates excess glycogen with normal branching. After glucagon administration, plasma glucose rises minimally. Which hepatic biochemical process is most directly impaired?
A: Glycogen branching
B: Glucose-6-phosphate hydrolysis
C: Fructose-1-phosphate cleavage
D: Mitochondrial beta-oxidation
E: Glycogen phosphorylase activation
Correct Answer: B
Three steps: identify fasting hypoglycemia with glycogen accumulation -> interpret the glucagon nonresponse and lactate pattern -> localize impaired free-glucose release. Ensure any actual generated question goes beyond a one-to-one syndrome/enzyme association by testing an additional perturbation or downstream consequence.

EXAMPLE 3 — PHARMACOLOGY / COMPETITIVE MECHANISM (illustrative)
Stem: A research volunteer receives a reversible competitive antagonist of a receptor mediating smooth-muscle contraction. An agonist concentration-response curve is measured before and after the antagonist. The maximal effect is unchanged, but a higher agonist concentration is required for half-maximal response. Which change in the agonist curve is expected?
A: Lower EC50 without Emax change
B: Higher EC50 without Emax change
C: Lower Emax without EC50 change
D: Higher Emax with lower EC50
E: Irreversible reduction in receptor number
Correct Answer: B
Three steps: interpret dose-response data -> distinguish reversible competitive from noncompetitive antagonism -> predict a rightward shift with preserved efficacy. In real outputs, include an additional meaningful discriminating modifier when necessary to ensure true three-step depth.

QUESTION EXPLANATION — REQUIRED FOR EACH ITEM
- 'Correct Answer Summary': explicitly state option letter/text and causal reason.
- 'Diagnostic Pathway': EXACTLY THREE sequential, case-specific strings: (1) evidence -> inferred process, (2) modifier -> narrowed mechanism, (3) mechanism -> selected option.
- 'Why the Other Options Fail': EXACTLY FOUR keys, the four incorrect letters; each value names the actual corresponding option's concept and explains specifically why it fails in THIS vignette. No generic dismissals.
- 'Examiner\'s Trap': one high-level, case-specific misconception distinguishing the strongest distractor from the correct answer.
- 'Learning Objective': one crisp, mechanistic Step 1 takeaway (not a restatement of diagnosis).
- Do not include the correct answer in the stem or give away the answer via explanations of unrelated questions. Every explanation must be self-contained and clinically/scientifically accurate.

FINAL SILENT 10/10 EDITORIAL AUDIT — REWRITE UNTIL ALL PASS
[ ] Exactly five questions and exactly five options per question.
[ ] Vignette is clear, internally consistent, information-dense and Step 1-appropriate.
[ ] Each item requires three meaningful linked inferences, not simple recall.
[ ] At least two options remain credible after recognizing the condition.
[ ] A specific second clue separates the best answer from the strongest distractor.
[ ] Single unambiguous correct answer; no unsupported assumption.
[ ] Each distractor is parallel, realistic and conceptually competitive.
[ ] Mechanism and downstream consequence are physiologically correct.
[ ] No invented numerical cutoff, genetic fact, or pharmacologic effect.
[ ] Five learning objectives are distinct and covered by supplied notes.
[ ] Correct-answer letter matches the actual option text.
[ ] All four incorrect keys and rationales match the actual options.
[ ] No cross-question explanation contamination, placeholders, or boilerplate.
[ ] Examiner's Trap identifies the precise attractive misconception.
[ ] Output is strictly machine-parseable JSON, with no markdown or commentary.

STRICT JSON OUTPUT CONTRACT
Return ONLY a JSON object in this EXACT shape (same casing and keys):
{
  "mcqs": [
    {
      "Stem": "...",
      "A": "...",
      "B": "...",
      "C": "...",
      "D": "...",
      "E": "...",
      "Correct Answer": "A",
      "Explanation": {
        "Correct Answer Summary": "...",
        "Diagnostic Pathway": ["...", "...", "..."],
        "Why the Other Options Fail": {
          "B": "...",
          "C": "...",
          "D": "...",
          "E": "..."
        },
        "Examiner's Trap": "...",
        "Learning Objective": "..."
      }
    }
  ]
}
- The example shows ONE object solely to illustrate schema. In successful output the mcqs array MUST contain EXACTLY FIVE objects.
- Correct Answer must be one of A/B/C/D/E, and the wrong-option object must have EXACTLY the other four keys, with no correct-answer key.
- All values are JSON strings except arrays/objects. Escape quotation marks and newlines properly. No trailing commas, no code fences, no extra keys, no preamble.
- If notes are missing, wholly unreadable, or cannot support five medically accurate distinct items, output ONLY: {"mcqs":[],"error":"Insufficient reliable source notes for five distinct Step 1 questions"}. Never pad with invented items.

INPUT NOTES:
{{NOTES}}
  */ };

  const source = promptContainer.toString();
  const start = source.indexOf("/*") + 2;
  const end = source.lastIndexOf("*/");

  return source.slice(start, end).trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE USMLE STEP 1 MCQ PROMPT HERE"
  )
) {
  throw new Error(
    "Paste the complete inline SYSTEM_PROMPT before starting the worker"
  );
}

const sleep = (milliseconds) =>
  new Promise((resolve) =>
    setTimeout(resolve, milliseconds)
  );

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
    throw new Error(
      `${label} must be a non-empty string`
    );
  }

  return text;
}

function serializeNotes(value) {
  if (
    value === null ||
    value === undefined
  ) {
    throw new Error(
      "notes_json is missing"
    );
  }

  if (typeof value === "string") {
    const text = value.trim();

    if (!text) {
      throw new Error(
        "notes_json is empty"
      );
    }

    try {
      return JSON.stringify(
        JSON.parse(text),
        null,
        2
      );
    } catch {
      return text;
    }
  }

  return JSON.stringify(
    value,
    null,
    2
  );
}

function buildInput(row) {
  const topic = requiredText(
    row.topic,
    "Topic"
  );

  const subject = requiredText(
    row.subject,
    "Subject"
  );

  const notes = serializeNotes(
    row[INPUT_COL]
  );

  return [
    `TOPIC: ${topic}`,
    `SUBJECT: ${subject}`,
    `TOPIC SERIAL NUMBER: ${
      row.topic_subjectwise_serial_number ??
      "Not provided"
    }`,
    `SOURCE ROW COUNT: ${
      row.source_row_count ??
      "Not provided"
    }`,
    "",
    "INPUT NOTES:",
    notes,
    "",
    "Generate exactly five original USMLE Step 1 MCQs from these notes.",
    "Each MCQ must contain exactly five options: A, B, C, D and E.",
    "Return only one valid JSON object matching the required schema.",
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
    throw new Error(
      "OpenAI returned empty output"
    );
  }

  return text;
}

function removeCodeFence(text) {
  let cleaned = String(text ?? "").trim();

  const fencedMatch = cleaned.match(
    /^```(?:json)?\s*([\s\S]*?)\s*```$/i
  );

  if (fencedMatch) {
    cleaned = fencedMatch[1].trim();
  }

  return cleaned;
}

function extractJsonObject(text) {
  const cleaned = removeCodeFence(text);

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
        "Model output does not contain a JSON object"
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

function validateString(
  value,
  label,
  minimumLength = 1
) {
  if (
    typeof value !== "string" ||
    value.trim().length < minimumLength
  ) {
    throw new Error(
      `${label} must be a valid non-empty string`
    );
  }

  return value.trim();
}

function optionWordCount(value) {
  return String(value)
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .length;
}

function validateMcqSet(value) {
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value)
  ) {
    throw new Error(
      "Output must be one JSON object"
    );
  }

  if (
    typeof value.error === "string" &&
    Array.isArray(value.mcqs) &&
    value.mcqs.length === 0
  ) {
    throw new Error(
      `Model reported insufficient source notes: ${value.error}`
    );
  }

  const topLevelKeys = Object.keys(value);

  if (
    topLevelKeys.length !== 1 ||
    topLevelKeys[0] !== "mcqs"
  ) {
    throw new Error(
      'Successful output must contain only the top-level key "mcqs"'
    );
  }

  if (!Array.isArray(value.mcqs)) {
    throw new Error(
      '"mcqs" must be an array'
    );
  }

  if (value.mcqs.length !== 5) {
    throw new Error(
      `Expected exactly 5 MCQs, received ${value.mcqs.length}`
    );
  }

  const letters = [
    "A",
    "B",
    "C",
    "D",
    "E"
  ];

  const requiredQuestionKeys = [
    "Stem",
    "A",
    "B",
    "C",
    "D",
    "E",
    "Correct Answer",
    "Explanation"
  ];

  const requiredExplanationKeys = [
    "Correct Answer Summary",
    "Diagnostic Pathway",
    "Why the Other Options Fail",
    "Examiner's Trap",
    "Learning Objective"
  ];

  value.mcqs.forEach(
    (mcq, index) => {
      const number = index + 1;

      if (
        !mcq ||
        typeof mcq !== "object" ||
        Array.isArray(mcq)
      ) {
        throw new Error(
          `MCQ ${number} must be an object`
        );
      }

      const questionKeys =
        Object.keys(mcq);

      for (
        const key of requiredQuestionKeys
      ) {
        if (!questionKeys.includes(key)) {
          throw new Error(
            `MCQ ${number} is missing "${key}"`
          );
        }
      }

      const unexpectedQuestionKeys =
        questionKeys.filter(
          (key) =>
            !requiredQuestionKeys.includes(key)
        );

      if (
        unexpectedQuestionKeys.length > 0
      ) {
        throw new Error(
          `MCQ ${number} contains unexpected keys: ${unexpectedQuestionKeys.join(", ")}`
        );
      }

      validateString(
        mcq.Stem,
        `MCQ ${number} Stem`,
        20
      );

      const normalizedOptions = [];

      for (const letter of letters) {
        const option = validateString(
          mcq[letter],
          `MCQ ${number} option ${letter}`
        );

        const words =
          optionWordCount(option);

        if (words > 8) {
          throw new Error(
            `MCQ ${number} option ${letter} contains ${words} words; maximum is 8`
          );
        }

        normalizedOptions.push(
          option.toLowerCase()
        );
      }

      if (
        new Set(normalizedOptions).size !== 5
      ) {
        throw new Error(
          `MCQ ${number} contains duplicate options`
        );
      }

      const correctAnswer =
        validateString(
          mcq["Correct Answer"],
          `MCQ ${number} Correct Answer`
        ).toUpperCase();

      if (!letters.includes(correctAnswer)) {
        throw new Error(
          `MCQ ${number} Correct Answer must be A, B, C, D or E`
        );
      }

      mcq["Correct Answer"] =
        correctAnswer;

      const explanation =
        mcq.Explanation;

      if (
        !explanation ||
        typeof explanation !== "object" ||
        Array.isArray(explanation)
      ) {
        throw new Error(
          `MCQ ${number} Explanation must be an object`
        );
      }

      const explanationKeys =
        Object.keys(explanation);

      for (
        const key of requiredExplanationKeys
      ) {
        if (
          !explanationKeys.includes(key)
        ) {
          throw new Error(
            `MCQ ${number} Explanation is missing "${key}"`
          );
        }
      }

      const unexpectedExplanationKeys =
        explanationKeys.filter(
          (key) =>
            !requiredExplanationKeys.includes(
              key
            )
        );

      if (
        unexpectedExplanationKeys.length > 0
      ) {
        throw new Error(
          `MCQ ${number} Explanation contains unexpected keys: ${unexpectedExplanationKeys.join(", ")}`
        );
      }

      validateString(
        explanation[
          "Correct Answer Summary"
        ],
        `MCQ ${number} Correct Answer Summary`,
        10
      );

      validateString(
        explanation["Examiner's Trap"],
        `MCQ ${number} Examiner's Trap`,
        10
      );

      validateString(
        explanation[
          "Learning Objective"
        ],
        `MCQ ${number} Learning Objective`,
        10
      );

      const pathway =
        explanation[
          "Diagnostic Pathway"
        ];

      if (
        !Array.isArray(pathway) ||
        pathway.length !== 3
      ) {
        throw new Error(
          `MCQ ${number} Diagnostic Pathway must contain exactly 3 steps`
        );
      }

      pathway.forEach(
        (step, stepIndex) => {
          validateString(
            step,
            `MCQ ${number} Diagnostic Pathway step ${
              stepIndex + 1
            }`,
            10
          );
        }
      );

      const failures =
        explanation[
          "Why the Other Options Fail"
        ];

      if (
        !failures ||
        typeof failures !== "object" ||
        Array.isArray(failures)
      ) {
        throw new Error(
          `MCQ ${number} Why the Other Options Fail must be an object`
        );
      }

      const expectedIncorrectLetters =
        letters.filter(
          (letter) =>
            letter !== correctAnswer
        );

      const actualFailureKeys =
        Object.keys(failures).sort();

      const expectedFailureKeys =
        [...expectedIncorrectLetters].sort();

      if (
        JSON.stringify(
          actualFailureKeys
        ) !==
        JSON.stringify(
          expectedFailureKeys
        )
      ) {
        throw new Error(
          `MCQ ${number} wrong-option keys must be exactly: ${expectedFailureKeys.join(", ")}`
        );
      }

      for (
        const letter of expectedIncorrectLetters
      ) {
        validateString(
          failures[letter],
          `MCQ ${number} explanation for option ${letter}`,
          10
        );
      }
    }
  );

  return value;
}

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
          instructions: SYSTEM_PROMPT,
          input: buildInput(row),
          max_output_tokens:
            MAX_OUTPUT_TOKENS
        });

      const rawText =
        extractText(response);

      const parsed =
        extractJsonObject(rawText);

      const validated =
        validateMcqSet(parsed);

      return {
        json: validated,
        characters: rawText.length
      };
    } catch (error) {
      lastError = error;

      if (isCreditError(error)) {
        throw error;
      }

      const validationFailure =
        /empty output|JSON|MCQ|mcqs|Stem|option|Correct Answer|Explanation|Diagnostic Pathway|Other Options|Examiner|Learning Objective|source notes|notes_json|unexpected keys|duplicate/i.test(
          errorText(error)
        );

      if (
        attempt === API_RETRIES ||
        (!isRetryable(error) &&
          !validationFailure)
      ) {
        break;
      }

      const delay =
        1000 * 2 ** attempt +
        Math.floor(
          Math.random() * 400
        );

      console.warn(
        `Retry ${attempt + 1}/${API_RETRIES} after ${delay} ms: ${errorText(error)}`
      );

      await sleep(delay);
    }
  }

  throw (
    lastError ||
    new Error(
      "USMLE Step 1 MCQ generation failed"
    )
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
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .lt(LOCK_AT_COL, cutoff);

  if (error) {
    throw new Error(
      `Failed to release expired MCQ locks: ${error.message}`
    );
  }
}

async function lockRow(candidate) {
  const lockedAt =
    new Date().toISOString();

  const { data, error } =
    await supabase
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

  const { data, error } =
    await supabase
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
      .order(
        "topic_subjectwise_serial_number",
        {
          ascending: true,
          nullsFirst: false
        }
      )
      .order("topic", {
        ascending: true
      })
      .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending MCQ rows: ${error.message}`
    );
  }

  if (!data?.length) {
    return [];
  }

  const results =
    await Promise.allSettled(
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

    if (
      result.status === "rejected"
    ) {
      console.error(
        "Row-lock error:",
        errorText(result.reason)
      );
    }
  }

  return rows;
}

async function saveSuccess(
  row,
  mcqJson
) {
  const { data, error } =
    await supabase
      .from(TABLE)
      .update({
        [OUTPUT_COL]: mcqJson,
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
      `Failed to save MCQs: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or mcq_json already exists"
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
    .eq(
      LOCK_AT_COL,
      row[LOCK_AT_COL]
    )
    .is(OUTPUT_COL, null);

  if (error) {
    console.error(
      `Failed to release MCQ lock ${row.id}: ${error.message}`
    );
  }
}

async function processRow(row) {
  console.log(
    `Generating USMLE Step 1 MCQs | ${row.subject} | ${
      row.topic_subjectwise_serial_number ??
      "-"
    } | ${row.topic}`
  );

  try {
    const result =
      await generateMcqs(row);

    await saveSuccess(
      row,
      result.json
    );

    console.log(
      `Completed | ${row.subject} | ${
        row.topic_subjectwise_serial_number ??
        "-"
      } | ${row.topic} | mcqs=${result.json.mcqs.length} | characters=${result.characters}`
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
        row.topic_subjectwise_serial_number ??
        "-"
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

      const result =
        await processRow(
          rows[index]
        );

      if (
        result.creditExhausted
      ) {
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
      rows
        .slice(next)
        .map(releaseLock)
    );
  }

  return {
    creditExhausted
  };
}

async function main() {
  await verifySchema();

  console.log(
    `USMLE STEP 1 NOTES -> MCQ WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `Model=${MODEL} | Table=${TABLE} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | Lock=${LOCK_COL} | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY} | MaxOutputTokens=${MAX_OUTPUT_TOKENS}`
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
        `Claimed ${rows.length} pending USMLE Step 1 topic row(s)`
      );

      const result =
        await processBatch(rows);

      if (
        result.creditExhausted
      ) {
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
    "Fatal USMLE Step 1 MCQ worker error:",
    error
  );

  process.exit(1);
});
