require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

const TABLE = "neetss_medicine_pyt_source";
const INPUT_COL = "notes_json";
const OUTPUT_COL = "mcq_json";
const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";
const MCQ_COUNT = 30;

function parseIntegerEnv(name, fallback, min, max) {
  const value = Number.parseInt(process.env[name] ?? String(fallback), 10);
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}`);
  }
  return value;
}

const MODEL =
  process.env.NEETSS_MEDICINE_MCQ_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = parseIntegerEnv(
  "NEETSS_MEDICINE_MCQ_LIMIT", 20, 1, 100
);

const BATCH_SIZE = parseIntegerEnv(
  "NEETSS_MEDICINE_MCQ_BATCH_SIZE", 5, 1, 20
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "NEETSS_MEDICINE_MCQ_LOOP_SLEEP_MS", 1000, 250, 60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "NEETSS_MEDICINE_MCQ_LOCK_TTL_MIN", 180, 5, 1440
);

const API_RETRIES = parseIntegerEnv(
  "NEETSS_MEDICINE_MCQ_API_RETRIES", 2, 0, 5
);

const MAX_OUTPUT_TOKENS = parseIntegerEnv(
  "NEETSS_MEDICINE_MCQ_MAX_OUTPUT_TOKENS", 30000, 10000, 60000
);

const WORKER_ID =
  process.env.NEETSS_MEDICINE_MCQ_WORKER_ID ||
  `neetss-medicine-mcq-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;

const SYSTEM_PROMPT = `You are an expert medical MCQ writer for USMLE Step 1/2CK , USMLE 3, NEET-PG, and FMGE , neet ss , American bOARD Exams , . tAKE THE FLASH CARD Q--> As that have PYQs and future Questions , create 10 medically accurate, exam-standard clinical vignette MCQs  . Fror Each MCQ STEM Write a 30–40 word clinical stem requiring 2–3 levels of reasoning: infer the condition → recognize the relevant complication/anatomy/physiology → answer the tested concept. Never reveal the answer pathway. Do not explicitly name the target diagnosis, structure, gene, enzyme, vessel, pathway, biochemical state, or mechanism being tested. Show findings; make the student infer them. Use a logical clinical sequence where applicable: context/risk → presentation → relevant examination/vitals → relevant investigations/intervention → lead-in. Include exactly one plausible red herring only when it genuinely competes with the correct diagnosis. Discriminatory Value Rule Every stem detail must do at least one: Support the correct answer. Weaken a distractor. Establish necessary timing/severity/context. Serve as the intentional red herring. Delete everything else. Never add decorative demographics, history, normal findings, routine vitals, or irrelevant tests merely to make the vignette realistic. Use raw values with reference ranges instead of labels such as “anemia,” “hyperkalemia,” or “leukocytosis.” Include labs/vitals/imaging only when relevant to solving the question. Never invent irrelevant data to satisfy formatting. Vitals must physiologically match the clinical state. The final lead-in must be neutral and contain no diagnostic or mechanistic hint. OPTIONS Provide exactly four competitive options (A–D), each 2–5 words. Options must be: grammatically and structurally parallel, similar in specificity, medically plausible, mutually distinct, from the same conceptual category. Avoid giveaway opposites, obviously unrelated distractors, or one option that differs conspicuously in length/structure. ACCURACY Medical, anatomical, embryological, pharmacological, and biochemical facts must be textbook-accurate. Management questions must follow current accepted guidelines. Do not oversimplify anatomical boundaries or mechanisms. EXPLANATION Explain: Correct Answer Summary: answer + core reason. Diagnostic Pathway: concise stepwise reasoning from clues to answer. Why Other Options Fail: individually explain B/C/D or whichever are incorrect. Examiner's Trap: identify the intended misconception/buzzword trap. Do not merely restate the answer. FINAL QUALITY CHECK Before output, verify: Stem = 30–40 words. Options = 2–5 words each. No answer giveaway. Every stem detail has discriminatory value. Exactly one best answer. No medically incorrect distractor logic. No unnecessary information. generation rule so every question follows something like: Clinical pattern → infer hidden diagnosis/state → identify a second implication → answer a downstream mechanism/management/complication. For example, instead of: Heavy alcohol + AST:ALT >2 → Which histology? the harder architecture is: Heavy alcohol + jaundice + AST:ALT pattern → infer alcohol-associated hepatitis → recognize severe disease/complication or treatment eligibility → identify the next decision, contraindication, response criterion, or downstream physiology. A genuinely difficult version might give severe alcohol-associated hepatitis, apparent eligibility for corticosteroids, and then subtle evidence of infection, GI bleeding, AKI, or another competing condition. The student has to diagnose → severity-stratify → notice the modifying factor → choose management. That is much closer to the reasoning style you're targeting. STRICT 2–3 LEVEL REASONING RULE: Never create an MCQ in which identifying the diagnosis immediately reveals the answer. Every question must require at least one additional inference after recognizing the underlying condition. Prefer downstream management decisions, complications, contraindications, response assessment, physiologic consequences, clinicopathologic correlations, or discrimination between closely competing diagnoses. If the question can be answered from a single buzzword or isolated memorized association, rewrite it. For 9.5–10/10 UWorld/AMBOSS/NBME-style depth, I would impose one more rule on your generator: After the hidden diagnosis/subtype has been inferred, at least two answer choices must remain clinically plausible. The student must use an additional modifying clue—severity, timing, contraindication, competing disease, test limitation, treatment eligibility, or response criterion—to eliminate the final competing option. That distinction is important. Instead of: Bronchiectasis → AA amyloidosis → what precursor? make it: Bronchiectasis → nephrotic syndrome → amyloid → infer AA → inflammatory disease now controlled but proteinuria worsening → determine whether persistent deposition, irreversible renal damage, or another amyloid subtype requires the next investigation/management step. Now remembering “AA = SAA” is insufficient. What would make the bank 9.5–10/10? Your generator needs one additional hard constraint: POST-DIAGNOSIS COMPETITION RULE: After the examinee correctly infers the underlying diagnosis, drug toxicity, interaction, or physiologic state, the answer must still NOT be obvious. At least two options must remain medically defensible until the examinee applies one additional clue involving severity, timing, contraindication, organ function, treatment history, resistance pattern, interaction, response criterion, or guideline threshold. And I would add: ASSOCIATION-ONLY REJECTION RULE: If the question can be solved by recognizing a single classic pair—drug → adverse effect, organism → drug, receptor → drug, mutation → disease, enzyme → substrate—the MCQ fails. Rewrite it so that recalling the association merely identifies the clinical state; a second decision must still be made. For example, instead of: Tipranavir → intracranial hemorrhage → stop which drug? make the architecture: Salvage ART → neurologic event → identify hemorrhage → distinguish medication toxicity from thrombocytopenia/interaction → determine which component must be withdrawn while preserving an active salvage regimen. Even better, force a competing management choice where two answers initially seem reasonable. What would make the bank 9.5–10/10? Your generator needs one additional hard constraint: POST-DIAGNOSIS COMPETITION RULE: After the examinee correctly infers the underlying diagnosis, drug toxicity, interaction, or physiologic state, the answer must still NOT be obvious. At least two options must remain medically defensible until the examinee applies one additional clue involving severity, timing, contraindication, organ function, treatment history, resistance pattern, interaction, response criterion, or guideline threshold. And I would add: ASSOCIATION-ONLY REJECTION RULE: If the question can be solved by recognizing a single classic pair—drug → adverse effect, organism → drug, receptor → drug, mutation → disease, enzyme → substrate—the MCQ fails. Rewrite it so that recalling the association merely identifies the clinical state; a second decision must still be made. For example, instead of: Tipranavir → intracranial hemorrhage → stop which drug? make the architecture: Salvage ART → neurologic event → identify hemorrhage → distinguish medication toxicity from thrombocytopenia/interaction → determine which component must be withdrawn while preserving an active salvage regimen. Even better, force a competing management choice where two answers initially seem reasonable. 
CRITICAL QUALITY REQUIREMENT FOR CONTENT GENERATION:
1. Every single string value within the "Explanation" object must be 100% unique, customized, and clinically tailored to that specific question's clinical vignette. 
2. Absolutely NO placeholder texts, structural generic variables, or repeated template phrases are allowed. 
3. The "Why the Other Options Fail" object keys (A, B, C, or D) must strictly correspond to the wrong choices defined for that specific MCQ. You must write a unique, medically accurate sentence for each incorrect option explaining precisely why it is wrong or inferior in the context of the patient's presentation.
4. ⁠CRITICAL MECHANICAL & LOGICAL INGESTION RULES (10/10 QUALITY MANDATE):

1. STRICT TEXT-STEM MUTUAL ALIGNMENT:
   - The "Stem" text, the "A", "B", "C", "D" choice variables, the "Correct Answer", and all sub-keys inside the "Explanation" object MUST remain locked within the exact same medical disease entity domain.
   - Absolutely NO crossing over, mixing up, or bleed-through of scenario text strings from other questions, preceding files, or historical data pools is permitted. If the choices evaluate a zoonosis (e.g., Weil disease), the Stem cannot reference gastrointestinal anatomy or any unrelated condition.

2. EXHAUSTIVE AND MANDATORY "WHY THE OTHER OPTIONS FAIL" KEYS:
   - For every question generated, the "Why the Other Options Fail" dictionary object MUST contain exactly THREE unique keys representing all three incorrect distractors. 
   - If the "Correct Answer" is "A", the keys inside "Why the Other Options Fail" MUST be explicitly labeled "B", "C", and "D". 
   - Missing or skipped keys are treated as a catastrophic system failure. Every single incorrect key must contain at least one complete sentence explaining why it is clinically inferior or wrong.

3. CONTEXTUAL ACCURACY LOOP-BACK RE-AUDIT:
   - Before printing the final output string, execute a loop-back validation check: Confirm that every option labeled as a failure actually matches the exact text string of that option listed in the choice block above it. 
   - (e.g., If Option C is "Azithromycin plus ceftriaxone", the text inside {"Why the Other Options Fail": {"C": "..."}} MUST explain why Azithromycin plus ceftriaxone fails).

4. ZERO PLACEHOLDER AND TRUNCATION POLICY:
   - Do not use generic diagnostic templates ("Infer the state...", "Apply the modifying clue..."). 
   - Every single line must contain explicit medical names, drugs, scores, or values tied directly to the clinical vignette. 
   - Do not truncate or compress descriptions; every rationale must provide definitive, actionable learning points for top-tier specialty aspirants.
To elevate standard medical questions to a true NEET SS Surgery or Medicine super-specialty standard, you must design questions that test higher-order discriminatory judgment rather than simple recall.These 3 rules ensure your questions meet that elite benchmark:1. Build Multi-Layered Clinical Vulnerability (The "Comorbidity Lock")NEET SS questions rarely present an isolated pathology. To achieve this quality, the clinical stem must feature competing physiological contradictions that eliminate simple textbook answers.How to apply it: Introduce advanced organ dysfunction (such as Stage 4 CKD, severe hepatic impairment, or decompensated heart failure) or acute safety risks (like active gastrointestinal bleeding) alongside the primary condition. This forces the candidate to navigate complex pharmacological trade-offs, making the standard "first-line" drug completely toxic or contraindicated.2. Design Traps Using "Near-Identical" Medical OptionsAt the super-specialty level, distractors must not be obviously wrong or non-sensical (e.g., avoiding choices like "monthly joint aspiration" or "increase purines"). Every single option must look highly plausible to a general practitioner.How to apply it: Ensure all choices are real medical interventions. Differentiate options by micro-nuances, such as:Contradicting an exact numeric threshold (e.g., targeting a serum urate of <5.0 mg/dL for severe tophaceous disease vs. <6.0 mg/dL for uncomplicated gout).Altering the timing or titration mechanics of a drug (e.g., initiating a medication immediately during a crisis versus delaying it or introducing it at a low dose with slow upward titration).3. Target Upstream Molecular Pathogenesis & PharmacogeneticsNEET SS heavily emphasizes the basic science underpinnings of clinical presentations. Questions should require a deep understanding of cellular cascades, receptors, and genetic markers.How to apply it: Do not stop at the diagnosis; push the question into the specific molecular pathway. Force candidates to identify the exact intracellular platform (such as the NLRP3 inflammasome and its downstream activation of caspase-1 to cleave pro-IL-1β), or test highly specific patient populations that mandate genetic screening (such as testing for the HLA-B*58:01 allele to prevent severe DRESS syndrome before starting xanthine oxidase inhibitors).
OUTPUT Return ONLY valid JSON CONTAINNING EACH MCQ as a object in the JSON : { "Stem": "...", "A": "...", "B": "...", "C": "...", "D": "...", "Correct Answer": "A", "Explanation": { "Correct Answer Summary": "...", "Diagnostic Pathway": ["...", "...", "..."], "Why the Other Options Fail": { "B": "...", "C": "...", "D": "..." }, "Examiner's Trap": "..." } } IMPORTANT JSON REQUIREMENT: Return exactly one valid JSON object using this structure: { "mcqs": [ { "Stem": "...", "A": "...", "B": "...", "C": "...", "D": "...", "Correct Answer": "A", "Explanation": { "Correct Answer Summary": "...", "Diagnostic Pathway": ["...", "...", "..."], "Why the Other Options Fail": { "B": "...", "C": "...", "D": "..." }, "Examiner's Trap": "..." } } ] } The mcqs array must contain exactly 10 MCQ objects. Do not include Markdown fences or any text outside JSON.`.trim();

if (!SYSTEM_PROMPT) {
  throw new Error("SYSTEM_PROMPT cannot be empty");
}

const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

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
  if (isCreditExhaustionError(error)) return false;

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
    /timeout|temporar|unavailable|rate limit|ECONNRESET|ETIMEDOUT|socket hang up/i.test(message) ||
    /invalid JSON|empty output|exactly 30|stem has|option .* has|duplicate options|invalid Correct Answer|Diagnostic Pathway|incorrect explanation keys|is missing|is invalid|no valid Explanation/i.test(message)
  );
}

function countWords(value) {
  return String(value).trim().split(/\s+/u).filter(Boolean).length;
}

function requireNonEmptyString(value, description) {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`${description} is missing`);
  }
  return value.trim();
}

function normalizeComparable(value) {
  return String(value)
    .normalize("NFKC")
    .trim()
    .replace(/\s+/gu, " ")
    .toLocaleLowerCase("en");
}

function buildUserInput(row) {
  return [
    "Return exactly one valid json object and no text outside the json object.",
    `The json object must contain exactly ${MCQ_COUNT} MCQs in the mcqs array.`,
    `SUBJECT: ${row.subject}`,
    `TOPIC: ${row.topic}`,
    `TOPIC SERIAL NUMBER: ${row.serial_number}`,
    "",
    "SOURCE RAPID REVISION NOTES:",
    JSON.stringify(row.notes_json, null, 2)
  ].join("\n");
}

function extractResponseText(response) {
  if (typeof response?.output_text === "string" && response.output_text.trim()) {
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

  const output = collected.join("\n").trim();
  if (!output) throw new Error("OpenAI returned empty output");
  return output;
}

function cleanJsonText(rawOutput) {
  return String(rawOutput)
    .trim()
    .replace(/^\s*```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();
}

function validateGeneratedOutput(rawOutput) {
  const cleaned = cleanJsonText(rawOutput);
  let parsed;

  try {
    parsed = JSON.parse(cleaned);
  } catch (error) {
    throw new Error(`Model returned invalid JSON: ${error.message}`);
  }

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Generated output must be one JSON object");
  }

  if (!Array.isArray(parsed.mcqs) || parsed.mcqs.length !== MCQ_COUNT) {
    throw new Error(
      `Generated output contains ${Array.isArray(parsed.mcqs) ? parsed.mcqs.length : 0} MCQs; exactly ${MCQ_COUNT} required`
    );
  }

  const optionLetters = ["A", "B", "C", "D"];
  const seenStems = new Set();

  const mcqs = parsed.mcqs.map((mcq, index) => {
    const number = index + 1;

    if (!mcq || typeof mcq !== "object" || Array.isArray(mcq)) {
      throw new Error(`MCQ ${number} is invalid`);
    }

    const stem = requireNonEmptyString(mcq.Stem, `MCQ ${number} Stem`);
    const stemWords = countWords(stem);

    if (stemWords < 30 || stemWords > 40) {
      throw new Error(
        `MCQ ${number} stem has ${stemWords} words; required range is 30â€“40`
      );
    }

    const stemKey = normalizeComparable(stem);
    if (seenStems.has(stemKey)) {
      throw new Error(`MCQ ${number} contains a duplicate stem`);
    }
    seenStems.add(stemKey);

    const options = {};
    for (const letter of optionLetters) {
      const option = requireNonEmptyString(
        mcq[letter],
        `MCQ ${number} option ${letter}`
      );
      const optionWords = countWords(option);
      if (optionWords < 2 || optionWords > 5) {
        throw new Error(
          `MCQ ${number} option ${letter} has ${optionWords} words; required range is 2â€“5`
        );
      }
      options[letter] = option;
    }

    const optionValues = optionLetters.map(
      (letter) => normalizeComparable(options[letter])
    );
    if (new Set(optionValues).size !== 4) {
      throw new Error(`MCQ ${number} contains duplicate options`);
    }

    const correctAnswer = String(mcq["Correct Answer"] || "")
      .trim()
      .toUpperCase();

    if (!optionLetters.includes(correctAnswer)) {
      throw new Error(`MCQ ${number} has invalid Correct Answer`);
    }

    const explanation = mcq.Explanation;
    if (
      !explanation ||
      typeof explanation !== "object" ||
      Array.isArray(explanation)
    ) {
      throw new Error(`MCQ ${number} has no valid Explanation`);
    }

    const summary = requireNonEmptyString(
      explanation["Correct Answer Summary"],
      `MCQ ${number} Correct Answer Summary`
    );

    const pathway = explanation["Diagnostic Pathway"];
    if (!Array.isArray(pathway) || pathway.length < 2 || pathway.length > 6) {
      throw new Error(
        `MCQ ${number} requires 2â€“6 Diagnostic Pathway steps`
      );
    }

    const normalizedPathway = pathway.map((step, stepIndex) =>
      requireNonEmptyString(
        step,
        `MCQ ${number} Diagnostic Pathway step ${stepIndex + 1}`
      )
    );

    const whyOthers = explanation["Why the Other Options Fail"];
    if (
      !whyOthers ||
      typeof whyOthers !== "object" ||
      Array.isArray(whyOthers)
    ) {
      throw new Error(
        `MCQ ${number} has no valid Why the Other Options Fail object`
      );
    }

    const requiredWrongLetters = optionLetters
      .filter((letter) => letter !== correctAnswer)
      .sort();

    const suppliedWrongLetters = Object.keys(whyOthers)
      .map((letter) => letter.trim().toUpperCase())
      .sort();

    if (
      JSON.stringify(requiredWrongLetters) !==
      JSON.stringify(suppliedWrongLetters)
    ) {
      throw new Error(
        `MCQ ${number} incorrect explanation keys must be ${requiredWrongLetters.join(", ")}`
      );
    }

    const normalizedWhyOthers = {};
    for (const letter of requiredWrongLetters) {
      normalizedWhyOthers[letter] = requireNonEmptyString(
        whyOthers[letter],
        `MCQ ${number} explanation for option ${letter}`
      );
    }

    const examinerTrap = requireNonEmptyString(
      explanation["Examiner's Trap"],
      `MCQ ${number} Examiner's Trap`
    );

    return {
      Stem: stem,
      A: options.A,
      B: options.B,
      C: options.C,
      D: options.D,
      "Correct Answer": correctAnswer,
      Explanation: {
        "Correct Answer Summary": summary,
        "Diagnostic Pathway": normalizedPathway,
        "Why the Other Options Fail": normalizedWhyOthers,
        "Examiner's Trap": examinerTrap
      }
    };
  });

  return { mcqs };
}

async function generateMcqs(row) {
  let lastError;

  for (let attempt = 0; attempt <= API_RETRIES; attempt += 1) {
    try {
      const response = await openai.responses.create({
        model: MODEL,
        instructions: SYSTEM_PROMPT,
        input: buildUserInput(row),
        max_output_tokens: MAX_OUTPUT_TOKENS,
        text: {
          format: {
            type: "json_object"
          }
        }
      });

      return validateGeneratedOutput(extractResponseText(response));
    } catch (error) {
      lastError = error;

      if (isCreditExhaustionError(error)) throw error;

      const retriesFinished = attempt === API_RETRIES;
      if (retriesFinished || !isRetryableError(error)) break;

      const delay =
        1000 * 2 ** attempt +
        Math.floor(Math.random() * 300);

      console.warn(
        `âš ï¸ API retry ${attempt + 1}/${API_RETRIES} after ${delay} ms`
      );
      await sleep(delay);
    }
  }

  throw lastError;
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
      `Failed to release expired MCQ locks: ${error.message}`
    );
  }
}

async function lockOneRow(row) {
  const lockedAt = new Date().toISOString();

  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: true,
      [LOCK_AT_COL]: lockedAt
    })
    .eq("id", row.id)
    .eq(LOCK_COL, false)
    .not(INPUT_COL, "is", null)
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

async function claimRows(limit) {
  await releaseExpiredLocks();

  const { data: availableRows, error } = await supabase
    .from(TABLE)
    .select("id")
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .eq(LOCK_COL, false)
    .order("serial_number", { ascending: true })
    .limit(limit);

  if (error) {
    throw new Error(
      `Failed to select pending MCQ rows: ${error.message}`
    );
  }

  if (!availableRows?.length) return [];

  const lockResults = await Promise.allSettled(
    availableRows.map((row) => lockOneRow(row))
  );

  const claimedRows = [];
  for (const result of lockResults) {
    if (result.status === "fulfilled" && result.value) {
      claimedRows.push(result.value);
    } else if (result.status === "rejected") {
      console.error("âŒ Row-lock error:", getErrorText(result.reason));
    }
  }

  return claimedRows;
}

async function saveSuccess(row, output) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: output,
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null
    })
    .eq("id", row.id)
    .eq(LOCK_COL, true)
    .eq(LOCK_AT_COL, row[LOCK_AT_COL])
    .is(OUTPUT_COL, null)
    .select("id");

  if (error) {
    throw new Error(`Failed to save MCQs: ${error.message}`);
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or MCQs already exist"
    );
  }
}

async function releaseRowLock(row) {
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
      `âŒ Failed to release lock ${row.id}: ${error.message}`
    );
  }
}

async function releaseClaimedRows(rows) {
  await Promise.allSettled(
    rows.map((row) => releaseRowLock(row))
  );
}

async function processRow(row) {
  console.log(
    `ðŸ§  Generating MCQs | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const output = await generateMcqs(row);
    await saveSuccess(row, output);

    console.log(
      `âœ… Completed | ${row.subject} | ${row.serial_number} | MCQs=${output.mcqs.length}`
    );

    return { success: true, creditExhausted: false };
  } catch (error) {
    await releaseRowLock(row);

    if (isCreditExhaustionError(error)) {
      console.error(
        "ðŸ›‘ API credits exhausted. Worker will stop safely."
      );
      return {
        success: false,
        creditExhausted: true,
        error
      };
    }

    console.error(
      `âŒ Failed | ${row.subject} | ${row.serial_number} | ${row.topic}: ${getErrorText(error)}`
    );

    return {
      success: false,
      creditExhausted: false,
      error
    };
  }
}

async function processWithConcurrency(rows) {
  let nextIndex = 0;
  let creditExhausted = false;

  async function runner() {
    while (nextIndex < rows.length && !creditExhausted) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      const result = await processRow(rows[currentIndex]);
      if (result.creditExhausted) creditExhausted = true;
    }
  }

  const runnerCount = Math.min(BATCH_SIZE, rows.length);

  await Promise.all(
    Array.from({ length: runnerCount }, () => runner())
  );

  if (creditExhausted) {
    await releaseClaimedRows(rows.slice(nextIndex));
  }

  return { creditExhausted };
}

async function main() {
  console.log(
    `ðŸš€ NEET-SS MEDICINE MCQ WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `âš™ï¸ Model=${MODEL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE} | MCQs/topic=${MCQ_COUNT} | Max output=${MAX_OUTPUT_TOKENS}`
  );

  while (true) {
    try {
      const rows = await claimRows(PICKUP_LIMIT);

      if (!rows.length) {
        await sleep(LOOP_SLEEP_MS);
        continue;
      }

      console.log(`ðŸ“¥ Claimed ${rows.length} topic(s)`);

      const result = await processWithConcurrency(rows);

      if (result.creditExhausted) {
        console.error(
          "ðŸ›‘ Worker stopped because the API account has no available credits."
        );
        process.exit(1);
      }
    } catch (error) {
      if (isCreditExhaustionError(error)) {
        console.error(
          "ðŸ›‘ Worker stopped: API credits exhausted."
        );
        process.exit(1);
      }

      console.error("âŒ Worker loop error:", getErrorText(error));
      await sleep(Math.max(LOOP_SLEEP_MS, 2000));
    }
  }
}

main().catch((error) => {
  console.error(
    "âŒ Fatal NEET-SS Medicine MCQ worker error:",
    error
  );
  process.exit(1);
});
