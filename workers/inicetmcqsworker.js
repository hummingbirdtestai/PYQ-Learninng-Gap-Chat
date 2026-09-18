require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// TABLE AND COLUMNS
// ─────────────────────────────────────────────

const TABLE = "inicet_pyt_source";

const INPUT_COL = "notes_json";
const OUTPUT_COL = "mcq_json";

const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

// ─────────────────────────────────────────────
// SETTINGS
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
      `${name} must be between ${min} and ${max}`
    );
  }

  return value;
}

const MODEL =
  process.env.INICET_MCQ_MODEL ||
  "gpt-5-mini";

const PICKUP_LIMIT = parseIntegerEnv(
  "INICET_MCQ_LIMIT",
  20,
  1,
  100
);

const BATCH_SIZE = parseIntegerEnv(
  "INICET_MCQ_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "INICET_MCQ_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "INICET_MCQ_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = parseIntegerEnv(
  "INICET_MCQ_API_RETRIES",
  2,
  0,
  5
);

const WORKER_ID =
  process.env.WORKER_ID ||
  `inicet-mcq-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = String.raw`
You are an expert medical MCQ writer for USMLE Step 1/2CK, NEET-PG, and FMGE. tAKE THE FLASH CARD Q--> As that have PYQs and future Questions , create 10 medically accurate, exam-standard clinical vignette MCQs. Fror Each MCQ STEM Write a 30–40 word clinical stem requiring 2–3 levels of reasoning: infer the condition → recognize the relevant complication/anatomy/physiology → answer the tested concept. Never reveal the answer pathway. Do not explicitly name the target diagnosis, structure, gene, enzyme, vessel, pathway, biochemical state, or mechanism being tested. Show findings; make the student infer them. Use a logical clinical sequence where applicable: context/risk → presentation → relevant examination/vitals → relevant investigations/intervention → lead-in. Include exactly one plausible red herring only when it genuinely competes with the correct diagnosis. Discriminatory Value Rule Every stem detail must do at least one: Support the correct answer. Weaken a distractor. Establish necessary timing/severity/context. Serve as the intentional red herring. Delete everything else. Never add decorative demographics, history, normal findings, routine vitals, or irrelevant tests merely to make the vignette realistic. Use raw values with reference ranges instead of labels such as “anemia,” “hyperkalemia,” or “leukocytosis.” Include labs/vitals/imaging only when relevant to solving the question. Never invent irrelevant data to satisfy formatting. Vitals must physiologically match the clinical state. The final lead-in must be neutral and contain no diagnostic or mechanistic hint. OPTIONS Provide exactly four competitive options (A–D), each 2–5 words. Options must be: grammatically and structurally parallel, similar in specificity, medically plausible, mutually distinct, from the same conceptual category. Avoid giveaway opposites, obviously unrelated distractors, or one option that differs conspicuously in length/structure. ACCURACY Medical, anatomical, embryological, pharmacological, and biochemical facts must be textbook-accurate. Management questions must follow current accepted guidelines. Do not oversimplify anatomical boundaries or mechanisms. EXPLANATION Explain: Correct Answer Summary: answer + core reason. Diagnostic Pathway: concise stepwise reasoning from clues to answer. Why Other Options Fail: individually explain B/C/D or whichever are incorrect. Examiner's Trap: identify the intended misconception/buzzword trap. Do not merely restate the answer. FINAL QUALITY CHECK Before output, verify: Stem = 30–40 words. Options = 2–5 words each. No answer giveaway. Every stem detail has discriminatory value. Exactly one best answer. No medically incorrect distractor logic. No unnecessary information. generation rule so every question follows something like: Clinical pattern → infer hidden diagnosis/state → identify a second implication → answer a downstream mechanism/management/complication. For example, instead of: Heavy alcohol + AST:ALT >2 → Which histology? the harder architecture is: Heavy alcohol + jaundice + AST:ALT pattern → infer alcohol-associated hepatitis → recognize severe disease/complication or treatment eligibility → identify the next decision, contraindication, response criterion, or downstream physiology. A genuinely difficult version might give severe alcohol-associated hepatitis, apparent eligibility for corticosteroids, and then subtle evidence of infection, GI bleeding, AKI, or another competing condition. The student has to diagnose → severity-stratify → notice the modifying factor → choose management. That is much closer to the reasoning style you're targeting. STRICT 2–3 LEVEL REASONING RULE: Never create an MCQ in which identifying the diagnosis immediately reveals the answer. Every question must require at least one additional inference after recognizing the underlying condition. Prefer downstream management decisions, complications, contraindications, response assessment, physiologic consequences, clinicopathologic correlations, or discrimination between closely competing diagnoses. If the question can be answered from a single buzzword or isolated memorized association, rewrite it. For 9.5–10/10 UWorld/AMBOSS/NBME-style depth, I would impose one more rule on your generator: After the hidden diagnosis/subtype has been inferred, at least two answer choices must remain clinically plausible. The student must use an additional modifying clue—severity, timing, contraindication, competing disease, test limitation, treatment eligibility, or response criterion—to eliminate the final competing option. That distinction is important. Instead of: Bronchiectasis → AA amyloidosis → what precursor? make it: Bronchiectasis → nephrotic syndrome → amyloid → infer AA → inflammatory disease now controlled but proteinuria worsening → determine whether persistent deposition, irreversible renal damage, or another amyloid subtype requires the next investigation/management step. Now remembering “AA = SAA” is insufficient. What would make the bank 9.5–10/10? Your generator needs one additional hard constraint: POST-DIAGNOSIS COMPETITION RULE: After the examinee correctly infers the underlying diagnosis, drug toxicity, interaction, or physiologic state, the answer must still NOT be obvious. At least two options must remain medically defensible until the examinee applies one additional clue involving severity, timing, contraindication, organ function, treatment history, resistance pattern, interaction, response criterion, or guideline threshold. And I would add: ASSOCIATION-ONLY REJECTION RULE: If the question can be solved by recognizing a single classic pair—drug → adverse effect, organism → drug, receptor → drug, mutation → disease, enzyme → substrate—the MCQ fails. Rewrite it so that recalling the association merely identifies the clinical state; a second decision must still be made. For example, instead of: Tipranavir → intracranial hemorrhage → stop which drug? make the architecture: Salvage ART → neurologic event → identify hemorrhage → distinguish medication toxicity from thrombocytopenia/interaction → determine which component must be withdrawn while preserving an active salvage regimen. Even better, force a competing management choice where two answers initially seem reasonable. What would make the bank 9.5–10/10? Your generator needs one additional hard constraint: POST-DIAGNOSIS COMPETITION RULE: After the examinee correctly infers the underlying diagnosis, drug toxicity, interaction, or physiologic state, the answer must still NOT be obvious. At least two options must remain medically defensible until the examinee applies one additional clue involving severity, timing, contraindication, organ function, treatment history, resistance pattern, interaction, response criterion, or guideline threshold. And I would add: ASSOCIATION-ONLY REJECTION RULE: If the question can be solved by recognizing a single classic pair—drug → adverse effect, organism → drug, receptor → drug, mutation → disease, enzyme → substrate—the MCQ fails. Rewrite it so that recalling the association merely identifies the clinical state; a second decision must still be made. For example, instead of: Tipranavir → intracranial hemorrhage → stop which drug? make the architecture: Salvage ART → neurologic event → identify hemorrhage → distinguish medication toxicity from thrombocytopenia/interaction → determine which component must be withdrawn while preserving an active salvage regimen. Even better, force a competing management choice where two answers initially seem reasonable. OUTPUT Return ONLY valid JSON CONTAINNING EACH MCQ as a object in the JSON : { "Stem": "...", "A": "...", "B": "...", "C": "...", "D": "...", "Correct Answer": "A", "Explanation": { "Correct Answer Summary": "...", "Diagnostic Pathway": ["...", "...", "..."], "Why the Other Options Fail": { "B": "...", "C": "...", "D": "..." }, "Examiner's Trap": "..." } } IMPORTANT JSON REQUIREMENT: Return exactly one valid JSON object using this structure: { "mcqs": [ { "Stem": "...", "A": "...", "B": "...", "C": "...", "D": "...", "Correct Answer": "A", "Explanation": { "Correct Answer Summary": "...", "Diagnostic Pathway": ["...", "...", "..."], "Why the Other Options Fail": { "B": "...", "C": "...", "D": "..." }, "Examiner's Trap": "..." } } ] } The mcqs array must contain exactly 10 MCQ objects. Do not include Markdown fences or any text outside JSON.
`.trim();

if (!SYSTEM_PROMPT) {
  throw new Error(
    "SYSTEM_PROMPT cannot be empty"
  );
}

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
  return /no credits remaining|insufficient_quota|billing_hard_limit|credit balance|billing/i.test(
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

function cleanJsonText(rawOutput) {
  return String(rawOutput)
    .trim()
    .replace(/^\s*```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();
}

function extractResponseText(response) {
  if (
    typeof response?.output_text === "string" &&
    response.output_text.trim()
  ) {
    return response.output_text.trim();
  }

  const parts = [];

  for (const outputItem of response?.output || []) {
    for (const contentItem of outputItem?.content || []) {
      if (
        contentItem?.type === "output_text" &&
        typeof contentItem.text === "string"
      ) {
        parts.push(contentItem.text);
      }
    }
  }

  const output = parts.join("\n").trim();

  if (!output) {
    throw new Error(
      "OpenAI returned empty output"
    );
  }

  return output;
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
    "",
    "RAPID REVISION NOTES:",
    JSON.stringify(row.notes_json, null, 2)
  ].join("\n");
}

// ─────────────────────────────────────────────
// VALIDATE EXPLANATION
// ─────────────────────────────────────────────

function validateExplanation(
  explanation,
  correctAnswer,
  mcqNumber
) {
  if (
    !explanation ||
    typeof explanation !== "object" ||
    Array.isArray(explanation)
  ) {
    throw new Error(
      `MCQ ${mcqNumber} has no valid Explanation object`
    );
  }

  const summary = String(
    explanation["Correct Answer Summary"] || ""
  ).trim();

  const pathway =
    explanation["Diagnostic Pathway"];

  const whyOthers =
    explanation["Why the Other Options Fail"];

  const examinerTrap = String(
    explanation["Examiner's Trap"] || ""
  ).trim();

  if (!summary) {
    throw new Error(
      `MCQ ${mcqNumber} is missing Correct Answer Summary`
    );
  }

  if (
    !Array.isArray(pathway) ||
    pathway.length < 2
  ) {
    throw new Error(
      `MCQ ${mcqNumber} requires at least two Diagnostic Pathway steps`
    );
  }

  const normalizedPathway = pathway.map(
    (step, index) => {
      const value = String(step || "").trim();

      if (!value) {
        throw new Error(
          `MCQ ${mcqNumber} has an empty Diagnostic Pathway step ${index + 1}`
        );
      }

      return value;
    }
  );

  if (
    !whyOthers ||
    typeof whyOthers !== "object" ||
    Array.isArray(whyOthers)
  ) {
    throw new Error(
      `MCQ ${mcqNumber} is missing Why the Other Options Fail`
    );
  }

  const incorrectLetters =
    ["A", "B", "C", "D"].filter(
      (letter) => letter !== correctAnswer
    );

  const normalizedWhyOthers = {};

  for (const letter of incorrectLetters) {
    const reason = String(
      whyOthers[letter] || ""
    ).trim();

    if (!reason) {
      throw new Error(
        `MCQ ${mcqNumber} is missing explanation for option ${letter}`
      );
    }

    normalizedWhyOthers[letter] = reason;
  }

  if (!examinerTrap) {
    throw new Error(
      `MCQ ${mcqNumber} is missing Examiner's Trap`
    );
  }

  return {
    "Correct Answer Summary": summary,
    "Diagnostic Pathway": normalizedPathway,
    "Why the Other Options Fail":
      normalizedWhyOthers,
    "Examiner's Trap": examinerTrap
  };
}

// ─────────────────────────────────────────────
// VALIDATE GENERATED MCQS
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
      "Output must be one JSON object"
    );
  }

  if (!Array.isArray(parsed.mcqs)) {
    throw new Error(
      "Generated JSON is missing the mcqs array"
    );
  }

  if (parsed.mcqs.length !== 10) {
    throw new Error(
      `Expected exactly 10 MCQs but received ${parsed.mcqs.length}`
    );
  }

  const normalizedMcqs = parsed.mcqs.map(
    (mcq, index) => {
      const mcqNumber = index + 1;

      if (
        !mcq ||
        typeof mcq !== "object" ||
        Array.isArray(mcq)
      ) {
        throw new Error(
          `MCQ ${mcqNumber} is not an object`
        );
      }

      const stem = String(
        mcq.Stem || ""
      ).trim();

      if (!stem) {
        throw new Error(
          `MCQ ${mcqNumber} is missing Stem`
        );
      }

      const stemWordCount =
        countWords(stem);

      if (
        stemWordCount < 30 ||
        stemWordCount > 40
      ) {
        throw new Error(
          `MCQ ${mcqNumber} stem has ${stemWordCount} words; required range is 30–40`
        );
      }

      const options = {};

      for (const letter of [
        "A",
        "B",
        "C",
        "D"
      ]) {
        const option = String(
          mcq[letter] || ""
        ).trim();

        if (!option) {
          throw new Error(
            `MCQ ${mcqNumber} is missing option ${letter}`
          );
        }

        const optionWordCount =
          countWords(option);

        if (
          optionWordCount < 2 ||
          optionWordCount > 5
        ) {
          throw new Error(
            `MCQ ${mcqNumber} option ${letter} has ${optionWordCount} words; required range is 2–5`
          );
        }

        options[letter] = option;
      }

      const uniqueOptions =
        new Set(
          Object.values(options).map(
            (value) => value.toLowerCase()
          )
        );

      if (uniqueOptions.size !== 4) {
        throw new Error(
          `MCQ ${mcqNumber} contains duplicate options`
        );
      }

      const correctAnswer = String(
        mcq["Correct Answer"] || ""
      )
        .trim()
        .toUpperCase();

      if (
        !["A", "B", "C", "D"].includes(
          correctAnswer
        )
      ) {
        throw new Error(
          `MCQ ${mcqNumber} has invalid Correct Answer`
        );
      }

      const explanation =
        validateExplanation(
          mcq.Explanation,
          correctAnswer,
          mcqNumber
        );

      return {
        Stem: stem,
        A: options.A,
        B: options.B,
        C: options.C,
        D: options.D,
        "Correct Answer": correctAnswer,
        Explanation: explanation
      };
    }
  );

  return {
    mcqs: normalizedMcqs
  };
}

// ─────────────────────────────────────────────
// CALL OPENAI
// ─────────────────────────────────────────────

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

          input: buildUserInput(row),

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
// RELEASE EXPIRED MCQ LOCKS
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
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .lt(LOCK_AT_COL, cutoff);

  if (error) {
    throw new Error(
      `Failed to release expired MCQ locks: ${error.message}`
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

// ─────────────────────────────────────────────
// CLAIM ROWS
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
      .is(OUTPUT_COL, null)
      .eq(LOCK_COL, false)
      .order("serial_number", {
        ascending: true
      })
      .limit(limit);

  if (error) {
    throw new Error(
      `Failed to select pending MCQ rows: ${error.message}`
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
// ─────────────────────────────────────────────

async function saveSuccess(row, mcqOutput) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: mcqOutput,
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
      "Save rejected because the lock changed or MCQs already exist"
    );
  }
}

// ─────────────────────────────────────────────
// RELEASE OWNED LOCK
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
    `🧠 Generating MCQs | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const mcqOutput =
      await generateMcqs(row);

    await saveSuccess(
      row,
      mcqOutput
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | MCQs=${mcqOutput.mcqs.length}`
    );

    return {
      success: true,
      creditExhausted: false
    };
  } catch (error) {
    await releaseRowLock(row);

    if (isCreditExhaustionError(error)) {
      console.error(
        "🛑 OpenAI credits exhausted"
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
// CONCURRENCY
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
    await releaseClaimedRows(
      rows.slice(nextIndex)
    );
  }

  return {
    creditExhausted
  };
}

// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────

async function main() {
  console.log(
    `🚀 INICET MCQ WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE}`
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

      if (result.creditExhausted) {
        console.error(
          "🛑 Worker stopped because API credits are unavailable"
        );

        process.exit(1);
      }
    } catch (error) {
      if (isCreditExhaustionError(error)) {
        console.error(
          "🛑 Worker stopped: OpenAI credits exhausted"
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
    "❌ Fatal INICET MCQ worker error:",
    error
  );

  process.exit(1);
});
