require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL =
  process.env.FMGE_EXPLANATION_MODEL || "gpt-5-mini";

const LIMIT = parseInt(
  process.env.FMGE_EXPLANATION_LIMIT || "10",
  10
);

const BATCH_SIZE = parseInt(
  process.env.FMGE_EXPLANATION_BATCH_SIZE || "5",
  10
);

const SLEEP_MS = parseInt(
  process.env.FMGE_EXPLANATION_LOOP_SLEEP_MS || "300",
  10
);

const LOCK_TTL_MIN = parseInt(
  process.env.FMGE_EXPLANATION_LOCK_TTL_MIN || "15",
  10
);

const WORKER_ID =
  process.env.WORKER_ID ||
  `fmge-explanation-worker-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 6)}`;

const TABLE = "fmge_master_content";

const INPUT_COL = "question";
const OUTPUT_COL = "explanation";
const LOCK_COL = "explanation_lock";
const LOCK_AT = "explanation_locked_at";

console.log("🚀 FMGE EXPLANATION WORKER STARTED:", WORKER_ID);

// ─────────────────────────────────────────────
// PROMPT — USED EXACTLY AS PROVIDED
// ─────────────────────────────────────────────
function buildPrompt(question) {
  return String.raw`
you are NEETPG FMGE Medical Educator , for this Question , give 5 Bullet pointerd Buzz word styled Explanation emphasising any logical points , closely related HYFs , Exam traps and correct way to remeber , that student Actively revise . Give as a UniCode Mark Down Text with highlighting key words in explanation the Bold , italic , Super scripts , subscripts , symbols
 ### 🚨 CRITICAL MEDICAL INSTRUCTION: CLINICAL DEPTH & SYSTEMIC RULES 1. **ANATOMICAL PRECISION RULE**: Never write general anatomical terms when sub-localizations exist. - *Example*: Do not just write "retraction pocket." You must explicitly differentiate Pars Flaccida ($P_f$) and Pars Tensa ($P_t$) using LaTeX/Unicode subscripts. 2. **CLASSIFICATION COMPARTMENTALIZATION**: If a question asks about a specific staging system (e.g., Tos, Sadé, House-Brackmann, Bismuth, TNM), the prompt MUST explicitly feature a "Multi-System Differential" or "Anatomical Cross-Reference" bullet point. This bullet must directly compare the correct answer stage with the same stage number of its closest competitor system to dismantle examiner traps. 3. **EXAM TRAP DISCRIMINATION**: Every question response must include exactly two explicitly labeled, high-yield clinical "Traps". These must be derived from actual PG exam variables (e.g., changing a single keyword like "entirely visible" to "partially hidden", or switching "malleus neck" to "incus long process"). 4. **MATHEMATICAL & SYMBOLIC ENHANCEMENT**: You must utilize rigorous Markdown and LaTeX typesetting syntax for formatting. Use symbols ($\pm$, $\rightarrow$, $\Rightarrow$, $\Delta$), subscripts ($P_f$, $P_t$), and uppercase Roman numerals ($I, II, III, IV$) instead of standard integers for staging to optimize scannability on mobile frontend viewports (RNW). 5. **THE "STAGE LADDER" MANDATE**: For any question based on a clinical classification, staging, or scoring system, you must provide a clean, complete vertical progression ladder of all stages. Do not summarize or skip steps. Each step must be limited to exactly one punchy, high-yield sentence fragment containing the specific diagnostic discriminator for that stage.

QUESTION:
${question}
`;
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function isRetryable(error) {
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(
    String(error?.message || error)
  );
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const response = await openai.chat.completions.create({
      model: MODEL,
      messages: [
        {
          role: "user",
          content: prompt
        }
      ]
    });

    const output =
      response.choices?.[0]?.message?.content?.trim();

    if (!output) {
      throw new Error("OpenAI returned an empty explanation");
    }

    return output;
  } catch (error) {
    if (isRetryable(error) && attempt <= 2) {
      const retryDelay = 800 * attempt;

      console.warn(
        `⚠️ OpenAI retry ${attempt}/2 after ${retryDelay} ms`
      );

      await sleep(retryDelay);
      return callOpenAI(prompt, attempt + 1);
    }

    throw error;
  }
}

// ─────────────────────────────────────────────
// RELEASE EXPIRED LOCKS
// ─────────────────────────────────────────────
async function releaseExpiredLocks() {
  const cutoff = new Date(
    Date.now() - LOCK_TTL_MIN * 60 * 1000
  ).toISOString();

  const { error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: false,
      [LOCK_AT]: null
    })
    .eq(LOCK_COL, true)
    .lt(LOCK_AT, cutoff)
    .is(OUTPUT_COL, null);

  if (error) {
    throw new Error(
      `Failed to release expired locks: ${error.message}`
    );
  }
}

// ─────────────────────────────────────────────
// CLAIM ROWS
// ─────────────────────────────────────────────
async function claimRows(limit) {
  await releaseExpiredLocks();

  const { data: availableRows, error: selectError } =
    await supabase
      .from(TABLE)
      .select("id, question")
      .not(INPUT_COL, "is", null)
      .is(OUTPUT_COL, null)
      .eq(LOCK_COL, false)
      .eq("active", true)
      .order("serial_number", { ascending: true })
      .limit(limit);

  if (selectError) {
    throw new Error(
      `Failed to select rows: ${selectError.message}`
    );
  }

  if (!availableRows?.length) {
    return [];
  }

  const ids = availableRows.map((row) => row.id);
  const lockedAt = new Date().toISOString();

  const { data: lockedRows, error: lockError } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: true,
      [LOCK_AT]: lockedAt
    })
    .in("id", ids)
    .eq(LOCK_COL, false)
    .is(OUTPUT_COL, null)
    .eq("active", true)
    .select("id, question");

  if (lockError) {
    throw new Error(
      `Failed to lock rows: ${lockError.message}`
    );
  }

  return lockedRows || [];
}

// ─────────────────────────────────────────────
// RELEASE ONE ROW LOCK
// ─────────────────────────────────────────────
async function releaseRowLock(rowId) {
  const { error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: false,
      [LOCK_AT]: null
    })
    .eq("id", rowId)
    .eq(LOCK_COL, true);

  if (error) {
    console.error(
      `❌ Could not release lock for ${rowId}:`,
      error.message
    );
  }
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  try {
    const output = await callOpenAI(
      buildPrompt(row.question)
    );

    const { error: updateError } = await supabase
      .from(TABLE)
      .update({
        [OUTPUT_COL]: output,
        [LOCK_COL]: false,
        [LOCK_AT]: null,
        updated_at: new Date().toISOString()
      })
      .eq("id", row.id)
      .eq(LOCK_COL, true)
      .is(OUTPUT_COL, null);

    if (updateError) {
      throw new Error(
        `Failed to save explanation: ${updateError.message}`
      );
    }

    console.log("✅ Explanation generated:", row.id);
  } catch (error) {
    console.error(
      "❌ Explanation failed:",
      row.id,
      error?.message || error
    );

    await releaseRowLock(row.id);
  }
}

// ─────────────────────────────────────────────
// PROCESS IN BATCHES
// ─────────────────────────────────────────────
async function processRowsInBatches(rows) {
  for (let index = 0; index < rows.length; index += BATCH_SIZE) {
    const batch = rows.slice(index, index + BATCH_SIZE);

    const results = await Promise.allSettled(
      batch.map((row) => processRow(row))
    );

    const rejected = results.filter(
      (result) => result.status === "rejected"
    );

    if (rejected.length) {
      console.error(
        `❌ ${rejected.length} unexpected batch failure(s)`
      );
    }
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(
    `🧠 FMGE EXPLANATION WORKER RUNNING | ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model: ${MODEL} | Limit: ${LIMIT} | Batch size: ${BATCH_SIZE}`
  );

  while (true) {
    try {
      const rows = await claimRows(LIMIT);

      if (!rows.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`📥 Claimed ${rows.length} row(s)`);

      await processRowsInBatches(rows);
    } catch (error) {
      console.error(
        "❌ Worker loop error:",
        error?.message || error
      );

      await sleep(Math.max(SLEEP_MS, 2000));
    }
  }
})();
