require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.MOCK_EXAM_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MOCK_EXAM_LIMIT || "10", 10);
const BATCH_SIZE   = parseInt(process.env.MOCK_EXAM_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.MOCK_EXAM_LOOP_SLEEP_MS || "300", 10);
const LOCK_TTL_MIN = parseInt(process.env.MOCK_EXAM_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `mock-exam-mcq-worker-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

const TABLE = "30_mock_tests";
const LOCK_COL = "mcq_json_lock";
const LOCK_AT  = "mcq_json_lock_at";

console.log("🚀 MOCK EXAM MCQ WORKER STARTED:", WORKER_ID);

// ─────────────────────────────────────────────
// PROMPT (USE EXACTLY AS GIVEN)
// ─────────────────────────────────────────────
function buildPrompt(mcqData) {
  return `
Create a NEETPG Styled MCQ based on most commonly tested high Yield pattern on Topic below in standard and difficulty level of UWorld Amboss First AID , with 4 options and they should not have option like None of Above , All of above , Stem should not contain Except . It should be Single Best Answer and has high probability to appenar in NEETPG EXAM give only the Question , single best answer , no need for explanation

INPUT:
${JSON.stringify(mcqData)}
`;
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i
    .test(String(e?.message || e));
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }]
    });

    return resp.choices?.[0]?.message?.content?.trim();

  } catch (e) {
    if (isRetryable(e) && attempt <= 2) {
      await sleep(800 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

// ─────────────────────────────────────────────
// CLAIM ROWS
// ─────────────────────────────────────────────
async function claimRows(limit) {

  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // Release expired locks
  await supabase
    .from(TABLE)
    .update({ [LOCK_COL]: null, [LOCK_AT]: null })
    .lt(LOCK_AT, cutoff);

  const { data, error } = await supabase
    .from(TABLE)
    .select("id, mcq")
    .not("mcq", "is", null)
    .not("exam_number", "is", null)
    .is("mcq_in_exam", null)
    .is(LOCK_COL, null)
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  const { data: locked } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: WORKER_ID,
      [LOCK_AT]: new Date().toISOString()
    })
    .in("id", ids)
    .is(LOCK_COL, null)
    .select("id, mcq");

  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  try {

    const output = await callOpenAI(buildPrompt(row.mcq));

    await supabase
      .from(TABLE)
      .update({
        mcq_in_exam: output,
        [LOCK_COL]: null,
        [LOCK_AT]: null
      })
      .eq("id", row.id);

    console.log("✅ MCQ generated:", row.id);

  } catch (err) {

    console.error("❌ Failed:", row.id, err.message);

    await supabase
      .from(TABLE)
      .update({
        [LOCK_COL]: null,
        [LOCK_AT]: null
      })
      .eq("id", row.id);
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {

  console.log(`🧠 MOCK EXAM WORKER RUNNING | ${WORKER_ID}`);

  while (true) {

    const rows = await claimRows(LIMIT);

    if (!rows.length) {
      await sleep(SLEEP_MS);
      continue;
    }

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      await Promise.allSettled(batch.map(processRow));
    }
  }
})();
