require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.IMAGE_STEM_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.IMAGE_STEM_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.IMAGE_STEM_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.IMAGE_STEM_LOOP_SLEEP_MS || "400", 10);
const LOCK_TTL_MIN = parseInt(process.env.IMAGE_STEM_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `image-stem-worker-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

const TABLE_NAME   = "mock_tests_phases";
const LOCK_COL     = "image_job_lock";
const LOCK_AT_COL  = "image_job_lock_at";

console.log("🚀 IMAGE STEM WORKER STARTED:", WORKER_ID);

// ─────────────────────────────────────────────
// PROMPT (USE EXACTLY AS GIVEN)
// ─────────────────────────────────────────────
function buildPrompt(mcqObj) {
  return `
You are a 30-year experienced NEET-PG / INI-CET paper setter, examiner, and question bank architect.

Your task is to generate a stem for NEETPG-style Integrated Image-Based MCQ based on:
Options, image_description, learning_gap, correct_answer, high_yield_facts.

✔ Tests process understanding  
✔ Requires 3-step reasoning chain  
✔ Must be a typical stem asked in NEETPG Exam  

STEM RULES:
- Stem MUST be more than 15 words and less than 20 words.
- MUST include the exact phrase: "Check the Image given."
- MUST NOT contain imaging/radiology description or keywords.
- MUST NOT leak image_description.

Only output the new stem text.

INPUT:
${JSON.stringify(mcqObj)}
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

  // clear expired locks
  await supabase
    .from(TABLE_NAME)
    .update({ [LOCK_COL]: null, [LOCK_AT_COL]: null })
    .lt(LOCK_AT_COL, cutoff);

  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select("id, image_mcqs")
    .eq("check_image_yes_no", "YES")
    .not("image_mcqs", "is", null)
    .is(LOCK_COL, null)
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  const { data: locked } = await supabase
    .from(TABLE_NAME)
    .update({
      [LOCK_COL]: WORKER_ID,
      [LOCK_AT_COL]: new Date().toISOString()
    })
    .in("id", ids)
    .is(LOCK_COL, null)
    .select("id, image_mcqs");

  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  try {

    const mcqObj = row.image_mcqs[0]; // assuming array of 1
    const newStem = await callOpenAI(buildPrompt(mcqObj));

    mcqObj.stem = newStem;

    await supabase
      .from(TABLE_NAME)
      .update({
        image_mcqs: [mcqObj],
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id);

    console.log("✅ Stem updated:", row.id);

  } catch (err) {

    console.error("❌ Failed:", row.id, err.message);

    await supabase
      .from(TABLE_NAME)
      .update({
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id);
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {

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
