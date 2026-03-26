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

const TABLE = "mock_tests_phases";
const LOCK_COL = "image_job_lock";
const LOCK_AT  = "image_job_lock_at";

console.log("🚀 IMAGE STEM WORKER STARTED:", WORKER_ID);

// ─────────────────────────────────────────────
// PROMPT (EXACTLY AS PROVIDED — NO CHANGE)
// ─────────────────────────────────────────────
function buildPrompt(mcqObj) {
  return `
You are a 30-year experienced NEET-PG / INI-CET paper setter, examiner, and question bank architect. Your task is to generate a stem as text for NEETPG-style Integrated Image-Based MCQ based on the Options , image_description , learning_gap ,correct_answer, high_yield_facts ✔ Tests process understanding ✔ Requires 3-step reasoning chain ✔ Must be a typical stem of MCQ asked in NEETPG Exam where the image can fit into the Clinical Case Vignette STEM RULES: - Stem MUST be more than 15 words and less than 20 words. - MUST include the exact phrase: "Check the Image given." - MUST NOT contain any imaging/radiology description or keywords. IMAGE HANDLING RULE (HIGHEST PRIORITY): - NEVER describe, summarize, or hint at the image inside the "stem". - The model MUST NOT leak any part of "image_description" into the stem. Only create the value of stem and give as text OUTPUT VALIDATION RULE: ✔ No radiology/imaging keywords present in stem No explanation. No markdown. No commentary.

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

  // Release stale locks
  await supabase
    .from(TABLE)
    .update({ [LOCK_COL]: null, [LOCK_AT]: null })
    .lt(LOCK_AT, cutoff);

  const { data, error } = await supabase
    .from(TABLE)
    .select("id, image_mcqs")
    .eq("check_image_yes_no", "yes")
    .not("image_mcqs", "is", null)
    .is("stem", null)
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
    .select("id, image_mcqs");

  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  try {

    const mcqObj = Array.isArray(row.image_mcqs)
      ? row.image_mcqs[0]
      : row.image_mcqs;

    const newStem = await callOpenAI(buildPrompt(mcqObj));

    await supabase
      .from(TABLE)
      .update({
        stem: newStem,
        [LOCK_COL]: null,
        [LOCK_AT]: null
      })
      .eq("id", row.id);

    console.log("✅ Stem saved:", row.id);

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

  console.log(`🧠 IMAGE STEM WORKER RUNNING | ${WORKER_ID}`);

  while (true) {
    try {

      const rows = await claimRows(LIMIT);

      if (!rows.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`📥 Claimed ${rows.length} rows`);

      for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);

        const results = await Promise.allSettled(
          batch.map(processRow)
        );

        const success = results.filter(r => r.status === "fulfilled").length;
        const failed  = results.filter(r => r.status === "rejected").length;

        console.log(`⚙️ Batch done | Success: ${success} | Failed: ${failed}`);
      }

    } catch (e) {
      console.error("❌ Worker loop error:", e.message);
      await sleep(1200);
    }
  }
})();
