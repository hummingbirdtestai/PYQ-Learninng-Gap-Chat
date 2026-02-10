require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.HYF_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.HYF_LIMIT || "150", 10);
const BATCH_SIZE   = parseInt(process.env.HYF_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.HYF_LOOP_SLEEP_MS || "200", 10);
const LOCK_TTL_MIN = parseInt(process.env.HYF_LOCK_TTL_MIN || "10", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `hyf-classifier-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS)
// ─────────────────────────────────────────────
function buildPrompt(question) {
  return `
This is a curriculum-design / exam-weighting question, not content writing.

Classify the topic based on how many USMLE-style high-yield buzzword facts
can realistically be generated from it.

Return ONLY ONE NUMBER from the following:

75 → very large, repeatedly tested, multi-system, guideline-heavy  
40 → medium–large, exam-relevant but bounded  
15 → short, definition-based, single-concept or list-type  

Return ONLY: 75 or 40 or 15

QUESTION:
${question}
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
      await sleep(600 * attempt);
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

  // Clear expired locks
  await supabase
    .from("all_subjects_raw")
    .update({ concept_lock: null, concept_lock_at: null })
    .lt("concept_lock_at", cutoff);

  const { data, error } = await supabase
    .from("all_subjects_raw")
    .select("id, question")
    .is("hyf_number", null)
    .is("concept_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;

  console.log("🔍 Rows fetched for HYF:", data?.length);
  
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  const { data: locked, error: err2 } = await supabase
    .from("all_subjects_raw")
    .update({
      concept_lock: WORKER_ID,
      concept_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("concept_lock", null)
    .select("id, question");

  if (err2) throw err2;
  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const raw = await callOpenAI(buildPrompt(row.question));

  const value = Number(raw);

  if (![75, 40, 15].includes(value)) {
    throw new Error(`Invalid HYF value returned: ${raw}`);
  }

  await supabase
    .from("all_subjects_raw")
    .update({
      hyf_number: value,
      concept_lock: null,
      concept_lock_at: null
    })
    .eq("id", row.id);

  return true;
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 HYF CLASSIFIER WORKER STARTED | ${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      for (let i = 0; i < claimed.length; i += BATCH_SIZE) {
        const batch = claimed.slice(i, i + BATCH_SIZE);

        await Promise.allSettled(batch.map(processRow));
      }
    } catch (e) {
      console.error("❌ Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
