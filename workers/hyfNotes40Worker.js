require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.HYF_NOTES_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.HYF_NOTES_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.HYF_NOTES_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.HYF_NOTES_LOOP_SLEEP_MS || "300", 10);
const LOCK_TTL_MIN = parseInt(process.env.HYF_NOTES_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `hyf-notes-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE **AS IS** — DO NOT MODIFY)
// ─────────────────────────────────────────────
function buildPrompt(question) {
  return `
give 20 Buzz word styled High Yield facts Must to remember in 2 Buckets , each with 10 Buzz word Styled HYFs. each in less than 6 Words , numbered globally from "1" to "20" , for each Bucket 1 USMLE Styled Clinical case Vignette based MCQ with 4 Options and Correct Answer and Exam Trap in the MCQ and what to Remember in Exam to test the HYFs in each of the Bucket . ──────────────────────────────────── JSON STRUCTURE (STRICT — DO NOT DEVIATE) ──────────────────────────────────── { "topic": "<topic_name>", "bucket_1": { ... }, "bucket_2": { ... } } Each bucket MUST contain EXACTLY: { "bucket": <1–2>, "title": "concise exam-oriented title", "hyfs": { "1": "...", "2": "...", "3": "...", "4": "...", "10": "..." }, "mcq": { "stem": "USMLE-style clinical vignette implying history, exam, investigations", "options": { "A": "...", "B": "...", "C": "...", "D": "..." }, "correct_answer": "A | B | C | D", "exam_trap": "common exam confusion tested here", "what_to_remember": "single decisive recall point for exams" } } ──────────────────────────────────── HIGH-YIELD FACT (HYF) RULES — VERY STRICT ──────────────────────────────────── • Total HYFs = EXACTLY 20 (10 per bucket) • HYFs must be numbered globally from "1" to "20" • - MUST contain **EXACTLY ONE word** that is ***bold + italic*** -Unicode arrows (↑ ↓ →), symbols (±, ≤, ≥), subscripts allowed GENERATE THE JSON NOW as code

TOPIC:
${question}

GENERATE THE JSON NOW as code
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
// CLAIM ROWS (hyf_number = 40)
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  await supabase
    .from("all_subjects_raw")
    .update({ concept_lock: null, concept_lock_at: null })
    .lt("concept_lock_at", cutoff);

  const { data, error } = await supabase
    .from("all_subjects_raw")
    .select("id, question")
    .eq("hyf_number", 40)
    .is("notes_hyf", null)
    .is("concept_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
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

  let json;
  try {
    json = JSON.parse(raw);
  } catch {
    throw new Error("Invalid JSON returned by model");
  }

  await supabase
    .from("all_subjects_raw")
    .update({
      notes_hyf: json,
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
  console.log(`🧠 HYF NOTES WORKER STARTED | ${WORKER_ID}`);

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
      console.error("❌ Worker error:", e);
      await sleep(1200);
    }
  }
})();
