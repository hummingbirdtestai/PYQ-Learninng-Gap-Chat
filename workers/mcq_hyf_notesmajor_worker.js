require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.MCQ_HYF_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_HYF_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.MCQ_HYF_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.MCQ_HYF_LOOP_SLEEP_MS || "300", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_HYF_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `mcq-hyf-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT
// ─────────────────────────────────────────────
function buildPrompt(mcqJson) {
  return `
The following are PYQs in NEETPG converted into Single Liners.

give 50 Buzz word styled High Yield facts Must to remember in 5 Buckets , each with 10 Buzz word Styled HYFs. each in less than 6 Words , numbered globally from "1" to "50"

────────────────────────────────────
JSON STRUCTURE (STRICT — DO NOT DEVIATE)
────────────────────────────────────

{
  "topic": "<topic_name>",
  "bucket_1": { ... },
  "bucket_2": { ... },
  "bucket_3": { ... },
  "bucket_4": { ... },
  "bucket_5": { ... }
}

Each bucket MUST contain EXACTLY:

{
  "bucket": <1–5>,
  "title": "concise exam-oriented title",
  "hyfs": {
    "1": "...",
    "2": "...",
    "3": "...",
    "4": "...",
    "50": "..."
  }
}

────────────────────────────────────
HIGH-YIELD FACT (HYF) RULES — VERY STRICT
────────────────────────────────────

• Total HYFs = EXACTLY 50 (10 per bucket)
• HYFs must be numbered globally from "1" to "50"
• MUST contain EXACTLY ONE word that is **bold + italic**
• Unicode arrows (↑ ↓ →), symbols (±, ≤, ≥), subscripts allowed

GENERATE THE JSON NOW as code

PYQs:
${JSON.stringify(mcqJson)}
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

function extractJson(text) {
  if (!text) throw new Error("Empty model response");

  // Remove code fences
  let cleaned = text
    .replace(/```json/gi, "")
    .replace(/```/g, "")
    .trim();

  // Try to extract first JSON object if extra text exists
  const firstBrace = cleaned.indexOf("{");
  const lastBrace  = cleaned.lastIndexOf("}");

  if (firstBrace !== -1 && lastBrace !== -1) {
    cleaned = cleaned.substring(firstBrace, lastBrace + 1);
  }

  return JSON.parse(cleaned);
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }],
      temperature: 0.2
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

  // Clear expired locks
  await supabase
    .from("mcq_hyf_list")
    .update({ mcq_json_lock: null, mcq_json_lock_at: null })
    .lt("mcq_json_lock_at", cutoff);

  const { data, error } = await supabase
    .from("mcq_hyf_list")
    .select("id, mcq_json")
    .eq("topic_type", "Major")
    .is("notes_hyf", null)
    .is("mcq_json_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  const { data: locked, error: err2 } = await supabase
    .from("mcq_hyf_list")
    .update({
      mcq_json_lock: WORKER_ID,
      mcq_json_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("mcq_json_lock", null)
    .select("id, mcq_json");

  if (err2) throw err2;

  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  try {
    const raw = await callOpenAI(buildPrompt(row.mcq_json));
    const json = extractJson(raw);

    await supabase
      .from("mcq_hyf_list")
      .update({
        notes_hyf: json,
        mcq_json_lock: null,
        mcq_json_lock_at: null
      })
      .eq("id", row.id);

    return true;

  } catch (err) {
    console.error("❌ Row failed:", row.id);
    console.error(err.message);

    // Release lock on failure
    await supabase
      .from("mcq_hyf_list")
      .update({
        mcq_json_lock: null,
        mcq_json_lock_at: null
      })
      .eq("id", row.id);

    throw err;
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 MCQ HYF WORKER STARTED | ${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`📥 Picked ${claimed.length} rows`);

      for (let i = 0; i < claimed.length; i += BATCH_SIZE) {
        const batch = claimed.slice(i, i + BATCH_SIZE);

        const results = await Promise.allSettled(batch.map(processRow));

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
