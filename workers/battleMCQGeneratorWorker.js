// /workers/battleMCQGeneratorWorker_balance_2.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL = process.env.BALANCE_BATTLE_MCQ_MODEL || "gpt-5-mini";
const LIMIT = parseInt(process.env.BALANCE_BATTLE_MCQ_LIMIT || "50", 10);
const LOCK_TTL_MIN = parseInt(process.env.BALANCE_BATTLE_MCQ_LOCK_TTL_MIN || "15", 10);
const WORKER_ID =
  process.env.WORKER_ID ||
  `balance-battle-2-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;

// ─────────────────────────────────────────────
// PROMPT BUILDER
// ─────────────────────────────────────────────
function buildPrompt(subject, topic) {
  return `
You are a **30 Years experienced NEETPG Paper Setter**, creating exam-level questions based on **NEETPG PYQs**, written in **USMLE-style** as seen in **Amboss, UWorld, First Aid, and NBME**.

Create **10 MCQs** that combine **clinical case vignettes** and **single-liner high-yield facts**, covering **the most tested and high-yield points** related to the topic given.
These MCQs should be **NEETPG PYQ-based** and **could appear exactly as-is in the NEETPG Exam**.

**Prompt Rules:**
- Output strictly as a **valid JSON array of 10 objects**.
- Each object must follow this format:
  {
    "Stem": "…",
    "Options": { "A": "…", "B": "…", "C": "…", "D": "…" },
    "Correct Answer": "A|B|C|D"
  }
- “Stem” → The full question stem only (⚠ No labels like “Clinical vignette:” or “Exam tip:”).
- Use **Unicode MarkUp** to highlight **bold**, *italic*, superscripts/subscripts (H₂O, Na⁺, Ca²⁺), and symbols/arrows (±, ↑, ↓, →, ∆).
- **No explanations**, **no commentary**, **no markdown/code fences**.
- Output must be **pure JSON only** (single array [ ... ]).
- If fewer than 10 can be generated due to token limits, still return valid JSON.

**INPUT SUBJECT:** ${subject}
**INPUT TOPIC:** ${topic}
`.trim();
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const isRetryable = (e) =>
  /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(String(e?.message || e));

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages,
      max_completion_tokens: 4000,
    });
    return resp.choices?.[0]?.message?.content?.trim() || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 2) {
      console.warn(`⚠ Retry attempt ${attempt}`);
      await sleep(1000 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    console.error("❌ OpenAI API call failed:", e.message || e);
    throw e;
  }
}

function safeParseJSON(raw) {
  if (!raw || raw.length < 10) return [];
  let cleaned = raw
    .trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```/i, "")
    .replace(/```$/i, "")
    .replace(/,\s*}/g, "}")
    .replace(/,\s*]/g, "]")
    .replace(/}\s*{/g, "}, {");

  if (!cleaned.startsWith("[")) cleaned = "[" + cleaned;
  if (!cleaned.endsWith("]")) cleaned += "]";

  try {
    return JSON.parse(cleaned);
  } catch {
    try {
      const fixed = cleaned.replace(/[^}]*$/, "}]");
      return JSON.parse(fixed);
    } catch {
      console.error("❌ JSON parse error snippet:", cleaned.slice(0, 200));
      return [];
    }
  }
}

// ─────────────────────────────────────────────
// LOCK SYSTEM (for balance_battle_mcqs_2)
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // Unlock expired rows
  await supabase
    .from("balance_battle_mcqs_2")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .lt("mcq_lock_at", cutoff);

  // Find candidates
  const { data: candidates, error: e1 } = await supabase
    .from("balance_battle_mcqs_2")
    .select("id, topic_json, subject, topic")
    .is("mcq_json", null)
    .is("mcq_lock", null)
    .not("topic_json", "is", null)
    .limit(limit);

  if (e1) throw new Error(e1.message);
  if (!candidates?.length) return [];

  const ids = candidates.map((r) => r.id);

  // Lock claimed rows
  const { data: locked, error: e2 } = await supabase
    .from("balance_battle_mcqs_2")
    .update({
      mcq_lock: WORKER_ID,
      mcq_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("mcq_json", null)
    .select("id, topic_json, subject, topic");

  if (e2) throw new Error(e2.message);
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids?.length) return;
  await supabase
    .from("balance_battle_mcqs_2")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const topicJson = row.topic_json || {};
  const subject = topicJson.subject || row.subject;
  const topic = topicJson.topic || row.topic;

  if (!subject || !topic) {
    console.log(`🚫 Skipping row ${row.id} (missing subject/topic)`);
    await clearLocks([row.id]);
    return { updated: 0 };
  }

  console.log(`🎯 Generating MCQs for [${subject}] → [${topic}]`);

  const prompt = buildPrompt(subject, topic);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const allMCQs = safeParseJSON(raw);

  if (!Array.isArray(allMCQs) || allMCQs.length < 3) {
    console.warn(`⚠ Invalid or empty JSON for row ${row.id}`);
    await clearLocks([row.id]);
    return { updated: 0 };
  }

  const { error: e3 } = await supabase
    .from("balance_battle_mcqs_2")
    .update({
      mcq_json: allMCQs,
      mcq_lock: null,
      mcq_lock_at: null,
      updated_at: new Date().toISOString(),
    })
    .eq("id", row.id);

  if (e3) throw new Error(e3.message);
  return { updated: 1 };
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🚀 Balance Battle MCQ Worker (v2) started | model=${MODEL} | limit=${LIMIT}`);
  console.log(`Worker ID: ${WORKER_ID}`);

  const claimed = await claimRows(LIMIT);
  if (!claimed.length) {
    console.log("✅ No rows found — exiting.");
    process.exit(0);
  }

  console.log(`⚙ Processing ${claimed.length} rows...`);
  const results = await Promise.allSettled(claimed.map((r) => processRow(r)));

  const updated = results.filter((r) => r.status === "fulfilled" && r.value.updated).length;
  console.log(`🌀 Done — updated=${updated}/${claimed.length}`);

  process.exit(0);
})();
