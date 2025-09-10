// workers/fixMissingCorrectAnswerWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.CORRECT_JSON_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CORRECT_JSON_LIMIT || "200", 10);
const BATCH_SIZE   = parseInt(process.env.CORRECT_JSON_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.CORRECT_JSON_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.CORRECT_JSON_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `fix-mcq-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(conversationJson) {
  const compact = JSON.stringify(conversationJson);
  return `
You are a JSON fixer. Input is a JSON with key "HYFs", each having an array "MCQs".

Rules:
- For each MCQ: if "correct_answer" is missing, add it.
- Derive correct answer (A/B/C/D) from options + feedback.
- If "correct_answer" exists, keep it.
- Do not change any other content.
- Output valid JSON only.

Fix this JSON:
${compact}
`.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(s);
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      max_completion_tokens: 2000,
      messages: [{ role: "user", content: prompt }],
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

function safeParseJson(raw) {
  const cleaned = raw.trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```/i, "")
    .replace(/```$/i, "");
  return JSON.parse(cleaned);
}

// ---------- Locking ----------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();
  await supabase
    .from("concepts_vertical")
    .update({ conversation_lock: null, conversation_lock_at: null })
    .eq("missing_correct_answer", true)
    .is("correct_jsons", null)
    .lt("conversation_lock_at", cutoff);
}

async function claimRows(limit) {
  await freeStaleLocks();

  const { data: candidates, error } = await supabase
    .from("concepts_vertical")
    .select("vertical_id, conversation_unicode")
    .eq("missing_correct_answer", true)
    .is("correct_jsons", null)
    .is("conversation_lock", null)
    .order("vertical_id", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.vertical_id);
  const { data: locked, error: e2 } = await supabase
    .from("concepts_vertical")
    .update({
      conversation_lock: WORKER_ID,
      conversation_lock_at: new Date().toISOString(),
    })
    .in("vertical_id", ids)
    .eq("missing_correct_answer", true)
    .is("correct_jsons", null)
    .is("conversation_lock", null)
    .select("vertical_id, conversation_unicode");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("concepts_vertical")
    .update({ conversation_lock: null, conversation_lock_at: null })
    .in("vertical_id", ids);
}

// ---------- Process ----------
async function processRow(row) {
  const prompt = buildPrompt(row.conversation_unicode);
  const raw = await callOpenAI(prompt);
  const jsonOut = safeParseJson(raw);

  const { error: upErr } = await supabase
    .from("concepts_vertical")
    .update({
      correct_jsons: jsonOut,
      conversation_lock: null,
      conversation_lock_at: null
    })
    .eq("vertical_id", row.vertical_id);

  if (upErr) {
    const preview = JSON.stringify(jsonOut).slice(0, 200);
    throw new Error(`Update failed for vertical_id=${row.vertical_id}: ${upErr.message}. Preview: ${preview}`);
  }
  return { updated: 1, total: 1 };
}

// ---------- Main ----------
(async function main() {
  console.log(`🧵 Fix Missing Correct Answer Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }
      console.log(`⚙️ claimed=${claimed.length}`);
      const results = await Promise.allSettled(claimed.map(processRow));
      let updated = 0;
      results.forEach((r, idx) => {
        if (r.status === "fulfilled") updated += r.value.updated;
        else {
          console.error(`   row ${idx + 1} error:`, r.reason?.message || r.reason);
          clearLocks([claimed[idx].vertical_id]);
        }
      });
      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
