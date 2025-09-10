// workers/zoologyUnicodeWorkerConversation.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.ZOOLOGY_CONVERSATION_UNICODE_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.ZOOLOGY_CONVERSATION_UNICODE_LIMIT || "200", 10);
const BATCH_SIZE   = parseInt(process.env.ZOOLOGY_CONVERSATION_UNICODE_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.ZOOLOGY_CONVERSATION_UNICODE_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.ZOOLOGY_CONVERSATION_UNICODE_LOCK_TTL_MIN || "15", 10);
const SUBJECT_FILTER = "Zoology"; // 🔒 hard-locked
const WORKER_ID    = process.env.WORKER_ID || `zoologyunicodeworkerconversation-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildMessages(conceptJson) {
  const { Concept, Explanation } = conceptJson || {};
  return [
    {
      role: "system",
      content: `
You are a senior 30+ yrs exp NEET Zoology Teacher.
Always return **strict JSON only** in this schema:

{
  "HYFs": [
    {
      "HYF": "Fact statement",
      "MCQs": [
        {
          "id": "UUID",
          "stem": "Question stem",
          "mcq_key": "mcq_1",
          "options": { "A": "...", "B": "...", "C": "...", "D": "..." },
          "feedback": { "wrong": "❌ why wrong", "correct": "✅ why correct" },
          "learning_gap": "Conceptual gap if missed",
          "correct_answer": "A"
        },
        { ... mcq_2 ... },
        { ... mcq_3 ... }
      ]
    }
  ]
}

Rules:
- Exactly 8 HYFs (HYF1–HYF8).
- Each HYF has 3 recursive MCQs (mcq_1 → mcq_3).
- Apply **bold/italic** markup to *key words* in HYF, stem, learning_gap, feedback (NOT inside options).
- Use Unicode subscripts/superscripts (H₂O, Na⁺, Ca²⁺).
- Do NOT include "uuid", "Concept", or "Explanation" in output.
- No extra keys, no text outside JSON.
`
    },
    { role: "user", content: JSON.stringify({ Concept, Explanation }) }
  ];
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function isRetryable(e) {
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(String(e?.message || e));
}
async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      response_format: { type: "json_object" },
      messages
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(messages, attempt + 1);
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
    .update({ conversation_unicode_lock: null, conversation_unicode_lock_at: null })
    .is("conversation_unicode", null)
    .lt("conversation_unicode_lock_at", cutoff)
    .eq("subject_name", SUBJECT_FILTER);
}

async function claimRows(limit) {
  await freeStaleLocks();
  const { data: candidates, error } = await supabase
    .from("concepts_vertical")
    .select("vertical_id, concept_json_unicode")
    .not("concept_json_unicode", "is", null)
    .is("conversation_unicode", null)
    .is("conversation_unicode_lock", null)
    .eq("subject_name", SUBJECT_FILTER)
    .order("vertical_id", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.vertical_id);
  const { data: locked, error: e2 } = await supabase
    .from("concepts_vertical")
    .update({
      conversation_unicode_lock: WORKER_ID,
      conversation_unicode_lock_at: new Date().toISOString(),
    })
    .in("vertical_id", ids)
    .is("conversation_unicode", null)
    .is("conversation_unicode_lock", null)
    .select("vertical_id, concept_json_unicode");
  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("concepts_vertical")
    .update({ conversation_unicode_lock: null, conversation_unicode_lock_at: null })
    .in("vertical_id", ids)
    .eq("subject_name", SUBJECT_FILTER);
}

// ---------- Process ----------
async function processRow(row) {
  const messages = buildMessages(row.concept_json_unicode);
  const raw = await callOpenAI(messages);
  const jsonOut = safeParseJson(raw);

  const { error: upErr } = await supabase
    .from("concepts_vertical")
    .update({
      conversation_unicode: jsonOut,
      conversation_unicode_lock: null,
      conversation_unicode_lock_at: null
    })
    .eq("vertical_id", row.vertical_id)
    .eq("subject_name", SUBJECT_FILTER);

  if (upErr) {
    const preview = JSON.stringify(jsonOut).slice(0, 200);
    throw new Error(`Update failed for vertical_id=${row.vertical_id}: ${upErr.message}. Preview: ${preview}`);
  }
  return { updated: 1, total: 1 };
}

// ---------- Batch ----------
async function processBatch(rows) {
  let updated = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const chunk = rows.slice(i, i + BATCH_SIZE);
    const results = await Promise.allSettled(chunk.map(processRow));
    for (let j = 0; j < results.length; j++) {
      const r = results[j];
      if (r.status === "fulfilled") {
        updated += r.value.updated;
      } else {
        console.error(`   row ${j + 1} error:`, r.reason?.message || r.reason);
        await clearLocks([chunk[j].vertical_id]);
      }
    }
  }
  return updated;
}

// ---------- Main ----------
(async function main() {
  console.log(`🧵 Zoology Unicode Conversation Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }
      console.log(`⚙️ claimed=${claimed.length}`);
      const updated = await processBatch(claimed);
      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
