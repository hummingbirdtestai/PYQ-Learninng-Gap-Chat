// workers/pgConversationUnicodeWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.NEETPG_CONVERSATION_UNICODE_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.NEETPG_CONVERSATION_UNICODE_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.NEETPG_CONVERSATION_UNICODE_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.NEETPG_CONVERSATION_UNICODE_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.NEETPG_CONVERSATION_UNICODE_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `pgconv-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildMessages(conceptJson) {
  return [
    {
      role: "system",
      content: `
You are a senior NEETPG mentor (30+ yrs exp, mastery of UWorld, First Aid, Amboss, NBME, Marrow, Prepladder).

Output = **strict JSON only**:

{
  "HYFs": [
    {
      "HYF": "High-yield fact (with **bold/italic** for key terms)",
      "MCQs": [
        {
          "id": "UUID",
          "stem": "Clinical vignette (with **bold/italic** on key words)",
          "mcq_key": "mcq_1",
          "options": { "A": "...", "B": "...", "C": "...", "D": "..." },
          "feedback": { 
            "wrong": "❌ why wrong (with **bold/italic** terms)",
            "correct": "✅ why correct (with **bold/italic** terms)" 
          },
          "learning_gap": "Conceptual gap if missed (with **bold/italic** terms)",
          "correct_answer": "B"
        },
        { ... mcq_2 ... },
        { ... mcq_3 ... }
      ]
    }
  ]
}

Rules:
- Exactly 8 HYFs, each with 3 recursive MCQs (mcq_1 → mcq_3).
- Use Unicode for subscripts/superscripts (H₂O, Na⁺, Ca²⁺).
- Apply **bold/italic** markup in HYF, stem, learning_gap, feedback (NOT in options).
- No extra keys or text outside JSON.
`
    },
    { role: "user", content: JSON.stringify(conceptJson || {}) }
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
    .from("mcq_bank")
    .update({ concept_json_lock: null, concept_json_locked_at: null })
    .is("conversation_unicode", null)
    .lt("concept_json_locked_at", cutoff);
}

async function claimRows(limit) {
  await freeStaleLocks();
  const { data: candidates, error } = await supabase
    .from("mcq_bank")
    .select("id, concept_json")
    .not("concept_json", "is", null)
    .is("conversation_unicode", null)
    .is("concept_json_lock", null)
    .order("id", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);
  const { data: locked, error: e2 } = await supabase
    .from("mcq_bank")
    .update({
      concept_json_lock: WORKER_ID,
      concept_json_locked_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("conversation_unicode", null)
    .is("concept_json_lock", null)
    .select("id, concept_json");
  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("mcq_bank")
    .update({ concept_json_lock: null, concept_json_locked_at: null })
    .in("id", ids);
}

// ---------- Process ----------
async function processRow(row) {
  const messages = buildMessages(row.concept_json);
  const raw = await callOpenAI(messages);
  const jsonOut = safeParseJson(raw);

  const { error: upErr } = await supabase
    .from("mcq_bank")
    .update({
      conversation_unicode: jsonOut,
      concept_json_lock: null,
      concept_json_locked_at: null
    })
    .eq("id", row.id);

  if (upErr) {
    const preview = JSON.stringify(jsonOut).slice(0, 200);
    throw new Error(`Update failed for id=${row.id}: ${upErr.message}. Preview: ${preview}`);
  }
  return { updated: 1 };
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
        await clearLocks([chunk[j].id]);
      }
    }
  }
  return updated;
}

// ---------- Main ----------
(async function main() {
  console.log(`🧵 pgConversationUnicodeWorker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);
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
