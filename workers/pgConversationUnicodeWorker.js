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
You are a senior NEETPG mentor (30+ yrs exp).
You must output **strict JSON only** with the exact schema below.

Schema:
{
  "HYFs": [
    {
      "HYF": "string (high-yield fact, with **bold/italic** markup for key terms)",
      "MCQs": [
        {
          "id": "UUID",
          "stem": "string (clinical vignette with **bold/italic** key terms)",
          "mcq_key": "mcq_1 | mcq_2",
          "options": { "A": "string", "B": "string", "C": "string", "D": "string" },
          "feedback": { 
            "wrong": "❌ string (with **bold/italic** terms)",
            "correct": "✅ string (with **bold/italic** terms)" 
          },
          "learning_gap": "string (concise conceptual gap with **bold/italic** terms)",
          "correct_answer": "A | B | C | D"
        }
      ]
    }
  ]
}

Rules:
- Always output exactly 4 HYFs.
- Each HYF must have exactly 2 MCQs.
- Every MCQ must include ALL required keys listed above.
- mcq_key must only be "mcq_1" or "mcq_2".
- correct_answer must be one of "A","B","C","D".
- Use valid UUID v4 format for "id".
- Use **Unicode subscripts/superscripts** (H₂O, Na⁺, Ca²⁺).
- Apply **bold/italic** ONLY in HYF, stem, feedback, learning_gap (never in options).
- Output must be valid JSON only, no extra text.
`
    },
    {
      role: "user",
      content: JSON.stringify(conceptJson || {})
    }
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

// ---------- Validator ----------
function validateJson(jsonOut, rowId) {
  const uuidV4Regex =
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

  if (!jsonOut || !jsonOut.HYFs || !Array.isArray(jsonOut.HYFs)) {
    console.error(`❌ Row ${rowId} invalid: HYFs missing or not array`);
    return false;
  }
  if (jsonOut.HYFs.length !== 4) {
    console.error(`❌ Row ${rowId} invalid: HYF count != 4`);
    return false;
  }

  for (const [i, hyf] of jsonOut.HYFs.entries()) {
    if (typeof hyf.HYF !== "string" || !hyf.HYF.trim()) {
      console.error(`❌ Row ${rowId} HYF[${i}] missing or empty HYF`);
      return false;
    }
    if (!hyf.MCQs || !Array.isArray(hyf.MCQs) || hyf.MCQs.length !== 2) {
      console.error(`❌ Row ${rowId} HYF[${i}] invalid MCQ count`);
      return false;
    }

    for (const mcq of hyf.MCQs) {
      if (!mcq.id || !uuidV4Regex.test(mcq.id)) {
        console.error(`❌ Row ${rowId} has invalid UUID: ${mcq.id}`);
        return false;
      }
      if (typeof mcq.stem !== "string" || !mcq.stem.trim()) {
        console.error(`❌ Row ${rowId} MCQ missing/empty stem`);
        return false;
      }
      if (!["mcq_1","mcq_2"].includes(mcq.mcq_key)) {
        console.error(`❌ Row ${rowId} MCQ invalid mcq_key: ${mcq.mcq_key}`);
        return false;
      }
      if (!mcq.options || !mcq.options.A || !mcq.options.B || !mcq.options.C || !mcq.options.D) {
        console.error(`❌ Row ${rowId} MCQ options incomplete`);
        return false;
      }
      if (![mcq.options.A, mcq.options.B, mcq.options.C, mcq.options.D].every(v => typeof v === "string" && v.trim())) {
        console.error(`❌ Row ${rowId} MCQ has empty option text`);
        return false;
      }
      if (!mcq.feedback || !mcq.feedback.wrong || !mcq.feedback.correct) {
        console.error(`❌ Row ${rowId} MCQ feedback missing`);
        return false;
      }
      if (typeof mcq.learning_gap !== "string" || !mcq.learning_gap.trim()) {
        console.error(`❌ Row ${rowId} MCQ missing/empty learning_gap`);
        return false;
      }
      if (!["A","B","C","D"].includes(mcq.correct_answer)) {
        console.error(`❌ Row ${rowId} MCQ invalid correct_answer: ${mcq.correct_answer}`);
        return false;
      }
    }
  }
  return true;
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
    .is("conversation_unicode", null) // only null rows
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

  if (!validateJson(jsonOut, row.id)) {
    console.error(`❌ Validation failed for id=${row.id}, skipping save`);
    await clearLocks([row.id]);
    return { updated: 0 };
  }

  const { error: upErr } = await supabase
    .from("mcq_bank")
    .update({
      conversation_unicode: jsonOut,
      concept_json_lock: null,
      concept_json_locked_at: null
    })
    .eq("id", row.id)
    .is("conversation_unicode", null); // safeguard, only update null rows

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
