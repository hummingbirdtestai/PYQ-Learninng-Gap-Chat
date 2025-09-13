// workers/botanyMcqUnicodeWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.BOTANY_MCQ_UNICODE_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.BOTANY_MCQ_UNICODE_LIMIT || "200", 10);
const BATCH_SIZE   = parseInt(process.env.BOTANY_MCQ_UNICODE_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.BOTANY_MCQ_UNICODE_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.BOTANY_MCQ_UNICODE_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `botany-mcq-unicode-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(conceptJson) {
  return `
You are an expert NEET Botany teacher.  
Input = JSON with { "Concept": "...", "Explanation": "..." }.

Task: Generate 6 recursive MCQs (mcq_1–mcq_6) that probe progressively deeper, linking Class XI–XII concepts to Class VII–X basics.

Output = strict JSON array of objects. Each object must have:

- stem  
- mcq_key ("mcq_1"…"mcq_6")  
- options (A–D)  
- correct_answer (A–D)  
- feedback.correct (✅ 2–3 sentences with praise + mnemonic/tip)  
- feedback.wrong (❌ 2–3 sentences correcting misconception)  
- learning_gap (concise description of misconception + which basic concept to test next)

Formatting rules:
- Use Markdown for emphasis (bold, italic).  
- Use Unicode for formulas, subscripts, superscripts, charges, symbols.  
- Keep stems exam-style, feedback student-friendly.

Input:
${JSON.stringify(conceptJson)}
  `.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(s);
}

// check which models allow temperature
function modelSupportsTemperature(model) {
  return /^gpt-4/.test(model) || /4o-mini/.test(model);
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const params = {
      model: MODEL,
      messages: [{ role: "user", content: prompt }],
    };
    if (modelSupportsTemperature(MODEL)) {
      params.temperature = 0.7;
    }
    const resp = await openai.chat.completions.create(params);
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
    .update({ mcq_1_6_lock: null, mcq_1_6_lock_at: null })
    .is("mcq_1_6_unicode", null)
    .lt("mcq_1_6_lock_at", cutoff)
    .eq("subject_name", "Botany");
}

async function claimRows(limit) {
  await freeStaleLocks();

  const { data: candidates, error } = await supabase
    .from("concepts_vertical")
    .select("vertical_id, concept_json_unicode")
    .eq("subject_name", "Botany")
    .not("concept_json_unicode", "is", null)
    .is("mcq_1_6_unicode", null)
    .is("mcq_1_6_lock", null)
    .order("vertical_id", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.vertical_id);
  const { data: locked, error: e2 } = await supabase
    .from("concepts_vertical")
    .update({
      mcq_1_6_lock: WORKER_ID,
      mcq_1_6_lock_at: new Date().toISOString(),
    })
    .in("vertical_id", ids)
    .is("mcq_1_6_unicode", null)
    .is("mcq_1_6_lock", null)
    .select("vertical_id, concept_json_unicode");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("concepts_vertical")
    .update({ mcq_1_6_lock: null, mcq_1_6_lock_at: null })
    .in("vertical_id", ids);
}

// ---------- Process ----------
async function processRow(row) {
  const prompt = buildPrompt(row.concept_json_unicode);
  const raw = await callOpenAI(prompt);
  const jsonOut = safeParseJson(raw);

  if (!Array.isArray(jsonOut) || jsonOut.length !== 6) {
    throw new Error(`Invalid MCQ output for vertical_id=${row.vertical_id}`);
  }

  const { error: upErr } = await supabase
    .from("concepts_vertical")
    .update({
      mcq_1_6_unicode: jsonOut,
      mcq_1_6_lock: null,
      mcq_1_6_lock_at: null,
    })
    .eq("vertical_id", row.vertical_id);

  if (upErr) {
    const preview = JSON.stringify(jsonOut).slice(0, 200);
    throw new Error(`Update failed for vertical_id=${row.vertical_id}: ${upErr.message}. Preview: ${preview}`);
  }
  return { updated: 1, total: 1 };
}

// ---------- Batch ----------
async function processBatch(rows) {
  const chunks = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    chunks.push(rows.slice(i, i + BATCH_SIZE));
  }

  let updated = 0;
  for (const chunk of chunks) {
    const results = await Promise.allSettled(chunk.map(processRow));
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (r.status === "fulfilled") {
        updated += r.value.updated;
      } else {
        console.error(`   row ${i + 1} error:`, r.reason?.message || r.reason);
        await clearLocks([chunk[i].vertical_id]);
      }
    }
  }
  return updated;
}

// ---------- Main ----------
(async function main() {
  console.log(`🌿 Botany MCQ Unicode Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);
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
