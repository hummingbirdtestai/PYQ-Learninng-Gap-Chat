// workers/conceptJsonWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ---------- Settings ----------
const MODEL        = process.env.CONCEPT_JSON_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CONCEPT_JSON_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.CONCEPT_JSON_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.CONCEPT_JSON_LOOP_SLEEP_MS || "1000", 10);
const LOCK_TTL_MIN = parseInt(process.env.CONCEPT_JSON_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `concept-json-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(mcqText) {
  return `
You are a senior NEETPG mentor (30+ yrs, mastery of UWorld/FirstAid/Amboss/Marrow).
Input: Single MCQ.

Task: Output only strict JSON in format:
{ "Concept": "...", "Explanation": "..." }

Rules:
- Be *very specific* and exam-oriented.
- Use markup *bold/italic* for key words in concept and Explanation.
- Use proper Unicode for subscripts/superscripts (H₂O, Na⁺, Ca²⁺).
- In "Explanation":
  • Define the core concept clearly.
  • Add 5–6 High-Yield Facts repeatedly tested in NEETPG/INICET/FMGE (specific to the concept).
- Do NOT explicitly state the “correct option/answer”.
- Output must be *only JSON*, no extra notes or text.

MCQ:
${mcqText}
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
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "text" }
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(500 * attempt);
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
    .from("mcq_bank")
    .update({ concept_json_lock: null, concept_json_locked_at: null })
    .is("concept_json", null)
    .lt("concept_json_locked_at", cutoff);
}

async function claimRows(limit) {
  await freeStaleLocks();

  const { data: candidates, error } = await supabase
    .from("mcq_bank")
    .select("id, mcq")
    .not("mcq", "is", null)
    .is("concept_json", null)
    .is("concept_json_lock", null)
    .order("created_at", { ascending: true })
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
    .is("concept_json", null)
    .is("concept_json_lock", null)
    .select("id, mcq");

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
  const prompt = buildPrompt(row.mcq);
  const raw = await callOpenAI(prompt);
  const parsed = safeParseJson(raw);

  // ✅ Always enforce UUID + schema
  const withUuid = {
    uuid: uuidv4(),
    Concept: parsed.Concept || "",
    Explanation: parsed.Explanation || ""
  };

  // ✅ Validation
  if (!withUuid.Concept || !withUuid.Explanation) {
    throw new Error(`Invalid JSON output for id=${row.id}: Missing Concept/Explanation`);
  }

  const { error: upErr } = await supabase
    .from("mcq_bank")
    .update({
      concept_json: withUuid,
      concept_json_lock: null,
      concept_json_locked_at: null
    })
    .eq("id", row.id);

  if (upErr) {
    throw new Error(`Update failed for id=${row.id}: ${upErr.message}`);
  }
  return { updated: 1, total: 1 };
}

// ---------- Batch ----------
async function processBatch(rows) {
  let updated = 0;
  for (const row of rows) {
    try {
      const r = await processRow(row);
      updated += r.updated;
    } catch (e) {
      console.error(`   row id=${row.id} error:`, e.message || e);
      await clearLocks([row.id]);
    }
  }
  return updated;
}

// ---------- Main ----------
(async function main() {
  console.log(`🧵 Concept JSON Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);
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
      await sleep(2000);
    }
  }
})();
