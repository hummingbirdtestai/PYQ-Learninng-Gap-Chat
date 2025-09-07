require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ---------- Settings ----------
const MODEL        = process.env.CONCEPT_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CONCEPT_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.CONCEPT_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.CONCEPT_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.CONCEPT_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `allconcepts-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt Builder ----------
function buildPrompt(conceptRaw) {
  return `
Reformat input into JSON array → [{ "Concept": "...", "Explanation": "..." }].
Preserve all content & markup exactly. No extra keys, no changes, only valid JSON.

INPUT:
${conceptRaw}
`.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(s);
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages,
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

function safeParseObject(raw) {
  const cleaned = raw
    .trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```/i, "")
    .replace(/```$/i, "")
    .replace(/,\s*}/g, "}")
    .replace(/,\s*]/g, "]");

  try {
    return JSON.parse(cleaned);
  } catch (e) {
    console.error("❌ JSON parse error. Raw snippet:", cleaned.slice(0, 200));
    throw e;
  }
}

// ---------- Locking ----------
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // free stale locks
  await supabase
    .from("concept_bank")
    .update({ all_concepts_json_lock: null, all_concepts_json_lock_at: null })
    .is("all_concepts_json", null)
    .lt("all_concepts_json_lock_at", cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from("concept_bank")
    .select("id, concept_raw")
    .is("all_concepts_json", null)
    .is("all_concepts_json_lock", null)
    .order("id", { ascending: true })
    .limit(limit);

  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  const { data: locked, error: e2 } = await supabase
    .from("concept_bank")
    .update({
      all_concepts_json_lock: WORKER_ID,
      all_concepts_json_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("all_concepts_json", null)
    .is("all_concepts_json_lock", null)
    .select("id, concept_raw");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("concept_bank")
    .update({ all_concepts_json_lock: null, all_concepts_json_lock_at: null })
    .in("id", ids);
}

// ---------- Process one row ----------
async function processRow(row) {
  const prompt = buildPrompt(row.concept_raw);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const parsed = safeParseObject(raw);

  // Attach uuid for traceability
  const final = { uuid: uuidv4(), concepts: parsed };

  await supabase
    .from("concept_bank")
    .update({ all_concepts_json: final })
    .eq("id", row.id);

  await clearLocks([row.id]);
  return { updated: 1, total: 1 };
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧵 AllConcepts Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);

      const results = await Promise.allSettled(claimed.map(row => processRow(row)));

      let updated = 0;
      results.forEach((r, idx) => {
        if (r.status === "fulfilled") {
          console.log(`   row ${idx + 1}: updated ${r.value.updated}/${r.value.total}`);
          updated += r.value.updated;
        } else {
          console.error(`   row ${idx + 1} error:`, r.reason.message || r.reason);
          clearLocks([claimed[idx].id]);
        }
      });

      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
