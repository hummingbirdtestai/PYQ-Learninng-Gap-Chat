// workers/unicodeConceptWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.CONCEPT_UNICODE_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CONCEPT_UNICODE_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.CONCEPT_UNICODE_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.CONCEPT_UNICODE_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.CONCEPT_UNICODE_LOCK_TTL_MIN || "15", 10);
const SUBJECT_FILTER = process.env.CONCEPT_UNICODE_SUBJECT || null; // e.g. "Botany"
const WORKER_ID    = process.env.WORKER_ID || `unicode-concept-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt Builder (token-cheap) ----------
function buildPrompt(conceptJson) {
  const compact = JSON.stringify(conceptJson);
  return `
You are a JSON fixer.
Input = JSON (from DB column).

Rules:
1. Keep keys, structure, order, Markdown, emojis exactly.
2. Replace all KaTeX/LaTeX ($…$) with Unicode subscripts/superscripts.
   - Examples: H2O → H₂O, Fe3+ → Fe³⁺, SO4^2− → SO₄²⁻, Na+ → Na⁺, O2− → O₂⁻, 104.5^\\circ → 104.5°.
3. Do NOT leave any $...$ fragments.
4. Do NOT add/remove keys or extra text.
5. Output only valid JSON.

${compact}
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
      temperature: 0,
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
  return JSON.parse(cleaned);
}

// ---------- Locking ----------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();
  const q = supabase
    .from("concepts_vertical")
    .update({ conversation_lock: null, conversation_lock_at: null })
    .is("concept_json_unicode", null)
    .lt("conversation_lock_at", cutoff);
  if (SUBJECT_FILTER) q.eq("subject_name", SUBJECT_FILTER);
  const { error } = await q;
  if (error) throw error;
}

async function claimRows(limit) {
  await freeStaleLocks();

  // Fetch candidates: has concept_json, no unicode yet, contains '$', and unlocked
  let q = supabase
    .from("concepts_vertical")
    .select("vertical_id, concept_json")
    .not("concept_json", "is", null)
    .is("concept_json_unicode", null)
    .is("conversation_lock", null)
    .order("vertical_id", { ascending: true })
    .limit(limit)
    .filter("concept_json", "cs", "$"); // quick 'contains $' via ::text LIKE, use 'cs' (contains) for jsonb->text indexless shortcut

  // If 'cs' on jsonb isn't available in your client, fallback to LIKE:
  // q = q.or(`concept_json.like.%$%`)

  if (SUBJECT_FILTER) q = q.eq("subject_name", SUBJECT_FILTER);

  const { data: candidates, error: e1 } = await q;
  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.vertical_id);

  // Lock them
  const { data: locked, error: e2 } = await supabase
    .from("concepts_vertical")
    .update({
      conversation_lock: WORKER_ID,
      conversation_lock_at: new Date().toISOString(),
    })
    .in("vertical_id", ids)
    .is("concept_json_unicode", null)
    .is("conversation_lock", null)
    .select("vertical_id, concept_json");

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

// ---------- Process one row ----------
async function processRow(row) {
  const prompt = buildPrompt(row.concept_json);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const jsonOut = safeParseObject(raw);

  await supabase
    .from("concepts_vertical")
    .update({ concept_json_unicode: jsonOut, conversation_lock: null, conversation_lock_at: null })
    .eq("vertical_id", row.vertical_id);

  return { updated: 1, total: 1 };
}

// ---------- Batch runner ----------
async function processBatch(rows) {
  const chunks = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    chunks.push(rows.slice(i, i + BATCH_SIZE));
  }

  let updated = 0;
  for (const chunk of chunks) {
    const results = await Promise.allSettled(chunk.map(processRow));
    results.forEach((r, idx) => {
      if (r.status === "fulfilled") {
        updated += r.value.updated;
      } else {
        console.error("Row error:", r.reason?.message || r.reason);
        // release lock on failed row
        clearLocks([chunk[idx].vertical_id]);
      }
    });
  }
  return updated;
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧵 Unicode Concept Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}${SUBJECT_FILTER ? " | subject="+SUBJECT_FILTER : ""}`);

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
