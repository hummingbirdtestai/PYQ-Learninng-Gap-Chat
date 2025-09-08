// workers/unicodeConceptWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.CONCEPT_UNICODE_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CONCEPT_UNICODE_LIMIT || "200", 10);
const BATCH_SIZE   = parseInt(process.env.CONCEPT_UNICODE_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.CONCEPT_UNICODE_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.CONCEPT_UNICODE_LOCK_TTL_MIN || "15", 10);
const SUBJECT_FILTER = process.env.CONCEPT_UNICODE_SUBJECT || null;
const WORKER_ID    = process.env.WORKER_ID || `unicode-concept-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt (short) ----------
function buildPrompt(conceptJson) {
  const compact = JSON.stringify(conceptJson);
  return [
    { role: "system", content:
`You are a JSON fixer.
Return ONLY valid JSON (no code fences).` },
    { role: "user", content:
`Input = JSON (from DB column).

Rules:
1) Keep keys, structure, order, Markdown, emojis exactly.
2) Replace all KaTeX/LaTeX ($…$) with Unicode subscripts/superscripts.
   Ex: H2O -> H₂O, Fe3+ -> Fe³⁺, SO4^2− -> SO₄²⁻, Na+ -> Na⁺, O2− -> O₂⁻, 104.5^\\circ -> 104.5°.
3) Do NOT leave any $...$ fragments.
4) Do NOT add/remove keys or any extra text.
5) Output only valid JSON.

${compact}` }
  ];
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(s);
}

// Force JSON output from the model
async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      temperature: 0,
      max_tokens: 2048,
      response_format: { type: "json_object" }, // <- critical
      messages
    });
    return resp.choices?.[0]?.message?.content ?? "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw e;
  }
}

// Strict JSON parser + cleaner
function toJsonObjectOrThrow(raw, ctx = "") {
  const cleaned = raw.trim()
    // strip accidental fences if any slipped through
    .replace(/^```json\s*/i, "")
    .replace(/^```/i, "")
    .replace(/```$/i, "");

  let obj;
  try {
    obj = JSON.parse(cleaned);
  } catch (e) {
    const preview = cleaned.slice(0, 400);
    throw new Error(`Model did not return JSON for ${ctx}. Preview: ${preview}`);
  }
  if (obj == null || typeof obj !== "object" || Array.isArray(obj)) {
    throw new Error(`Model returned non-object JSON for ${ctx}`);
  }
  return obj;
}

// ---------- Locking ----------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();
  let q = supabase
    .from("concepts_vertical")
    .update({ conversation_lock: null, conversation_lock_at: null })
    .is("concept_json_unicode", null)
    .lt("conversation_lock_at", cutoff);
  if (SUBJECT_FILTER) q = q.eq("subject_name", SUBJECT_FILTER);
  const { error } = await q;
  if (error) throw error;
}

async function claimRows(limit) {
  await freeStaleLocks();

  // NOTE: PostgREST can't filter jsonb with ::text LIKE directly in JS client reliably.
  // Create a lightweight view/filter in SQL for efficiency:
  //   CREATE OR REPLACE VIEW v_concepts_needing_unicode AS
  //   SELECT vertical_id, concept_json FROM public.concepts_vertical
  //   WHERE concept_json IS NOT NULL
  //     AND concept_json_unicode IS NULL
  //     AND concept_json::text LIKE '%$%';
  //
  // Then query that view here. If you can't add a view, remove the LIKE filter and let the model run (higher cost).

  let q = supabase
    .from("v_concepts_needing_unicode") // <- use the view
    .select("vertical_id, concept_json")
    .order("vertical_id", { ascending: true })
    .limit(limit);

  if (SUBJECT_FILTER) {
    // If you also included subject_name in the view, filter here; otherwise skip.
  }

  const { data: candidates, error: e1 } = await q;
  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.vertical_id);
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

// ---------- Row processing ----------
async function processRow(row) {
  const messages = buildPrompt(row.concept_json);
  const raw = await callOpenAI(messages);
  const jsonOut = toJsonObjectOrThrow(raw, `vertical_id=${row.vertical_id}`);

  // Extra guard: ensure no remaining '$'
  const hasDollar = JSON.stringify(jsonOut).includes("$");
  if (hasDollar) {
    throw new Error(`Output still contains '$' for vertical_id=${row.vertical_id}`);
  }

  const { error: upErr } = await supabase
    .from("concepts_vertical")
    .update({ concept_json_unicode: jsonOut, conversation_lock: null, conversation_lock_at: null })
    .eq("vertical_id", row.vertical_id);

  if (upErr) {
    // Log a short preview to debug the exact offending JSON
    const preview = JSON.stringify(jsonOut).slice(0, 400);
    throw new Error(`Update failed for vertical_id=${row.vertical_id}: ${upErr.message}. Preview: ${preview}`);
  }

  return { updated: 1, total: 1 };
}

// ---------- Batch ----------
async function processBatch(rows) {
  const chunks = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE) chunks.push(rows.slice(i, i + BATCH_SIZE));

  let updated = 0;
  for (const chunk of chunks) {
    const results = await Promise.allSettled(chunk.map(processRow));
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (r.status === "fulfilled") {
        updated += r.value.updated;
      } else {
        console.error("Row error:", r.reason?.message || r.reason);
        await clearLocks([chunk[i].vertical_id]);
      }
    }
  }
  return updated;
}

// ---------- Main loop ----------
(async function main() {
  console.log(`🧵 Unicode Concept Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);
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
