// workers/unicodeMcqWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.MCQ_UNICODE_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_UNICODE_LIMIT || "200", 10);
const BATCH_SIZE   = parseInt(process.env.MCQ_UNICODE_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.MCQ_UNICODE_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_UNICODE_LOCK_TTL_MIN || "15", 10);
const SUBJECT_FILTER = process.env.MCQ_UNICODE_SUBJECT || null;
const WORKER_ID    = process.env.WORKER_ID || `unicode-mcq-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(mcqJson) {
  return `
  Fix JSON by replacing all LaTeX ($…$) with Unicode subscripts/superscripts.
  Rules:
  - Keep keys, order, Markdown, emojis exactly.
  - Replace: H2O→H₂O, Fe3+→Fe³⁺, SO4^2−→SO₄²⁻, Na+→Na⁺, O2−→O₂⁻, ^\\circ→°.
  - Do not add/remove keys.
  - Output only valid JSON.
  
  Input:
  ${JSON.stringify(mcqJson)}
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
      response_format: { type: "text" }, // allow arrays or objects
      messages: [{ role: "user", content: prompt }]
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
  let q = supabase
    .from("concepts_vertical")
    .update({ mcq_1_6_lock: null, mcq_1_6_lock_at: null })
    .is("mcq_1_6_unicode", null)
    .lt("mcq_1_6_lock_at", cutoff);

  if (SUBJECT_FILTER) q = q.eq("subject_name", SUBJECT_FILTER);
  await q;
}

async function claimRows(limit) {
  await freeStaleLocks();

  let q = supabase
    .from("concepts_vertical")
    .select("vertical_id, mcq_3_6_json")
    .not("mcq_3_6_json", "is", null)
    .is("mcq_1_6_unicode", null)
    .is("mcq_1_6_lock", null)
    .order("vertical_id", { ascending: true })
    .limit(limit);

  if (SUBJECT_FILTER) q = q.eq("subject_name", SUBJECT_FILTER);

  const { data: candidates, error } = await q;
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
    .select("vertical_id, mcq_3_6_json");

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
  const prompt = buildPrompt(row.mcq_3_6_json);
  const raw = await callOpenAI(prompt);
  const jsonOut = safeParseJson(raw);

  if (JSON.stringify(jsonOut).includes("$")) {
    throw new Error(`Output still contains '$' for vertical_id=${row.vertical_id}`);
  }

  const { error: upErr } = await supabase
    .from("concepts_vertical")
    .update({
      mcq_1_6_unicode: jsonOut,
      mcq_1_6_lock: null,
      mcq_1_6_lock_at: null
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
  console.log(`🧵 Unicode MCQ Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);
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
