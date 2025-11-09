// workers/matchMCQCorrectionWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.MATCH_MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MATCH_MCQ_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.MATCH_MCQ_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.MATCH_MCQ_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.MATCH_MCQ_LOCK_TTL_MIN || "15", 10);
const SUBJECT_FILTER = process.env.MATCH_MCQ_SUBJECT || null;
const WORKER_ID    = process.env.WORKER_ID || `match-mcq-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(mcqJson) {
  return `
You are a **30-year NEET Biology Paper Setter**. The MCQ given may or may not be of the **“Match the Following”** type.

Your tasks:
1. **Identify** whether the MCQ is a “Match the Following” question.
2. If it *is* a Match the Following type and is **missing columns** or **improperly structured**, **recreate it fully and correctly** in JSON format with:
   - "question": full question stem
   - "options": { "A": ..., "B": ..., "C": ..., "D": ... }
   - "correct_answer": correct option or mapping
   - "exam": [list of exams if known]
   - "reference": textbook or source
   - "image_type": true/false
   - "mcq_type": true   ← marks it as a Match the Following type
   - (For Match the Following: clearly show *Column I* and *Column II* pairs)

3. If it is **not** a Match the Following type, return the original MCQ content properly formatted in JSON with `"mcq_type": false`.

**Return the output strictly as a valid JSON object** (no markdown, no explanation).

Input MCQ:
${JSON.stringify(mcqJson, null, 2)}
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
      response_format: { type: "json" },
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

// ---------- Locking ----------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();
  let q = supabase
    .from("biology_raw_new_flattened")
    .update({ match_lock: null, match_lock_at: null })
    .not("match_mcq", "is", null)
    .lt("match_lock_at", cutoff);
  if (SUBJECT_FILTER) q = q.eq("subject_name", SUBJECT_FILTER);
  await q;
}

async function claimRows(limit) {
  await freeStaleLocks();
  let q = supabase
    .from("biology_raw_new_flattened")
    .select("id, match_mcq")
    .not("match_mcq", "is", null)
    .is("match_mcq_corrected", null)
    .is("match_lock", null)
    .order("id", { ascending: true })
    .limit(limit);
  if (SUBJECT_FILTER) q = q.eq("subject_name", SUBJECT_FILTER);

  const { data: candidates, error } = await q;
  if (error) throw error;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);
  const { data: locked, error: e2 } = await supabase
    .from("biology_raw_new_flattened")
    .update({
      match_lock: WORKER_ID,
      match_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("match_lock", null)
    .select("id, match_mcq");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("biology_raw_new_flattened")
    .update({ match_lock: null, match_lock_at: null })
    .in("id", ids);
}

// ---------- Process ----------
async function processRow(row) {
  const prompt = buildPrompt(row.match_mcq);
  const output = await callOpenAI(prompt);

  if (!output || output.length < 20) {
    throw new Error(`Empty or too short output for id=${row.id}`);
  }

  let parsed;
  try {
    parsed = JSON.parse(output);
  } catch (err) {
    throw new Error(`Invalid JSON for id=${row.id}: ${err.message}`);
  }

  const { error: upErr } = await supabase
    .from("biology_raw_new_flattened")
    .update({
      match_mcq_corrected: parsed,
      match_lock: null,
      match_lock_at: null,
    })
    .eq("id", row.id);

  if (upErr) {
    const preview = output.slice(0, 200);
    throw new Error(`Update failed for id=${row.id}: ${upErr.message}. Preview: ${preview}`);
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
        console.error(`   row ${chunk[i].id} error:`, r.reason?.message || r.reason);
        await clearLocks([chunk[i].id]);
      }
    }
  }
  return updated;
}

// ---------- Main ----------
(async function main() {
  console.log(`🧩 Match-MCQ Correction Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }
      console.log(`⚙️ Claimed ${claimed.length} rows`);
      const updated = await processBatch(claimed);
      console.log(`✅ Completed batch: updated=${updated}/${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
