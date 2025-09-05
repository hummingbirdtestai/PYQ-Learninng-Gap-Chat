// workers/mcqWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ---------- Settings ----------
const MCQ_MODEL        = process.env.MCQ_MODEL || "gpt-5-mini"; 
const MCQ_LIMIT        = parseInt(process.env.MCQ_LIMIT || "100", 10); // claim more
const MCQ_BATCH_SIZE   = parseInt(process.env.MCQ_BATCH_SIZE || "10", 10); // batch per API call
const MCQ_SLEEP_MS     = parseInt(process.env.MCQ_LOOP_SLEEP_MS || "500", 10);
const MCQ_LOCK_TTL_MIN = parseInt(process.env.MCQ_LOCK_TTL_MIN || "15", 10);
const WORKER_ID        = process.env.WORKER_ID || `mcq-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt Builder ----------
function buildPrompt(concepts) {
  const compact = concepts.map(c => JSON.stringify(c.concept_json)).join(",\n");

  return `
You are an **Expert NEET Chemistry Teacher**.  
Given an array of JSON objects, each with a Concept and Explanation, generate **one NEET-standard MCQ** (mcq_1) per concept.

Rules:
- The MCQ must test the **most critical learning gap** (likely confusion).  
- Output must be a **valid JSON array** with exactly ${concepts.length} objects.  
- Each object should have:
  stem, mcq_key ("mcq_1"), options (A–D), correct_answer, feedback.correct, feedback.wrong, learning_gap.  
- stem: NEET-level with **bold buzzwords** and *italics*.  
- options: 4 balanced choices.  
- correct_answer: single uppercase letter.  
- feedback.correct: ✅ 2–3 sentences (praise + mnemonic/tip).  
- feedback.wrong: ❌ 2–3 sentences (explain mistake + correction).  
- learning_gap: one concise sentence.  

INPUT Concepts (array):
[${compact}]
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
      model: MCQ_MODEL,
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

function safeParseArray(raw) {
  const cleaned = raw
    .trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```/i, "")
    .replace(/```$/i, "");

  try {
    const arr = JSON.parse(cleaned);
    return Array.isArray(arr) ? arr : [arr];
  } catch (e) {
    console.error("❌ JSON parse error. Raw snippet:", cleaned.slice(0, 200));
    throw e;
  }
}

// ---------- Locking ----------
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - MCQ_LOCK_TTL_MIN * 60 * 1000).toISOString();

  await supabase
    .from("concepts_vertical")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .is("mcq", null)
    .is("mcq_1", null)
    .lt("mcq_lock_at", cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from("concepts_vertical")
    .select("vertical_id, concept_json")
    .not("concept_json", "is", null)
    .is("mcq", null)
    .is("mcq_1", null)
    .order("vertical_id", { ascending: true })
    .limit(limit);

  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.vertical_id);

  const { data: locked, error: e2 } = await supabase
    .from("concepts_vertical")
    .update({
      mcq_lock: WORKER_ID,
      mcq_lock_at: new Date().toISOString()
    })
    .in("vertical_id", ids)
    .is("mcq", null)
    .is("mcq_1", null)
    .is("mcq_lock", null)
    .select("vertical_id, concept_json");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("concepts_vertical")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .in("vertical_id", ids);
}

// ---------- Process one mini-batch ----------
async function processBatch(batch) {
  const prompt = buildPrompt(batch);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const objs = safeParseArray(raw);

  if (objs.length !== batch.length) {
    throw new Error(`Expected ${batch.length} MCQs, got ${objs.length}`);
  }

  const updates = [];
  for (let i = 0; i < batch.length; i++) {
    const row = batch[i];
    let obj = objs[i];
    if (obj && typeof obj === "object" && !obj.uuid) {
      obj.uuid = uuidv4();
    }
    updates.push({ id: row.vertical_id, data: { mcq_1: obj } });
  }

  for (const u of updates) {
    const { error: upErr } = await supabase
      .from("concepts_vertical")
      .update(u.data)
      .eq("vertical_id", u.id);
    if (upErr) throw upErr;
  }

  await clearLocks(batch.map(r => r.vertical_id));
  return { updated: updates.length, total: batch.length };
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧵 MCQ Worker ${WORKER_ID} | model=${MCQ_MODEL} | claim=${MCQ_LIMIT} | batch=${MCQ_BATCH_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(MCQ_LIMIT);
      if (!claimed.length) {
        await sleep(MCQ_SLEEP_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);

      // break into mini-batches of MCQ_BATCH_SIZE
      const batches = [];
      for (let i = 0; i < claimed.length; i += MCQ_BATCH_SIZE) {
        batches.push(claimed.slice(i, i + MCQ_BATCH_SIZE));
      }

      // process all mini-batches in parallel
      const results = await Promise.allSettled(batches.map(b => processBatch(b)));

      let updated = 0;
      results.forEach((r, idx) => {
        if (r.status === "fulfilled") {
          console.log(`   batch ${idx + 1}: updated ${r.value.updated}/${r.value.total}`);
          updated += r.value.updated;
        } else {
          console.error(`   batch ${idx + 1} error:`, r.reason.message || r.reason);
          clearLocks(batches[idx].map(r => r.vertical_id));
        }
      });

      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
