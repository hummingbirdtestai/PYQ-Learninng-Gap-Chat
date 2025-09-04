// workers/mcqWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ---------- Settings ----------
const MCQ_MODEL        = process.env.MCQ_MODEL || "gpt-5-mini"; // use mini model
const MCQ_LIMIT        = parseInt(process.env.MCQ_LIMIT || "50", 10);
const MCQ_BLOCK_SIZE   = parseInt(process.env.MCQ_BLOCK_SIZE || "10", 10);
const MCQ_SLEEP_MS     = parseInt(process.env.MCQ_LOOP_SLEEP_MS || "800", 10);
const MCQ_LOCK_TTL_MIN = parseInt(process.env.MCQ_LOCK_TTL_MIN || "15", 10);
const WORKER_ID        = process.env.WORKER_ID || `mcq-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt Builder ----------
function buildPrompt(conceptJson) {
  const raw = JSON.stringify(conceptJson, null, 2);

  return `
You are an Expert NEET Chemistry Teacher.  
Given a JSON with a Concept and Explanation, identify the most **critical learning gap** that a NEET student may face, and create **one NEET-standard Chemistry MCQ** (mcq_1) targeting that gap.

Rules:
- Use the Concept + Explanation as the base.  
- The MCQ must reveal the most likely confusion or misconception (critical learning gap).  
- Output strictly in JSON with keys:
  stem, mcq_key ("mcq_1"), options (A–D), correct_answer, feedback.correct, feedback.wrong, learning_gap.  

Formatting:
- stem: NEET-level with **bold buzzwords** and *italics*.  
- options: 4 balanced choices.  
- correct_answer: single uppercase letter.  
- feedback.correct: ✅ 3–5 sentences (praise + mnemonic/tip).  
- feedback.wrong: ❌ 3–5 sentences (explain mistake + correction).  
- learning_gap: concise description of misconception.  

INPUT Concept JSON:  
${raw}
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

function safeParseJSON(raw) {
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
    console.error("❌ JSON parse error. Raw snippet:", cleaned.slice(0, 250));
    throw e;
  }
}

// ---------- Locking ----------
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - MCQ_LOCK_TTL_MIN * 60 * 1000).toISOString();

  // free stale locks on rows where mcq IS NULL
  await supabase
    .from("concepts_vertical")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .is("mcq", null)
    .lt("mcq_lock_at", cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from("concepts_vertical")
    .select("vertical_id, concept_json")
    .not("concept_json", "is", null)
    .is("mcq", null)
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

// ---------- Process one block ----------
async function processBlock(block) {
  const updates = [];

  for (const [i, row] of block.entries()) {
    try {
      console.log(`📝 [${i + 1}/${block.length}] Processing row ${row.vertical_id}`);

      const prompt = buildPrompt(row.concept_json);
      console.log(`   🔑 Prompt built for row ${row.vertical_id}`);

      const raw = await callOpenAI([{ role: "user", content: prompt }]);
      console.log(`   📥 OpenAI returned output for row ${row.vertical_id} (length=${raw.length})`);

      const obj = safeParseJSON(raw);
      console.log(`   ✅ JSON parsed successfully for row ${row.vertical_id}`);

      // attach UUID if missing
      if (obj && typeof obj === "object" && !obj.uuid) {
        obj.uuid = uuidv4();
      }

      // 🔑 Save into mcq_1 column (not mcq)
      updates.push({ id: row.vertical_id, data: { mcq_1: obj } });
    } catch (e) {
      console.error(`❌ Error processing row ${row.vertical_id}:`, e.message || e);
      await clearLocks([row.vertical_id]);
    }
  }

  for (const u of updates) {
    console.log(`   💾 Writing MCQ_1 back to Supabase for row ${u.id}`);
    const { error: upErr } = await supabase
      .from("concepts_vertical")
      .update(u.data)
      .eq("vertical_id", u.id);
    if (upErr) throw upErr;
    console.log(`   📌 Row ${u.id} updated successfully`);
  }

  await clearLocks(block.map(r => r.vertical_id));
  return { updated: updates.length, total: block.length };
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧵 MCQ Worker ${WORKER_ID} | model=${MCQ_MODEL} | claim=${MCQ_LIMIT} | block=${MCQ_BLOCK_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(MCQ_LIMIT);
      if (!claimed.length) {
        await sleep(MCQ_SLEEP_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);
      let updated = 0;
      for (let i = 0; i < claimed.length; i += MCQ_BLOCK_SIZE) {
        const block = claimed.slice(i, i + MCQ_BLOCK_SIZE);
        try {
          const r = await processBlock(block);
          updated += r.updated;
          console.log(`   block ${i / MCQ_BLOCK_SIZE + 1}: updated ${r.updated}/${r.total}`);
        } catch (e) {
          console.error("   block error:", e.message || e);
          await clearLocks(block.map(r => r.vertical_id));
        }
      }

      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
