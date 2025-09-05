// workers/mcq2Worker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ---------- Settings ----------
const MCQ_MODEL        = process.env.MCQ_MODEL || "gpt-5-mini";
const MCQ_LIMIT        = parseInt(process.env.MCQ_LIMIT || "100", 10);
const MCQ_BATCH_SIZE   = parseInt(process.env.MCQ_BATCH_SIZE || "10", 10);
const MCQ_SLEEP_MS     = parseInt(process.env.MCQ_LOOP_SLEEP_MS || "500", 10);
const MCQ_LOCK_TTL_MIN = parseInt(process.env.MCQ_LOCK_TTL_MIN || "15", 10);
const WORKER_ID        = process.env.WORKER_ID || `mcq2-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt Builder ----------
function buildPrompt(items) {
  const compact = items.map(c => c.lg_1_text).join("\n\n");

  return `
You are an **Expert NEET Teacher** Chemistry.  
Given an array of short learning gap texts, generate **one NEET-standard MCQ** (mcq_2) for each.  

Rules:
- The MCQ must be strictly based on the learning gap in the given text.  
- Output must be a **valid JSON array** with exactly ${items.length} objects.  
- Each object should have:
  stem, mcq_key ("mcq_2"), options (A–D), correct_answer, feedback.correct, feedback.wrong, learning_gap.  

Formatting:
- Use **Markdown** for emphasis (**bold**, *italic*).  
- Use **KaTeX/LaTeX inside $...$** for all formulas, subscripts, superscripts, roots, sigma, integrals, and chemical notations.  
  Examples:  
    - H2O → $H_2O$  
    - SO4^2- → $SO_4^{2-}$  
    - F = ma → $F = ma$  
    - sqrt(2gh) → $\\sqrt{2gh}$  
    - Summation → $\\sum_{i=1}^{n} i$  
    - Integral → $\\int_0^\\infty e^{-x} dx$

- stem: NEET-level question.  
- options: 4 balanced choices.  
- correct_answer: single uppercase letter.  
- feedback.correct: ✅ 2–3 sentences (praise + mnemonic/tip).  
- feedback.wrong: ❌ 2–3 sentences (explain mistake + correction).  
- learning_gap should identify recursively the route cause of confusion that led to answer this MCQ wrong because of the gaps in  concepts in Class VII to Class X.

INPUT Learning Gaps:
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
    .is("mcq_2", null)
    .lt("mcq_lock_at", cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from("concepts_vertical")
    .select("vertical_id, lg_1_text")
    .not("lg_1_text", "is", null)
    .is("mcq_2", null)
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
    .is("mcq_2", null)
    .is("mcq_lock", null)
    .select("vertical_id, lg_1_text");

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
    updates.push({ id: row.vertical_id, data: { mcq_2: obj } });
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
  console.log(`🧵 MCQ2 Worker ${WORKER_ID} | model=${MCQ_MODEL} | claim=${MCQ_LIMIT} | batch=${MCQ_BATCH_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(MCQ_LIMIT);
      if (!claimed.length) {
        await sleep(MCQ_SLEEP_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);

      const batches = [];
      for (let i = 0; i < claimed.length; i += MCQ_BATCH_SIZE) {
        batches.push(claimed.slice(i, i + MCQ_BATCH_SIZE));
      }

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
