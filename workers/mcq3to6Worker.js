// workers/mcq3to6Worker.js
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
const WORKER_ID        = process.env.WORKER_ID || `mcq3to6-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt Builder ----------
function buildPrompt(items) {
  const compact = items.map(c => c.lg_2_text).join("\n\n");

  return `
You are an **Expert NEET Chemistry Teacher**.  
Given one learning gap text, generate exactly 4 **recursive MCQs** (mcq_3–mcq_6).  

- Each MCQ must go deeper than the previous, tracing confusion back to **Class VII–X NCERT basics**.  
- Output = strict **JSON array of 4 objects**.  
- Each object must include keys:  
  stem, mcq_key ("mcq_3"…"mcq_6"), options (A–D), correct_answer (A–D),  
  feedback.correct (✅ 2–3 sentences with praise + mnemonic/tip),  
  feedback.wrong (❌ 2–3 sentences explaining mistake + correction),  
  learning_gap (concise description of the misconception tested **and** the fundamental concept that should be tested in the *next* MCQ).  
- Use **Markdown** for emphasis (**bold**, *italic*).  
- Use **KaTeX/LaTeX ($…$)** for chemistry formulas, subscripts, superscripts, charges, and thermodynamics.  

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
    .replace(/```$/i, "")
    .replace(/,\s*}/g, "}")
    .replace(/,\s*]/g, "]");

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
    .is("mcq_3_6_json", null)
    .lt("mcq_lock_at", cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from("concepts_vertical")
    .select("vertical_id, lg_2_text")
    .not("lg_2_text", "is", null)
    .is("mcq_3_6_json", null)
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
    .is("mcq_3_6_json", null)
    .is("mcq_lock", null)
    .select("vertical_id, lg_2_text");

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

  if (objs.length !== 4) {
    throw new Error(`Expected 4 MCQs, got ${objs.length}`);
  }

  // assign UUID to each MCQ
  objs.forEach(mcq => {
    if (!mcq.uuid) mcq.uuid = uuidv4();
  });

  const row = batch[0]; // only one lg_2_text per row
  await supabase
    .from("concepts_vertical")
    .update({ mcq_3_6_json: objs })
    .eq("vertical_id", row.vertical_id);

  await clearLocks([row.vertical_id]);
  return { updated: 1, total: 1 };
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧵 MCQ3–6 Worker ${WORKER_ID} | model=${MCQ_MODEL} | claim=${MCQ_LIMIT} | batch=${MCQ_BATCH_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(MCQ_LIMIT);
      if (!claimed.length) {
        await sleep(MCQ_SLEEP_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);

      // process each row individually (1 lg_2_text = 1 set of 4 MCQs)
      const results = await Promise.allSettled(claimed.map(row => processBatch([row])));

      let updated = 0;
      results.forEach((r, idx) => {
        if (r.status === "fulfilled") {
          console.log(`   row ${idx + 1}: updated ${r.value.updated}/${r.value.total}`);
          updated += r.value.updated;
        } else {
          console.error(`   row ${idx + 1} error:`, r.reason.message || r.reason);
          clearLocks([claimed[idx].vertical_id]);
        }
      });

      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
