// workers/mcqWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ---------- Settings ----------
const MCQ_MODEL        = process.env.MCQ_MODEL || "gpt-5";
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
You are given the JSON of Concept and Explanation. 
Based on this Concept and Explanation, create 6 NEET Chemistry MCQs.  

Create them of NEET Exam standard.  

The first MCQ, called mcq_1, will be based on the Concept and Explanation.  
For mcq_1, add a learning_gap on what is the confusing point or underlying conceptual deficit due to which that mcq_1 will be answered wrong by a NEET Chemistry preparing student.  

Next, mcq_2 should be recursively based on the learning_gap of mcq_1.  
Every MCQ created will be recursively based to detect deeper levels of learning gaps.  
The same rule follows when creating mcq_2 based on the learning_gap of mcq_1 and so on until mcq_6.  

🎯 The purpose of recursive MCQs is to dig deeper to know whether the connected concepts are missing that are taught in Class X, Class IX, Class VIII, and Class VII in NCERT Chemistry book.  

Always generate exactly 6 levels of learning gaps.  
- Level 1 = exact confusion of the Concept and Explanation  
- Levels 2–6 = progressively deeper, easier, fundamental gaps, each logically explaining the previous one, based on the concepts discussed in Class X, IX, VIII, and VII in NCERT Chemistry book  

Mention which MCQ it is (mcq_1 … mcq_6) on mcq_key.  

Each MCQ must contain the following keys:  
- stem  
- mcq_key  
- options  
- feedback.wrong  
- feedback.correct  
- learning_gap  
- correct_answer  

🚨 Uncompromising MCQ Rules (must follow verbatim):  
- Clear stem of MCQ of NEET actual exam standard.  
- 4 balanced options (A–D).  
- Correct answer mapped.  
- stem: Markdown **bold buzzwords** and *italics*.  
- correct_answer: single uppercase letter.  
- feedback.correct: ✅ acknowledgement, praise, high-yield reinforcement, mnemonic/tip; 3–5 empathetic, live sentences.  
- feedback.wrong: ❌ acknowledgement, why it seems logical, correction, mnemonic/hook; 3–5 empathetic, live sentences.  
- learning_gap: one concise sentence explaining the misconception and learning gap responsible for the error.  

👉 Output must be strict JSON in the following format (array of 6 objects):  

[
  {
    "stem": "",
    "mcq_key": "mcq_1",
    "options": { "A": "", "B": "", "C": "", "D": "" },
    "correct_answer": "",
    "feedback": { "correct": "", "wrong": "" },
    "learning_gap": ""
  },
  {
    "stem": "",
    "mcq_key": "mcq_2",
    "options": { "A": "", "B": "", "C": "", "D": "" },
    "correct_answer": "",
    "feedback": { "correct": "", "wrong": "" },
    "learning_gap": ""
  },
  {
    "stem": "",
    "mcq_key": "mcq_3",
    "options": { "A": "", "B": "", "C": "", "D": "" },
    "correct_answer": "",
    "feedback": { "correct": "", "wrong": "" },
    "learning_gap": ""
  },
  {
    "stem": "",
    "mcq_key": "mcq_4",
    "options": { "A": "", "B": "", "C": "", "D": "" },
    "correct_answer": "",
    "feedback": { "correct": "", "wrong": "" },
    "learning_gap": ""
  },
  {
    "stem": "",
    "mcq_key": "mcq_5",
    "options": { "A": "", "B": "", "C": "", "D": "" },
    "correct_answer": "",
    "feedback": { "correct": "", "wrong": "" },
    "learning_gap": ""
  },
  {
    "stem": "",
    "mcq_key": "mcq_6",
    "options": { "A": "", "B": "", "C": "", "D": "" },
    "correct_answer": "",
    "feedback": { "correct": "", "wrong": "" },
    "learning_gap": ""
  }
]

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
    .replace(/```$/i, "");

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

  // free stale locks
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

  for (const row of block) {
    try {
      const prompt = buildPrompt(row.concept_json);
      const raw = await callOpenAI([{ role: "user", content: prompt }]);
      const obj = safeParseJSON(raw);

      updates.push({ id: row.vertical_id, data: { mcq: obj } });
    } catch (e) {
      console.error(`❌ Error processing row ${row.vertical_id}:`, e.message || e);
      await clearLocks([row.vertical_id]);
    }
  }

  for (const u of updates) {
    const { error: upErr } = await supabase
      .from("concepts_vertical")
      .update(u.data)
      .eq("vertical_id", u.id);
    if (upErr) throw upErr;
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
