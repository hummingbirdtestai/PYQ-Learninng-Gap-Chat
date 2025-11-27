require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.MCQ_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.MCQ_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `mcq-mbbs-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ─────────────────────────────────────────────
// MCQ PROMPT BUILDER
// ─────────────────────────────────────────────
function buildPrompt(topic) {
return `
You are a **NEET-PG Exam paper setter with 30 years of experience**, deeply familiar with question patterns of **NEET-PG, NBME, AMBOSS, UWorld, and First Aid**.

From the following medical TOPIC, create **exactly 5 NEET-PG style clinical case vignette MCQs**.

Follow this exact JSON format:

{
 "mcq_n": {
   "stem": "Clinical vignette using **bold**, _italic_, arrows (→ ↑ ↓), Greek (α β Δ μ), subscripts/superscripts (₁₂³⁺⁻). End with 'Which of the following is most likely...?'",
   "options": { "A": "...", "B": "...", "C": "...", "D": "..." },
   "correct_answer": "A",
   "feedback": {
     "wrong": "❌ Why the wrong answers are incorrect — short, factual.",
     "correct": "✅ Why the correct answer is correct — crisp reasoning.",
     "high_yield_facts": "🔑 One-line pearls like real NEET-PG review.",
     "learning_gap": "💡 Most common mistake students make & how to avoid it."
   }
 }
}

⛔ STRICT RULES:
• 5 MCQs ONLY  
• No “EXCEPT”, no “All of the following”  
• Must contain: **age**, **symptoms**, **labs**, **clinical clues**  
• Difficulty = moderate-to-severe  
• Correct answer = ONLY A/B/C/D  

TOPIC:
${topic}
`;
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(s);
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages,
      temperature: 0.2
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(500 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw e;
  }
}

function safeParse(raw) {
  const cleaned = raw
    .trim()
    .replace(/^```json/i, "")
    .replace(/^```/, "")
    .replace(/```$/, "");

  try {
    return JSON.parse(cleaned);
  } catch (err) {
    console.error("❌ JSON Parse ERROR:", cleaned.slice(0, 200));
    throw err;
  }
}

// ─────────────────────────────────────────────
// CLAIM ROWS (using concept_lock / concept_lock_at)
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // 1. Release old locks
  await supabase
    .from("subject_curriculum")
    .update({ concept_lock: null, concept_lock_at: null })
    .lt("concept_lock_at", cutoff);

  // 2. Pick rows with topic (NOT concept)
  const { data: rows, error: err1 } = await supabase
    .from("subject_curriculum")
    .select("id, subject, chapter, topic, chapter_id, topic_id")
    .not("topic", "is", null)
    .not("topic", "eq", "")
    .is("concept_lock", null)
    .order("id", { ascending: true })
    .limit(limit);

  if (err1) throw err1;
  if (!rows || rows.length === 0) return [];

  const ids = rows.map(r => r.id);

  // 3. Apply lock
  const { data: locked, error: err2 } = await supabase
    .from("subject_curriculum")
    .update({
      concept_lock: WORKER_ID,
      concept_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("concept_lock", null)
    .select("id, subject, chapter, topic, chapter_id, topic_id");

  if (err2) throw err2;
  return locked || [];
}

// ─────────────────────────────────────────────
// CLEAR LOCKS
// ─────────────────────────────────────────────
async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("subject_curriculum")
    .update({ concept_lock: null, concept_lock_at: null })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW — CREATE MCQs
// ─────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.topic);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const parsed = safeParse(raw);

  const payload = {
    id: uuidv4(),
    subject: row.subject,
    chapter: row.chapter,
    topic: row.topic,
    chapter_id: row.chapter_id,
    topic_id: row.topic_id,
    mcq_json: parsed,
  };

  // Insert into practice_mcq
  await supabase.from("practice_mcq").insert(payload);

  // Clear lock
  await clearLocks([row.id]);

  return { updated: 1 };
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 MCQ Worker Started | worker=${WORKER_ID} | model=${MODEL}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows`);

      const results = await Promise.allSettled(
        claimed.map(r => processRow(r))
      );

      let updated = 0;
      results.forEach((res, idx) => {
        if (res.status === "fulfilled") {
          console.log(`   ✅ Row ${idx + 1} processed`);
          updated += res.value.updated;
        } else {
          console.error(`   ❌ Row ${idx + 1} failed:`, res.reason);
          clearLocks([claimed[idx].id]);
        }
      });

      console.log(`🔁 Loop complete: updated=${updated}/${claimed.length}`);

    } catch (e) {
      console.error("Loop error:", e);
      await sleep(1000);
    }
  }
})();
