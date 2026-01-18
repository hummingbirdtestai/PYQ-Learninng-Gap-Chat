require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.CONCEPT_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CONCEPT_LIMIT || "30", 10);
const BATCH_SIZE   = parseInt(process.env.CONCEPT_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.CONCEPT_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.CONCEPT_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `concept-obg-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ─────────────────────────────────────────────
// CONCEPT PROMPT (USE AS-IS)
// ─────────────────────────────────────────────
function buildPrompt(question) {
  return `
You are an 30 Years experienced Undergraduate MBBS **Gynaecology and Obstertrics * Teacher expert in Preparing MBBS Students to do Last Minute revision for University MBBS exams Theory Papers . MBBS Students needed Bullet Points Summary Notes to answer this Question as Buzzword style for Active Recall and Spaced Repettion to write Answers in Exam . The content should be 

1) Concept Summariy on Bulleted points  
2) High-Yield Facts  
3) Clinical Case Vignettes  
4) Summary Table // Mnemonics  
5) ULTRA–HIGH-YIELD NEET-PG ONE-LINERS with 📌 Pure recall bullets 📌 No explanations 📌 Designed for rapid revision + MCQ solving  

Give the output *strictly in Markdown code blocks* with Unicode symbols.  
In the output, explicitly *bold and italicize* all important key words, clinical terms, diseases, signs, investigations, and headings for emphasis using proper Markdown (e.g., bold, italic).  
Use headings, *bold, *italic, arrows (→, ↑, ↓), subscripts/superscripts (₁, ₂, ³, ⁺, ⁻), Greek letters, and emojis (💡🫀🫁🧠⚕📘) naturally throughout for visual clarity.  

Do *NOT* output as JSON.  
Do *NOT* add any titles or headers beyond the 5 sections I specify.  
Output ONLY those 5 sections exactly as numbered.  
Output as ONE single Markdown code block.

QUESTION:
${question}
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

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }]
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 2) {
      await sleep(600 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

// ─────────────────────────────────────────────
// CLAIM ROWS (concept IS NULL)
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // 1️⃣ Clear expired locks
  await supabase
    .from("obg_raw")
    .update({ concept_lock: null, concept_lock_at: null })
    .lt("concept_lock_at", cutoff);

  // 2️⃣ Fetch rows needing concept
  const { data: rows, error } = await supabase
    .from("obg_raw")
    .select("id, question")
    .is("concept", null)
    .is("concept_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // 3️⃣ Lock rows
  const { data: locked, error: err2 } = await supabase
    .from("obg_raw")
    .update({
      concept_lock: WORKER_ID,
      concept_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("concept_lock", null)
    .select("id, question");

  if (err2) throw err2;

  return locked || [];
}

// ─────────────────────────────────────────────
// CLEAR LOCKS
// ─────────────────────────────────────────────
async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("obg_raw")
    .update({ concept_lock: null, concept_lock_at: null })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS SINGLE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.question);

  let output = await callOpenAI(prompt);

  // Ensure Markdown code block presence
  if (!output.trim().startsWith("```")) {
    throw new Error("❌ Output is not wrapped in Markdown code block");
  }

  await supabase
    .from("obg_raw")
    .update({
      concept: output,
      concept_lock: null,
      concept_lock_at: null
    })
    .eq("id", row.id);

  return true;
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 OBG CONCEPT WORKER STARTED | ${WORKER_ID} | model=${MODEL}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows`);

      for (let i = 0; i < claimed.length; i += BATCH_SIZE) {
        const batch = claimed.slice(i, i + BATCH_SIZE);

        const results = await Promise.allSettled(
          batch.map(r => processRow(r))
        );

        results.forEach((res, idx) => {
          if (res.status === "fulfilled") {
            console.log(`   ✅ Concept generated`);
          } else {
            console.error(`   ❌ Failed row ${batch[idx].id}`, res.reason);
            clearLocks([batch[idx].id]);
          }
        });
      }

    } catch (e) {
      console.error("❌ Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
