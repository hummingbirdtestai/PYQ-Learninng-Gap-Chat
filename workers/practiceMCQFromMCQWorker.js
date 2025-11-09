// workers/practiceMCQFromMCQWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.PRACTICE_MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.PRACTICE_MCQ_LIMIT || "200", 10);
const BATCH_SIZE   = parseInt(process.env.PRACTICE_MCQ_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.PRACTICE_MCQ_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.PRACTICE_MCQ_LOCK_TTL_MIN || "15", 10);
const SUBJECT_FILTER = process.env.PRACTICE_MCQ_SUBJECT || null;
const WORKER_ID    = process.env.WORKER_ID || `practice-from-mcq-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(mcqJson) {
  return `
You are a 30-year NEET Biology paper-setter. Based on the NEET PYQ below, create 5 NEET-style MCQs that can come as MCQs in future NEET Exams.

• Output a valid JSON array (no text outside JSON).  
• Each object must follow this exact schema 👇  
{
  "stem": "Question text (Markdown + Unicode + emojis 💡🧠⚕️📘)",
  "options": { "A": "", "B": "", "C": "", "D": "" },
  "feedback": {
    "wrong": "❌ Why it’s wrong — use Markdown, **bold/italic** key terms, arrows (→, ↑, ↓), subscripts/superscripts (₁, ₂, ³, ⁺, ⁻), Greek letters (α, β, γ)",
    "correct": "✅ Why it’s correct — clear, brief, factual explanation"
  },
  "learning_gap": "1-line student misconception",
  "correct_answer": "A"
}
• Use Markdown formatting throughout.  
• Bold/italicize all important biological words (e.g., oogenesis, prophase I, LH surge).  
• Do NOT bold/italicize options.  
• Keep authentic NEET tone, concise factual phrasing.  
• These MCQs should ensure the student attains 100% strike rate in NEET through deep concept practice.

NEET PYQ:
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
      response_format: { type: "json_object" },
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
    .update({ concept_lock: null, concept_lock_at: null })
    .is("practice_mcqs", null)
    .lt("concept_lock_at", cutoff);
  if (SUBJECT_FILTER) q = q.eq("subject_name", SUBJECT_FILTER);
  await q;
}

async function claimRows(limit) {
  await freeStaleLocks();
  let q = supabase
    .from("biology_raw_new_flattened")
    .select("id, mcq_json")
    .not("mcq_json", "is", null)
    .is("practice_mcqs", null)
    .is("concept_lock", null)
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
      concept_lock: WORKER_ID,
      concept_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("practice_mcqs", null)
    .is("concept_lock", null)
    .select("id, mcq_json");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("biology_raw_new_flattened")
    .update({ concept_lock: null, concept_lock_at: null })
    .in("id", ids);
}

// ---------- Process ----------
async function processRow(row) {
  const prompt = buildPrompt(row.mcq_json);
  const output = await callOpenAI(prompt);

  if (!output || output.length < 50) {
    throw new Error(`Empty or too short output for id=${row.id}`);
  }

  // Validate JSON
  let parsedOutput;
  try {
    parsedOutput = JSON.parse(output);
  } catch (e) {
    throw new Error(`Invalid JSON output for id=${row.id}: ${e.message}`);
  }

  const { error: upErr } = await supabase
    .from("biology_raw_new_flattened")
    .update({
      practice_mcqs: parsedOutput,
      concept_lock: null,
      concept_lock_at: null,
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
  console.log(`🧠 Practice MCQ-from-PYQ Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);
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
