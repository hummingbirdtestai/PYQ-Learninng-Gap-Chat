// /app/workers/subjectImageMCQWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.SUBJECT_IMAGE_MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.SUBJECT_IMAGE_MCQ_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.SUBJECT_IMAGE_MCQ_BLOCK_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.SUBJECT_IMAGE_MCQ_SLEEP_MS || "1000", 10);
const LOCK_TTL_MIN = parseInt(process.env.SUBJECT_IMAGE_MCQ_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `subject-image-mcq-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt Builder ----------
function buildPrompt(keyword) {
  return `
You are a NEET-PG Exam paper setter with 30 years of experience, deeply familiar with NEET-PG, INICET, AMBOSS, UWorld, NBME, and FIRST AID question styles. 
From the given Concept and Topic , create ONE *Clinical Vignette–based MCQ* (moderate–severe difficulty) that mirrors the style and reasoning depth of AMBOSS/UWorld/NBME/USMLE/MRCP exams, with *very high probability* of appearing in NEET-PG next year. 
OUTPUT FORMAT — Output valid JSON only ( no arrays, no extra text): 
{ 
  "stem": "", 
  "mcq_key": "mcq_1", 
  "options": { "A": "", "B": "", "C": "", "D": "" }, 
  "correct_answer": "A|B|C|D", 
  "learning_gap": "", 
  "high_yield_facts": "" 
}
🧠 STYLE GUIDE - Use Unicode Markup to highlight keywords for *bold*, *italic*, superscripts (Na⁺), subscripts, arrows (→), and symbols (±), equations,math 
- Stem must read like a real NEET-PG clinical vignette. 
- No “EXCEPT” or “All of the following”.

Concept / Topic:
${keyword}
  `.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function isRetryable(e) {
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(String(e?.message || e));
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "text" },
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

function safeParseJson(raw, id) {
  try {
    const cleaned = raw.trim()
      .replace(/^```json\s*/i, "")
      .replace(/^```/, "")
      .replace(/```$/, "");
    return JSON.parse(cleaned);
  } catch (err) {
    throw new Error(`❌ Failed to parse JSON for id=${id}: ${err.message}. Raw: ${raw.slice(0,200)}`);
  }
}

// ---------- Locking ----------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();
  await supabase
    .from("subject_images_flatten")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .is("mcq_image", null)
    .lt("mcq_lock_at", cutoff);
}

async function claimRows(limit) {
  await freeStaleLocks();
  const { data, error } = await supabase
    .from("subject_images_flatten")
    .select("id, key_word")
    .is("mcq_image", null)
    .is("mcq_lock", null)
    .not("key_word", "is", null)
    .limit(limit);
  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);
  const { data: locked, error: e2 } = await supabase
    .from("subject_images_flatten")
    .update({ mcq_lock: WORKER_ID, mcq_lock_at: new Date().toISOString() })
    .in("id", ids)
    .is("mcq_image", null)
    .is("mcq_lock", null)
    .select("id, key_word");
  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("subject_images_flatten")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .in("id", ids);
}

// ---------- Processing ----------
async function processRow(row) {
  const prompt = buildPrompt(row.key_word);
  const raw = await callOpenAI(prompt);
  const jsonOut = safeParseJson(raw, row.id);

  if (!jsonOut || typeof jsonOut !== "object" || !jsonOut.mcq_key) {
    throw new Error(`❌ Invalid JSON structure for id=${row.id}`);
  }

  const { error } = await supabase
    .from("subject_images_flatten")
    .update({
      mcq_image: jsonOut,
      mcq_lock: null,
      mcq_lock_at: null,
      updated_at: new Date().toISOString(),
    })
    .eq("id", row.id);

  if (error) throw new Error(`Update failed id=${row.id}: ${error.message}`);
  return { updated: 1 };
}

async function processBatch(rows) {
  const chunks = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE)
    chunks.push(rows.slice(i, i + BATCH_SIZE));

  let updated = 0;
  for (const chunk of chunks) {
    const results = await Promise.allSettled(chunk.map(processRow));
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (r.status === "fulfilled") updated += r.value.updated;
      else {
        console.error(r.reason?.message || r.reason);
        await clearLocks([chunk[i].id]);
      }
    }
  }
  return updated;
}

// ---------- Main ----------
(async function main() {
  console.log(`🧵 Subject Image MCQ Worker ${WORKER_ID} | model=${MODEL}`);
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }
      console.log(`⚙️ claimed=${claimed.length}`);
      const updated = await processBatch(claimed);
      console.log(`✅ updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
