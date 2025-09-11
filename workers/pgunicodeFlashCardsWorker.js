// workers/pgunicodeFlashCardsWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.FLASHCARDS_UNICODE_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.FLASHCARDS_UNICODE_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.FLASHCARDS_UNICODE_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.FLASHCARDS_UNICODE_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.FLASHCARDS_UNICODE_LOCK_TTL_MIN || "15", 10);
const SUBJECT_FILTER = process.env.FLASHCARDS_UNICODE_SUBJECT || null;
const WORKER_ID    = process.env.WORKER_ID || `pg-flashcards-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(conceptJson) {
  return `
You are a senior NEETPG mentor (30+ yrs, mastery of UWorld/First Aid/Amboss/NBME/Marrow/Prepladder).

Input: ${JSON.stringify(conceptJson)}

Task:  
- Create 10 high-yield flashcards (JSON array) for NEETPG/INICET/FMGE active recall & spaced repetition.  
- Flow: recognition → mechanism → causes → associations → differentials → investigations → management → complications → exam traps.  
- Cover related concepts too (basic, para-clinical, clinical).  
- Mix direct fact Qs and clinical vignette Qs.  
- Each card unique, exam-centric, remediation-oriented.  
- Answers short (1–2 words/phrases).  
- No MCQs.

Output format (strict JSON only):
[
  { "Question": "string", "Answer": "string" }
]

Rules:  
- Use **bold/italic markup** only for *key words* in questions.  
- Use Unicode for subscripts/superscripts (H₂O, Na⁺, Ca²⁺).  
- No meta/filler.  
- Output JSON only.
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
      response_format: { type: "text" }
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(500 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

function safeParseJson(raw) {
  const cleaned = raw.trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```/, "")
    .replace(/```$/, "");
  return JSON.parse(cleaned);
}

// ---------- Locking ----------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();
  let q = supabase
    .from("mcq_bank")
    .update({ lg_flashcard_lock: null, lg_flashcard_locked_at: null })
    .is("flashcard_unicode", null)
    .lt("lg_flashcard_locked_at", cutoff);

  if (SUBJECT_FILTER) q = q.eq("subject", SUBJECT_FILTER);
  await q;
}

async function claimRows(limit) {
  await freeStaleLocks();

  let q = supabase
    .from("mcq_bank")
    .select("id, concept_json")
    .not("concept_json", "is", null)
    .is("flashcard_unicode", null)
    .is("lg_flashcard_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (SUBJECT_FILTER) q = q.eq("subject", SUBJECT_FILTER);

  const { data: candidates, error } = await q;
  if (error) throw error;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);
  const { data: locked, error: e2 } = await supabase
    .from("mcq_bank")
    .update({
      lg_flashcard_lock: WORKER_ID,
      lg_flashcard_locked_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("flashcard_unicode", null)
    .is("lg_flashcard_lock", null)
    .select("id, concept_json");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("mcq_bank")
    .update({ lg_flashcard_lock: null, lg_flashcard_locked_at: null })
    .in("id", ids);
}

// ---------- Process ----------
async function processRow(row) {
  const prompt = buildPrompt(row.concept_json);
  const raw = await callOpenAI(prompt);
  const jsonOut = safeParseJson(raw);

  if (!Array.isArray(jsonOut)) {
    throw new Error(`Output not array for id=${row.id}`);
  }

  const { error: upErr } = await supabase
    .from("mcq_bank")
    .update({
      flashcard_unicode: jsonOut,
      lg_flashcard_lock: null,
      lg_flashcard_locked_at: null
    })
    .eq("id", row.id);

  if (upErr) {
    const preview = JSON.stringify(jsonOut).slice(0, 200);
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
        console.error(`   row ${i + 1} error:`, r.reason?.message || r.reason);
        await clearLocks([chunk[i].id]);
      }
    }
  }
  return updated;
}

// ---------- Main ----------
(async function main() {
  console.log(`🧵 PG Unicode FlashCards Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }
      console.log(`⚙️ claimed=${claimed.length}`);
      const updated = await processBatch(claimed);
      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
