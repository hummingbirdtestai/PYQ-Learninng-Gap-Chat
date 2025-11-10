// workers/zoobotunicodeFlashCardsWorker.js
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
function buildPrompt(conceptText) {
  return `
You are a senior NEET mentor (30+ yrs, completely encyclopedic of NCERT Class XI and Class XII Botany and Zoology Textbooks and 3000 PYQs from Past 37 Years of NEET, AIPMT exams).

Input: ${JSON.stringify(conceptText)}

Task:
- Create 10 high-yield flashcards (JSON array) for NEET Preparation for active recall & spaced repetition.
- Flow: Base your emphasis totally on NCERT Class XI and Class XII Botany and Zoology Textbooks.
- Each card unique, exam-centric, remediation-oriented.
- Answers short (1–2 words/phrases).
- No MCQs.
- Don’t mention PYQ or reference NEET directly, only use the text to base flashcards.

Output format (strict JSON only):
[
  { "Question": "string", "Answer": "string" }
]

Rules:
- Use Markdown Unicode formatting emphasizing **bold/italic** key terms, arrows (→, ↑, ↓), subscripts/superscripts (H₂O, Na⁺, Ca²⁺, ₁, ₂, ³, ⁺, ⁻), Greek letters (α, β, γ), emojis.
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
      console.warn(`⚠️ Retrying OpenAI call (attempt ${attempt}) due to:`, e.message);
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
    .from("biology_raw_new_flattened")
    .update({ match_lock: null, match_lock_at: null })
    .is("flash_cards", null)
    .lt("match_lock_at", cutoff);

  if (SUBJECT_FILTER) q = q.eq("subject_name", SUBJECT_FILTER);
  await q;
}

async function claimRows(limit) {
  await freeStaleLocks();

  let q = supabase
    .from("biology_raw_new_flattened")
    .select("id, concept_phase")
    .not("concept_phase", "is", null)
    .is("flash_cards", null)
    .is("match_lock", null)
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
      match_lock: WORKER_ID,
      match_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("flash_cards", null)
    .is("match_lock", null)
    .select("id, concept_phase");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("biology_raw_new_flattened")
    .update({ match_lock: null, match_lock_at: null })
    .in("id", ids);
}

// ---------- Process ----------
async function processRow(row) {
  const prompt = buildPrompt(row.concept_phase);
  const raw = await callOpenAI(prompt);
  let jsonOut;

  try {
    jsonOut = safeParseJson(raw);
  } catch (e) {
    throw new Error(`Invalid JSON for id=${row.id}: ${e.message}`);
  }

  if (!Array.isArray(jsonOut)) {
    throw new Error(`Output not array for id=${row.id}`);
  }

  const { error: upErr } = await supabase
    .from("biology_raw_new_flattened")
    .update({
      flash_cards: jsonOut,
      match_lock: null,
      match_lock_at: null
    })
    .eq("id", row.id);

  if (upErr) {
    const preview = JSON.stringify(jsonOut).slice(0, 200);
    throw new Error(`Update failed for id=${row.id}: ${upErr.message}. Preview: ${preview}`);
  }
  return { updated: 1 };
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
        console.error(`❌ Row ${chunk[i].id} error:`, r.reason?.message || r.reason);
        await clearLocks([chunk[i].id]);
      }
    }
  }
  return updated;
}

// ---------- Main ----------
(async function main() {
  console.log(`🧠 PG Unicode FlashCards Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);
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
