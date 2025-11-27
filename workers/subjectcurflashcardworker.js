require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ───────────────────────────────────────────────────────────────────
// SETTINGS
// ───────────────────────────────────────────────────────────────────
const MODEL        = process.env.FLASHCARD_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.FLASHCARD_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.FLASHCARD_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.FLASHCARD_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.FLASHCARD_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `flashcards-mbbs-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ───────────────────────────────────────────────────────────────────
// PROMPT GENERATOR
// ───────────────────────────────────────────────────────────────────
function buildPrompt(concept) {
  return `
You are 30 Years experienced MBBS TEACHER, NMC COMPETENCE BASED MEDICAL EDUCATION EXPERT and UNIVERSITY MBBS exam paper setter.

This is the concept from NMC CBME syllabus. Create **10 flashcards** for rapid revision.

RULES:
• Output strictly in **JSON array**, each item with keys: "Question", "answer".
• Questions 1–5 → **Clinical vignettes** (USMLE/NEET-PG style, ~150 words).
• Questions 6–10 → **One-line high-yield recall**.
• "answer" must be **2–3 words + ≤10-word mnemonic**.
• Use **Markdown + Unicode** characters: **, _, ₂ , ³ , → , α , β etc.
• No LaTeX. No MCQs.
• Tone: **senior teacher, logical memory cues, exam-focused**. No emotional tone.
• Be concise, clinical, high-yield.

CONCEPT PARAGRAPH:
${concept}
`.trim();
}

// ───────────────────────────────────────────────────────────────────
// HELPERS
// ───────────────────────────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(s);
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
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

function safeParseObject(raw) {
  const cleaned = raw
    .trim()
    .replace(/^```json/i, "")
    .replace(/^```/, "")
    .replace(/```$/, "")
    .replace(/,\s*}/g, "}")
    .replace(/,\s*]/g, "]");

  try {
    return JSON.parse(cleaned);
  } catch (err) {
    console.error("❌ JSON Parse Error:", cleaned.slice(0, 200));
    throw err;
  }
}

// ───────────────────────────────────────────────────────────────────
// LOCKING SYSTEM
// Target table: subject_curriculum
// Drop table: flashcard_phases
// ───────────────────────────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // 1. Release expired locks
  await supabase
    .from("subject_curriculum")
    .update({ flashcard_lock: null, flashcard_lock_at: null })
    .is("concept", null, false)
    .lt("flashcard_lock_at", cutoff);

  // 2. Select rows that have concept but no flashcards generated yet
  const { data: rows, error: err1 } = await supabase
    .from("subject_curriculum")
    .select("id, subject_id, subject, concept")
    .not("concept", "is", null)
    .not("concept", "eq", "")
    .is("flashcards_generated", null)
    .is("flashcard_lock", null)
    .order("id", { ascending: true })
    .limit(limit);

  if (err1) throw err1;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // 3. Apply lock
  const { data: locked, error: err2 } = await supabase
    .from("subject_curriculum")
    .update({
      flashcard_lock: WORKER_ID,
      flashcard_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("flashcards_generated", null)
    .is("flashcard_lock", null)
    .select("id, subject_id, subject, concept");

  if (err2) throw err2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("subject_curriculum")
    .update({ flashcard_lock: null, flashcard_lock_at: null })
    .in("id", ids);
}

// ───────────────────────────────────────────────────────────────────
// PROCESS ONE ROW
// ───────────────────────────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.concept);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const parsed = safeParseObject(raw);

  const payload = {
    id: uuidv4(),
    subject_id: row.subject_id,
    subject: row.subject,
    concept: row.concept,
    phase_json: parsed,
  };

  // Write into flashcard_phases
  await supabase.from("flashcard_phases").insert(payload);

  // Mark source row as processed
  await supabase
    .from("subject_curriculum")
    .update({ flashcards_generated: true })
    .eq("id", row.id);

  await clearLocks([row.id]);

  return { updated: 1 };
}

// ───────────────────────────────────────────────────────────────────
// MAIN WORKER LOOP
// ───────────────────────────────────────────────────────────────────
(async function main() {
  console.log(`📘 MBBS Flashcard Worker Started | worker=${WORKER_ID} | model=${MODEL}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} concept rows`);

      const results = await Promise.allSettled(
        claimed.map(row => processRow(row))
      );

      let updated = 0;
      results.forEach((r, idx) => {
        if (r.status === "fulfilled") {
          console.log(`   ✅ Processed row #${idx + 1}`);
          updated += r.value.updated;
        } else {
          console.error(`   ❌ Error in row #${idx + 1}:`, r.reason);
          clearLocks([claimed[idx].id]);
        }
      });

      console.log(`🔁 Batch complete → inserted=${updated} / total=${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e);
      await sleep(1000);
    }
  }
})();
