require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ───────────────────────────────────────────────
// SETTINGS
// ───────────────────────────────────────────────
const MODEL        = process.env.FLASHCARD_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.FLASHCARD_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.FLASHCARD_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.FLASHCARD_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.FLASHCARD_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `flashcards-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ───────────────────────────────────────────────
// PROMPT BUILDER
// ───────────────────────────────────────────────
function buildPrompt(concept) {
  return `
You are an expert NEET-PG faculty. Create 10 flashcards for rapid revision from the paragraph below.

Rules:
• Output strictly in JSON array with 3 keys: "mentor_reply", "Question", "answer".
• "mentor_reply" → Add a clue inspiring student to remember. It should be logical, not emotional.
• "answer" → 2–3 words + ≤10-word mnemonic or clue.
• First 5 → USMLE/NEET-PG–style clinical vignettes (~150 words each; NBME, AMBOSS, UWorld, FA standard).
• Last 5 → one-line high-yield recall Qs (NEETPG/FMGE; for active recall).
• Use Markdown + Unicode (**, _, ₂ , ³ , → , α , β, etc.), no LaTeX, no MCQs.
• Tone = senior teacher giving logical cues for memory recall, not emotional encouragement.
• Be concise, clinical, and exam-oriented.

PARAGRAPH:
${concept}
`.trim();
}

// ───────────────────────────────────────────────
// HELPERS
// ───────────────────────────────────────────────
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
    .replace(/^```json\s*/i, "")
    .replace(/^```/i, "")
    .replace(/```$/i, "")
    .replace(/,\s*}/g, "}")
    .replace(/,\s*]/g, "]");

  try {
    return JSON.parse(cleaned);
  } catch (e) {
    console.error("❌ JSON parse error:", cleaned.slice(0, 200));
    throw e;
  }
}

// ───────────────────────────────────────────────
// LOCKING
// ───────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // 1. Free expired locks
  await supabase
    .from("flashcard_raw")
    .update({ phase_json_lock: null, phase_json_lock_at: null })
    .is("phase_json", null)
    .lt("phase_json_lock_at", cutoff);

  // 2. Select candidates — rows that HAVE concept but no phase_json yet
  const { data: candidates, error: e1 } = await supabase
    .from("flashcard_raw")
    .select("id, concept")
    .not("concept", "is", null)
    .not("concept", "eq", "")
    .is("phase_json", null)
    .is("phase_json_lock", null)
    .order("id", { ascending: true })
    .limit(limit);

  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  // 3. Lock claimed rows
  const { data: locked, error: e2 } = await supabase
    .from("flashcard_raw")
    .update({
      phase_json_lock: WORKER_ID,
      phase_json_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("phase_json", null)
    .is("phase_json_lock", null)
    .select("id, concept");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("flashcard_raw")
    .update({ phase_json_lock: null, phase_json_lock_at: null })
    .in("id", ids);
}

// ───────────────────────────────────────────────
// PROCESS ONE ROW
// ───────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.concept);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const parsed = safeParseObject(raw);

  const final = { uuid: uuidv4(), flashcards: parsed };

  await supabase
    .from("flashcard_raw")
    .update({ phase_json: final })
    .eq("id", row.id);

  await clearLocks([row.id]);
  return { updated: 1 };
}

// ───────────────────────────────────────────────
// MAIN LOOP
// ───────────────────────────────────────────────
(async function main() {
  console.log(`🧠 Flashcard Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows`);

      const results = await Promise.allSettled(claimed.map(row => processRow(row)));

      let updated = 0;
      results.forEach((r, idx) => {
        if (r.status === "fulfilled") {
          console.log(`   ✅ row ${idx + 1} updated`);
          updated += r.value.updated;
        } else {
          console.error(`   ❌ row ${idx + 1} error:`, r.reason.message || r.reason);
          clearLocks([claimed[idx].id]);
        }
      });

      console.log(`🔁 loop complete — updated=${updated} / ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
