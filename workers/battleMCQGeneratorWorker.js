// /workers/battleMCQGeneratorWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.SUBJECT_IMAGE_MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.SUBJECT_IMAGE_MCQ_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.SUBJECT_IMAGE_MCQ_BLOCK_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.SUBJECT_IMAGE_MCQ_SLEEP_MS || "1000", 10);
const LOCK_TTL_MIN = parseInt(process.env.SUBJECT_IMAGE_MCQ_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `battle-mcq-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ─────────────────────────────────────────────
// PROMPT BUILDER
// ─────────────────────────────────────────────
function buildPrompt(conceptText) {
  return `
You are a **NEETPG Paper Setter with 30 Years of Experience**.

Create **30 MCQs** in **strict JSON format** from the following concept.
Each MCQ must test a **high-yield fact** or **clinical vignette** directly based on the concept.

**Prompt Rules:**
- Output strictly as valid JSON array of 30 objects.
- Each object must contain:
  {
    "Stem": "…",
    "Options": { "A": "…", "B": "…", "C": "…", "D": "…" },
    "Correct Answer": "A|B|C|D"
  }
- Style: Clinical Case Vignette + High-Yield Fact type.
- Use Unicode markup for **bold**, *italic*, superscripts (Na⁺), subscripts, arrows (→), and symbols (±), equations, etc.
- Avoid explanations or text outside JSON.

**INPUT CONCEPT:**
${conceptText}
`.trim();
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function isRetryable(e) {
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(String(e?.message || e));
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

function safeParseJSON(raw) {
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
    console.error("❌ JSON parse error. Snippet:", cleaned.slice(0, 300));
    throw e;
  }
}

// ─────────────────────────────────────────────
// LOCK SYSTEM
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // Clear stale locks
  await supabase
    .from("flashcard_raw")
    .update({ mentor_reply_lock: null, mentor_reply_lock_at: null })
    .lt("mentor_reply_lock_at", cutoff);

  // Fetch rows needing battle_mcqs
  const { data: candidates, error: e1 } = await supabase
    .from("flashcard_raw")
    .select("id, concept_final")
    .is("battle_mcqs", null)
    .is("mentor_reply_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map((r) => r.id);

  // Apply lock
  const { data: locked, error: e2 } = await supabase
    .from("flashcard_raw")
    .update({
      mentor_reply_lock: WORKER_ID,
      mentor_reply_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("battle_mcqs", null)
    .select("id, concept_final");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("flashcard_raw")
    .update({ mentor_reply_lock: null, mentor_reply_lock_at: null })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const concept = row.concept_final;
  if (!concept || !concept.trim()) throw new Error("Empty concept_final");

  const prompt = buildPrompt(concept);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const mcqJSON = safeParseJSON(raw);

  await supabase
    .from("flashcard_raw")
    .update({
      battle_mcqs: mcqJSON,
      mentor_reply_lock: null,
      mentor_reply_lock_at: null,
      updated_at: new Date().toISOString(),
    })
    .eq("id", row.id);

  return { updated: 1 };
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🔥 BattleMCQ Generator Worker ${WORKER_ID} | model=${MODEL} | limit=${LIMIT}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows`);
      const results = await Promise.allSettled(claimed.map((r) => processRow(r)));

      let updated = 0;
      results.forEach((res, i) => {
        if (res.status === "fulfilled") {
          console.log(`✅ Row ${i + 1}: MCQs generated`);
          updated += res.value.updated;
        } else {
          console.error(`❌ Row ${i + 1} error:`, res.reason.message || res.reason);
          clearLocks([claimed[i].id]);
        }
      });

      console.log(`🌀 Batch complete — updated=${updated}/${claimed.length}`);
    } catch (err) {
      console.error("💥 Loop error:", err.message || err);
      await sleep(2000);
    }
  }
})();
