// /workers/battleMCQGeneratorWorker_10.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL = process.env.SUBJECT_IMAGE_MCQ_MODEL || "gpt-5-mini";
const LIMIT = parseInt(process.env.SUBJECT_IMAGE_MCQ_LIMIT || "5", 10);
const SLEEP_MS = parseInt(process.env.SUBJECT_IMAGE_MCQ_SLEEP_MS || "5000", 10);
const LOCK_TTL_MIN = parseInt(process.env.SUBJECT_IMAGE_MCQ_LOCK_TTL_MIN || "15", 10);
const WORKER_ID =
  process.env.WORKER_ID ||
  `battle-mcq10-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;

// ─────────────────────────────────────────────
// PROMPT BUILDER (30 MCQs ONLY)
// ─────────────────────────────────────────────
function buildPrompt(conceptText) {
  return `
You are a **30 Years experienced NEETPG Paper Setter**, creating exam-level questions based on **NEETPG PYQs**, written in **USMLE-style** as seen in **Amboss, UWorld, First Aid, and NBME**.

Create **30 MCQs** that combine **clinical case vignettes** and **single-liner high-yield facts**, covering **the most tested and high-yield points** related to the topic given.
These MCQs should be **NEETPG PYQ-based** and **could appear exactly as-is in the NEETPG Exam**.

**Prompt Rules:**
- Output strictly as a **valid JSON array of 30 objects**.
- Each object must follow this format:
  {
    "Stem": "…",
    "Options": { "A": "…", "B": "…", "C": "…", "D": "…" },
    "Correct Answer": "A|B|C|D"
  }
- “Stem” → The full question stem only (⚠ No labels like “Clinical vignette:” or “Exam tip:”).
- Use **Unicode MarkUp** to highlight **bold**, *italic*, superscripts/subscripts (H₂O, Na⁺, Ca²⁺), and symbols/arrows (±, ↑, ↓, →, ∆).
- **No explanations**, **no commentary**, **no markdown/code fences**.
- Output must be **pure JSON only** (single array [ ... ]).
- If fewer than 30 can be generated due to token limits, still return valid JSON.

**INPUT CONCEPT:**
${conceptText}
`.trim();
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const isRetryable = (e) =>
  /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(String(e?.message || e));

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages,
      max_completion_tokens: 4000,
    });
    return resp.choices?.[0]?.message?.content?.trim() || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 2) {
      console.warn(`⚠ Retry attempt ${attempt} due to transient error`);
      await sleep(1000 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    console.error("❌ OpenAI API call failed:", e.message || e);
    throw e;
  }
}

// ─────────────────────────────────────────────
// JSON PARSER (CLEAN & SAFE)
// ─────────────────────────────────────────────
function safeParseJSON(raw) {
  if (!raw || raw.length < 10) return [];
  let cleaned = raw
    .trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```/i, "")
    .replace(/```$/i, "")
    .replace(/,\s*}/g, "}")
    .replace(/,\s*]/g, "]")
    .replace(/}\s*{/g, "}, {");

  if (!cleaned.startsWith("[")) cleaned = "[" + cleaned;
  if (!cleaned.endsWith("]")) cleaned += "]";

  try {
    return JSON.parse(cleaned);
  } catch {
    try {
      const fixed = cleaned.replace(/[^}]*$/, "}]");
      return JSON.parse(fixed);
    } catch {
      console.error("❌ JSON parse error. Snippet:", cleaned.slice(0, 200));
      return [];
    }
  }
}

// ─────────────────────────────────────────────
// LOCK SYSTEM (skip invalid or processed rows)
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  await supabase
    .from("flashcard_raw")
    .update({ mentor_reply_lock: null, mentor_reply_lock_at: null })
    .lt("mentor_reply_lock_at", cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from("flashcard_raw")
    .select("id, concept_final")
    .is("battle_mcqs_final", null)
    .is("battle_mcqs_final_10", null)
    .is("mentor_reply_lock", null)
    .not("concept_final", "is", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (e1) throw new Error(e1.message);
  if (!candidates?.length) return [];

  const ids = candidates.map((r) => r.id);
  const { data: locked, error: e2 } = await supabase
    .from("flashcard_raw")
    .update({
      mentor_reply_lock: WORKER_ID,
      mentor_reply_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("battle_mcqs_final", null)
    .select("id, concept_final");

  if (e2) throw new Error(e2.message);
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids?.length) return;
  await supabase
    .from("flashcard_raw")
    .update({ mentor_reply_lock: null, mentor_reply_lock_at: null })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW — SKIPS EMPTY / BAD INPUT
// ─────────────────────────────────────────────
async function processRow(row) {
  const concept = row.concept_final;

  if (
    !concept ||
    concept.trim().length < 20 ||
    /I don't see|paste the raw concept|undefined|null/i.test(concept)
  ) {
    console.log(`🚫 Skipping invalid concept for row ${row.id}`);
    await supabase
      .from("flashcard_raw")
      .update({
        error_log: "SKIPPED_INVALID_CONCEPT",
        mentor_reply_lock: null,
        mentor_reply_lock_at: null,
      })
      .eq("id", row.id);
    return { updated: 0 };
  }

  const prompt = buildPrompt(concept);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const allMCQs = safeParseJSON(raw);

  if (!Array.isArray(allMCQs) || allMCQs.length < 3) {
    console.warn(`⚠ Skipping save for row ${row.id} — empty or invalid JSON`);
    await supabase
      .from("flashcard_raw")
      .update({
        battle_mcqs_final_10: null,
        error_log: `EMPTY_OR_INVALID_JSON (${allMCQs.length})`,
        mentor_reply_lock: null,
        mentor_reply_lock_at: null,
      })
      .eq("id", row.id);
    return { updated: 0 };
  }

  const { error: e3 } = await supabase
    .from("flashcard_raw")
    .update({
      battle_mcqs_final_10: allMCQs,
      mentor_reply_lock: null,
      mentor_reply_lock_at: null,
      updated_at: new Date().toISOString(),
    })
    .eq("id", row.id);

  if (e3) throw new Error(e3.message);
  return { updated: 1 };
}

// ─────────────────────────────────────────────
// MAIN LOOP (cost-efficient & self-stopping)
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🚀 BattleMCQ (30) Generator Worker Started | model=${MODEL} | limit=${LIMIT}`);
  console.log(`Worker ID: ${WORKER_ID}`);

  const claimed = await claimRows(LIMIT);

  if (!claimed.length) {
    console.log("✅ No valid rows found — exiting to save cost.");
    process.exit(0);
  }

  console.log(`⚙ Claimed ${claimed.length} rows for processing`);
  const results = await Promise.allSettled(claimed.map((r) => processRow(r)));

  let updated = 0;
  for (let i = 0; i < results.length; i++) {
    const res = results[i];
    if (res.status === "fulfilled") {
      updated += res.value.updated;
    } else {
      console.error(`❌ Row ${i + 1} failed: ${res.reason.message || res.reason}`);
      await clearLocks([claimed[i].id]);
    }
  }

  console.log(`🌀 Batch complete — updated=${updated}/${claimed.length}`);

  if (updated === 0) {
    console.log("😴 No updates — sleeping 2 minutes before exit...");
    await sleep(120000);
  }

  console.log("🏁 Worker completed — shutting down to minimize cost.");
  process.exit(0);
})();
