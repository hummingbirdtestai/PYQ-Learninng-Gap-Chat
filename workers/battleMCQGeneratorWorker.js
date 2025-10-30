// /workers/battleMCQGeneratorWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL = process.env.SUBJECT_IMAGE_MCQ_MODEL || "gpt-5-mini";
const LIMIT = parseInt(process.env.SUBJECT_IMAGE_MCQ_LIMIT || "50", 10);
const SLEEP_MS = parseInt(process.env.SUBJECT_IMAGE_MCQ_SLEEP_MS || "1000", 10);
const LOCK_TTL_MIN = parseInt(process.env.SUBJECT_IMAGE_MCQ_LOCK_TTL_MIN || "15", 10);
const WORKER_ID =
  process.env.WORKER_ID ||
  `battle-mcq-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;

// ─────────────────────────────────────────────
// PROMPT BUILDER (UNCHANGED FROM YOUR VERSION)
// ─────────────────────────────────────────────
function buildPrompt(conceptText) {
  return `
You are a **30 Years experienced NEETPG Paper Setter**, creating exam-level questions based on **NEETPG PYQs**, written in **USMLE-style** as seen in **Amboss, UWorld, First Aid, and NBME**.

Create **30 MCQs** that combine **clinical case vignettes** and **single-liner high-yield facts**, covering **all the most tested and high-yield points** related to the topic given.  
These MCQs should be **NEETPG PYQ-based** and **could appear exactly as-is in the NEETPG Exam**.

**Prompt Rules:**
- Output strictly as a **valid JSON array of 30 objects**.
- Each object must follow this format:
  {
    "Stem": "…",
    "Options": { "A": "…", "B": "…", "C": "…", "D": "…" },
    "Correct Answer": "A|B|C|D"
  }
- Each question should sound **USMLE-styled** — logical, clinical, or concept-driven — not random trivia.
- Each “Stem” must begin directly with the question text only.  
  ⚠ **Do not include labels like “Clinical vignette:”, “High-yield:”, “Exam tip:”, or “Single-line fact:” — just start the question directly.**
- Use **Unicode MarkUp** to highlight:
  - **bold**, *italic*
  - Superscripts (e.g., Na⁺, Ca²⁺)
  - Subscripts (e.g., H₂O)
  - Arrows (→)
  - Symbols (±, ↑, ↓, ∆)
  - Equations where appropriate
- **No explanations**, **no commentary**, **no extra text**, and **no markdown/code fences**.
- Output must be **pure JSON only** — a single valid JSON array enclosed in [ ] with commas between all 30 objects.
- ⚠ Ensure there are no trailing commas and the output ends with a closing bracket (]).
- If you can’t make 30 due to token limit, still return valid JSON.

**INPUT CONCEPT:**
${conceptText}
`.trim();
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function isRetryable(e) {
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(
    String(e?.message || e)
  );
}

// ✅ FIXED: use `max_completion_tokens` instead of `max_tokens`
async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages,
      max_completion_tokens: 4000, // <-- correct param name
    });
    return resp.choices?.[0]?.message?.content?.trim() || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      console.warn(`⚠️ Retry attempt ${attempt} due to transient error`);
      await sleep(400 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    console.error("❌ OpenAI API call failed:", e.message || e);
    throw e;
  }
}

// ─────────────────────────────────────────────
// ROBUST JSON PARSER (AUTO-REPAIRS TRUNCATED OUTPUT)
// ─────────────────────────────────────────────
function safeParseJSON(raw) {
  let cleaned = raw
    .trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```/i, "")
    .replace(/```$/i, "")
    .replace(/,\s*}/g, "}")
    .replace(/,\s*]/g, "]")
    .replace(/}\s*{/g, "}, {"); // add commas if missing

  // Ensure array brackets
  if (!cleaned.startsWith("[")) cleaned = "[" + cleaned;
  if (!cleaned.endsWith("]")) cleaned = cleaned + "]";

  // Count braces to close incomplete objects
  const open = (cleaned.match(/{/g) || []).length;
  const close = (cleaned.match(/}/g) || []).length;
  if (open > close) cleaned += "}".repeat(open - close);

  try {
    return JSON.parse(cleaned);
  } catch (e1) {
    // fallback attempt
    let fallback = cleaned;
    if (!fallback.trim().endsWith("}]")) fallback = fallback.replace(/[^}]*$/, "}]");
    try {
      return JSON.parse(fallback);
    } catch (e2) {
      console.error("❌ JSON parse error even after cleanup. Snippet:", cleaned.slice(0, 400));
      throw new Error("Invalid JSON output from OpenAI after cleanup attempts");
    }
  }
}

// ─────────────────────────────────────────────
// LOCK SYSTEM
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // Free stale locks
  await supabase
    .from("flashcard_raw")
    .update({ mentor_reply_lock: null, mentor_reply_lock_at: null })
    .lt("mentor_reply_lock_at", cutoff);

  // Get rows where battle_mcqs is null
  const { data: candidates, error: e1 } = await supabase
    .from("flashcard_raw")
    .select("id, concept_final")
    .is("battle_mcqs", null)
    .is("mentor_reply_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (e1) throw new Error(e1.message);
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
// PROCESS ONE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const concept = row.concept_final;
  if (!concept || !concept.trim()) throw new Error("Empty concept_final");

  const prompt = buildPrompt(concept);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);

  let mcqJSON;
  try {
    mcqJSON = safeParseJSON(raw);
  } catch (err) {
    console.error(`❌ JSON parse failed for row ${row.id}: ${err.message}`);
    await supabase
      .from("flashcard_raw")
      .update({
        battle_mcqs_final: null,
        error_log: raw.slice(0, 2000),
        mentor_reply_lock: null,
        mentor_reply_lock_at: null,
        updated_at: new Date().toISOString(),
      })
      .eq("id", row.id);
    return { updated: 0 };
  }

  const { error: e3 } = await supabase
    .from("flashcard_raw")
    .update({
      battle_mcqs_final: mcqJSON,
      mentor_reply_lock: null,
      mentor_reply_lock_at: null,
      updated_at: new Date().toISOString(),
    })
    .eq("id", row.id);

  if (e3) throw new Error(e3.message);
  return { updated: 1 };
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🚀 BattleMCQ Generator Worker Started | model=${MODEL} | limit=${LIMIT}`);
  console.log(`Worker ID: ${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        console.log("⏸️ No unlocked rows found — sleeping...");
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows for processing`);

      const results = await Promise.allSettled(claimed.map((r) => processRow(r)));

      let updated = 0;
      for (let i = 0; i < results.length; i++) {
        const res = results[i];
        if (res.status === "fulfilled") {
          console.log(`✅ Row ${i + 1}: MCQs generated`);
          updated += res.value.updated;
        } else {
          console.error(`❌ Row ${i + 1} failed: ${res.reason.message || res.reason}`);
          await clearLocks([claimed[i].id]);
        }
      }

      console.log(`🌀 Batch complete — updated=${updated}/${claimed.length}`);
    } catch (err) {
      console.error("💥 Main loop error:", err.message || err);
      await sleep(3000);
    }
  }
})();
