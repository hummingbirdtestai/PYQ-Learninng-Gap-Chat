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
// PROMPT BUILDER
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
- Use **Unicode MarkUp** for **bold**, *italic*, superscripts (Na⁺, Ca²⁺), subscripts (H₂O), arrows (→), and symbols (±, ↑, ↓, ∆).
- **No explanations**, **no extra text**, and **no markdown fences**.
- Output must be **pure JSON only** (single array [ ... ]) with commas between all 30 objects.
- If you can’t make 30 due to token limit, still return valid JSON.

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

// ✅ Correct param for GPT-5 models
async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages,
      max_completion_tokens: 6000,
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
// ROBUST JSON PARSER
// ─────────────────────────────────────────────
function safeParseJSON(raw) {
  if (!raw || raw.length < 10) return [];

  // Detect obvious truncation
  if (raw.endsWith('"') || raw.endsWith(',') || raw.endsWith('{')) {
    console.warn("⚠️ Detected truncated output — discarding for retry");
    return [];
  }

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

  const open = (cleaned.match(/{/g) || []).length;
  const close = (cleaned.match(/}/g) || []).length;
  if (open > close) cleaned += "}".repeat(open - close);

  try {
    return JSON.parse(cleaned);
  } catch {
    try {
      const fixed = cleaned.replace(/[^}]*$/, "}]");
      return JSON.parse(fixed);
    } catch {
      console.error("❌ JSON parse error. Snippet:", cleaned.slice(0, 400));
      return [];
    }
  }
}

// ─────────────────────────────────────────────
// LOCK SYSTEM
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
    .is("battle_mcqs", null)
    .is("mentor_reply_lock", null)
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
// PROCESS ONE ROW (split into 10+10+10)
// ─────────────────────────────────────────────
async function processRow(row) {
  const concept = row.concept_final;
  if (!concept || !concept.trim()) throw new Error("Empty concept_final");

  async function generateBatch(batchNum, start, end) {
    const subPrompt = `
${buildPrompt(concept)}

Now generate only **MCQs ${start}–${end}** following the same rules.
`.trim();

    const raw = await callOpenAI([{ role: "user", content: subPrompt }]);
    return safeParseJSON(raw);
  }

  let allMCQs = [];
  try {
    const part1 = await generateBatch(1, 1, 10);
    const part2 = await generateBatch(2, 11, 20);
    const part3 = await generateBatch(3, 21, 30);
    allMCQs = [...part1, ...part2, ...part3];
  } catch (err) {
    console.error(`❌ JSON parse failed for row ${row.id}: ${err.message}`);
    await supabase
      .from("flashcard_raw")
      .update({
        battle_mcqs_final: null,
        error_log: `PARSE_FAIL: ${err.message}`,
        mentor_reply_lock: null,
        mentor_reply_lock_at: null,
        updated_at: new Date().toISOString(),
      })
      .eq("id", row.id);
    return { updated: 0 };
  }

  // 🧩 Safety: skip saving empty results
  if (!Array.isArray(allMCQs) || allMCQs.length === 0) {
    console.warn(`⚠️ Skipping save for row ${row.id} — empty MCQ array`);
    await supabase
      .from("flashcard_raw")
      .update({
        battle_mcqs_final: null,
        error_log: "EMPTY_ARRAY",
        mentor_reply_lock: null,
        mentor_reply_lock_at: null,
        updated_at: new Date().toISOString(),
      })
      .eq("id", row.id);
    return { updated: 0 };
  }

  // ✅ Save valid MCQs
  const { error: e3 } = await supabase
    .from("flashcard_raw")
    .update({
      battle_mcqs_final: allMCQs,
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
