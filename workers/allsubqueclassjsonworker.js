require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.CONCEPT_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CONCEPT_LIMIT || "30", 10);
const BATCH_SIZE   = parseInt(process.env.CONCEPT_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.CONCEPT_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.CONCEPT_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `class-json-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — UNCHANGED)
// ─────────────────────────────────────────────
function buildPrompt(question) {
  return `
You are a medical exam-content generation engine.

Your task is to generate a STRICTLY VALID JSON object for ONE medical topic from the subject of General Medicine.

This is an EXAM-FIRST FACT COMPRESSION task.

You MUST follow these rules exactly:

1. Output ONLY a single JSON object.
   - No markdown outside JSON
   - No commentary
   - No explanations
   - No preamble or epilogue

2. The JSON must contain EXACTLY:
   - "topic"
   - "subject"
   - "concept_1" through "concept_10"

3. Each concept_N object MUST contain:
   - "title"
   - "concept" → array of high-yield exam facts
   - "mcq" → a USMLE / NBME / AMBOSS-level clinical vignette MCQ
     - stem (paragraph-length clinical case)
     - options (A–D)
     - correct_answer
     - explanation
     - wrong_answers_explained (for A–D except correct)
     - exam_trap
   - "student_doubts" → EXACTLY 3 objects, each with:
     - doubt
     - answer

4. Organize ALL high-yield facts into EXACTLY 10 logical conceptual buckets.
   - Do NOT repeat facts across concepts.
   - Concepts must be mutually exclusive and collectively exhaustive.
   - Use standard Harrison / Davidson / CMDT / UWorld logic.

5. MCQs must:
   - Be paragraph-length clinical vignettes
   - Test reasoning, not recall
   - Match USMLE / NEET-PG difficulty
   - Have ONE best answer

6. Language rules:
   - Clinically precise
   - Exam-safe
   - Textbook-aligned
   - No motivational language
   - No fluff
   - No teaching commentary

7. Compulsary strict Formatting rules inside JSON to text in:
   - "concept" → array of high-yield exam facts
   - "mcq" stem
   - "mcq" explanation
   - "mcq" wrong_answers_explained
   - "exam_trap"
   - "student_doubts" fields ("doubt" and "answer")

   Formatting rules:
   - Use Markdown for **bold** and ***bold-italic*** to highlight important key words
   - Use Unicode symbols (→ ↑ ↓ ≥ ≤ ₁ ₂ ⁺ ⁻)
   - Emojis ONLY if they add medical meaning (use sparingly)

8. Assume the output will be:
   - Parsed programmatically
   - Rendered in a mobile UI
   - Used for adaptive testing and doubt resolution

9. If any field is missing, malformed, or extra:
   - The output is considered INVALID.

10. Topic input will be provided dynamically.
    - Use it verbatim as the "topic" value.
    - Subject is ALWAYS "General Medicine".

Generate the JSON now.

QUESTION:
${question}
`;
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i
    .test(String(e?.message || e));
}

async function callOpenAI(prompt, attempt = 1) {
  if (!prompt || typeof prompt !== "string" || !prompt.trim()) {
    throw new Error("❌ Invalid prompt");
  }

  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }]
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 2) {
      await sleep(600 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

// ─────────────────────────────────────────────
// SAFE JSON PARSER
// ─────────────────────────────────────────────
function safeParseJson(raw) {
  const cleaned = raw
    .trim()
    .replace(/^```json/i, "")
    .replace(/^```/, "")
    .replace(/```$/, "");

  const match = cleaned.match(/\{[\s\S]*\}/);
  if (!match) throw new Error("❌ No JSON object found");

  return JSON.parse(match[0]);
}

// ─────────────────────────────────────────────
// CLAIM ROWS (LOCK-ONLY STRATEGY)
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // 1️⃣ Clear expired locks
  await supabase
    .from("all_subjects_raw")
    .update({
      concept_lock: null,
      concept_lock_at: null
    })
    .lt("concept_lock_at", cutoff);

  // 2️⃣ Fetch eligible rows
  const { data, error } = await supabase
    .from("all_subjects_raw")
    .select("id, question")
    .eq("subject", "General Medicine")
    .not("question", "is", null)
    .is("class_json", null)
    .is("concept_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  // 3️⃣ Lock rows
  const { data: locked, error: err2 } = await supabase
    .from("all_subjects_raw")
    .update({
      concept_lock: WORKER_ID,
      concept_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("concept_lock", null)
    .select("id, question");

  if (err2) throw err2;
  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS SINGLE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  let raw = await callOpenAI(buildPrompt(row.question));
  let parsed;

  try {
    parsed = safeParseJson(raw);
  } catch {
    raw = await callOpenAI(buildPrompt(row.question));
    parsed = safeParseJson(raw);
  }

  await supabase
    .from("all_subjects_raw")
    .update({
      class_json: parsed,
      concept_lock: null,
      concept_lock_at: null
    })
    .eq("id", row.id);

  return true;
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 QUESTION → CLASS_JSON WORKER STARTED | ${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows`);

      for (let i = 0; i < claimed.length; i += BATCH_SIZE) {
        const batch = claimed.slice(i, i + BATCH_SIZE);

        await Promise.allSettled(batch.map(processRow));
      }
    } catch (e) {
      console.error("❌ Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
