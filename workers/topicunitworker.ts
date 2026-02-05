require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_LIMIT || "150", 10);
const BATCH_SIZE   = parseInt(process.env.MCQ_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.MCQ_LOOP_SLEEP_MS || "200", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_LOCK_TTL_MIN || "10", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `topic-concept-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — LOGIC UNCHANGED)
// ─────────────────────────────────────────────
function buildPrompt(topic) {
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
     - stem
     - options (A–D)
     - correct_answer
     - explanation
     - wrong_answers_explained
     - exam_trap
   - "student_doubts" → EXACTLY 3 objects

4. Organize ALL high-yield facts into EXACTLY 10 logical conceptual buckets.
   - No repetition
   - Mutually exclusive and collectively exhaustive

5. MCQs must be reasoning-based and exam-level.

6. Language rules:
   - Clinically precise
   - Exam-safe
   - Textbook-aligned

7. Formatting rules inside JSON strings:
   - Use **bold** and ***bold-italic***
   - Use Unicode (→ ↑ ↓ ≥ ≤ ₁ ₂ ⁺ ⁻)

8. Subject is ALWAYS "General Medicine".

9. If any field is missing or extra → INVALID.

10. Use topic verbatim.

Generate the JSON now.

TOPIC:
${topic}
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
// SAFE JSON PARSER (OBJECT ONLY)
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

  // Clear expired locks
  await supabase
    .from("topic_teaching_units")
    .update({
      topic_concept_json_lock: null,
      topic_concept_json_lock_at: null
    })
    .lt("topic_concept_json_lock_at", cutoff);

  // Fetch eligible rows
  const { data, error } = await supabase
    .from("topic_teaching_units")
    .select("id, topic")
    .is("topic_concept_json", null)
    .not("topic", "is", null)
    .is("topic_concept_json_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  // Lock rows
  const { data: locked, error: err2 } = await supabase
    .from("topic_teaching_units")
    .update({
      topic_concept_json_lock: WORKER_ID,
      topic_concept_json_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("topic_concept_json_lock", null)
    .select("id, topic");

  if (err2) throw err2;
  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  let raw = await callOpenAI(buildPrompt(row.topic));
  let parsed;

  try {
    parsed = safeParseJson(raw);
  } catch {
    raw = await callOpenAI(buildPrompt(row.topic));
    parsed = safeParseJson(raw);
  }

  await supabase
    .from("topic_teaching_units")
    .update({
      topic_concept_json: parsed,
      topic_concept_json_lock: null,
      topic_concept_json_lock_at: null,
      updated_at: new Date().toISOString()
    })
    .eq("id", row.id);

  return true;
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 TOPIC → CONCEPT JSON WORKER STARTED | ${WORKER_ID}`);

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

        const results = await Promise.allSettled(
          batch.map(processRow)
        );

        results.forEach((res, idx) => {
          if (res.status !== "fulfilled") {
            console.error(`❌ Failed row ${batch[idx].id}`, res.reason);
          }
        });
      }
    } catch (e) {
      console.error("❌ Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
