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
  `concept-mcq-${process.pid}-${Math.random().toString(36).slice(2, 6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — DO NOT TOUCH)
// ─────────────────────────────────────────────
function buildPrompt(conceptJson) {
  return `
You are a senior NEET-PG and USMLE question writer with 30+ years of experience, trained in NBME-style blueprinting, cognitive-level balancing, and recursive learning-gap remediation.

Your task is to generate EXACTLY 10 MCQs, each mapped to a distinct concept with a numeric key value from 1 → 10.

ABSOLUTE RULES (NON-NEGOTIABLE)

1️⃣ Concept Mapping
Each MCQ MUST have a numeric key "concept_value"
MUST be an integer
Allowed values: 1 to 10 only
Each value used once and only once

2️⃣ Difficulty & Pedagogy
Difficulty level: NEET-PG / USMLE / NBME = Moderate → Hard
No trivia
No memory-only facts unless clinically meaningful
NO “EXCEPT” questions
ONE MCQ = ONE core concept
ONE best answer only
Stems must resemble real exam questions (clinical vignette or high-yield single-liner)

3️⃣ Formatting Rules (STRICT)
Use bold, italic
Use Unicode arrows (→ ↑ ↓)
Use subscripts/superscripts (₁₂³⁺⁻)
Use Greek letters (α β Δ μ)
Minimal emojis only where relevant (✅ ❌ 💡)

❌ NO markdown outside JSON
❌ NO extra keys
❌ NO trailing commas
❌ NO explanations outside JSON

MANDATORY MCQ JSON TEMPLATE
Each MCQ MUST follow this structure exactly:

{
  "concept_value": 1,
  "stem": "",
  "options": {
    "A": "",
    "B": "",
    "C": "",
    "D": ""
  },
  "feedback": {
    "A": "",
    "B": "",
    "C": "",
    "D": ""
  },
  "learning_gap": "💡 One-line high-yield takeaway",
  "correct_answer": ""
}

FEEDBACK RULES (VERY IMPORTANT)
For EVERY option (A–D):
✅ If CORRECT:
- Explicitly reinforce the tested concept
- Explain why it is correct
- Include mechanisms, associations, or exam pearls

❌ If WRONG:
- Identify the recursive learning gap
- Explain why a student might choose it
- Remediate the underlying misconception
- Do NOT repeat explanations across options

OUTPUT FORMAT (FINAL CHECK)
• Output MUST be a single JSON ARRAY
• Array MUST contain EXACTLY 10 MCQs
• Concept values MUST be 1 through 10
• JSON must be strictly valid
• No text before or after JSON

CONTENT (Concept JSON):
${JSON.stringify(conceptJson)}
`;
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(s);
}

async function callOpenAI(prompt, attempt = 1) {
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
// SAFE JSON PARSE
// ─────────────────────────────────────────────
function safeParseJson(raw) {
  const txt = raw
    .trim()
    .replace(/^```json/i, "")
    .replace(/^```/, "")
    .replace(/```$/, "");

  const match = txt.match(/\[[\s\S]*\]/);
  if (!match) throw new Error("❌ No JSON array found");

  return JSON.parse(match[0]);
}

// ─────────────────────────────────────────────
// CLAIM ROWS
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // 1️⃣ Clear expired locks
  await supabase
    .from("all_subjects_raw")
    .update({
      concept_mcq_json_lock: null,
      concept_mcq_json_lock_at: null
    })
    .lt("concept_mcq_json_lock_at", cutoff);

  // 2️⃣ Fetch rows needing MCQs
  const { data: rows, error } = await supabase
    .from("all_subjects_raw")
    .select("id, concept_json")
    .not("concept_json", "is", null)
    .is("concept_mcq_json", null)
    .is("concept_mcq_json_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // 3️⃣ Lock rows
  const { data: locked, error: err2 } = await supabase
    .from("all_subjects_raw")
    .update({
      concept_mcq_json_lock: WORKER_ID,
      concept_mcq_json_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("concept_mcq_json_lock", null)
    .select("id, concept_json");

  if (err2) throw err2;
  return locked || [];
}

// ─────────────────────────────────────────────
// CLEAR LOCKS
// ─────────────────────────────────────────────
async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("all_subjects_raw")
    .update({
      concept_mcq_json_lock: null,
      concept_mcq_json_lock_at: null
    })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.concept_json);

  let raw = await callOpenAI(prompt);
  let parsed;

  try {
    parsed = safeParseJson(raw);
  } catch {
    raw = await callOpenAI(prompt);
    parsed = safeParseJson(raw);
  }

  if (!Array.isArray(parsed) || parsed.length !== 10) {
    throw new Error("❌ Output is not an array of exactly 10 MCQs");
  }

  await supabase
    .from("all_subjects_raw")
    .update({
      concept_mcq_json: parsed,
      concept_mcq_json_lock: null,
      concept_mcq_json_lock_at: null
    })
    .eq("id", row.id);

  return true;
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 CONCEPT MCQ WORKER STARTED | ${WORKER_ID} | model=${MODEL}`);

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
          batch.map(r => processRow(r))
        );

        results.forEach((res, idx) => {
          if (res.status === "fulfilled") {
            console.log(`   ✅ MCQs generated`);
          } else {
            console.error(`   ❌ Failed row ${batch[idx].id}`, res.reason);
            clearLocks([batch[idx].id]);
          }
        });
      }
    } catch (e) {
      console.error("❌ Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
