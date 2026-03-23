require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.REPLACE_MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.REPLACE_MCQ_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.REPLACE_MCQ_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.REPLACE_MCQ_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.REPLACE_MCQ_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `replace-mcq-worker-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

const TABLE_NAME = "mock_tests_phases";
const IN_COL     = "concept";
const OUT_COL    = "replace_mcq";
const LOCK_COL   = "image_job_lock";
const LOCK_AT_COL= "image_job_lock_at";

console.log("🚀 REPLACE MCQ WORKER STARTED:", WORKER_ID);

// ─────────────────────────────────────────────
// PROMPT (USE EXACTLY AS PROVIDED — NO CHANGE)
// ─────────────────────────────────────────────
function buildPrompt(conceptJson) {
  return `
You are a 30-year experienced NEET-PG / INI-CET paper setter, examiner, and question bank architect.

The above object is based on a High Yield Topics.

Identify that topic and on that topic identify most tested NEETPG CHEAT SHEET, EXAM TRAP frequently tested in NEETPG Exam.

Your task is to generate a NEETPG-style Integrated MCQ on that High Yield fact based on NEETPG Pattern exactly that comes in future NEETPG Exam.

Be very specific and think like NEETPG Paper Setter and perfectly MCQ you offer is exactly like it is asked in NEETPG Exam ditto.

Use the content given only to identify the topic but frame MCQ on the High Yield fact other than covered in that exact content in it.

✔ Tests process understanding  
✔ Requires 3-step reasoning chain  
✔ Must be a typical MCQ asked in NEETPG Exam  

STEM RULES:
- Stem MUST be more than 15 words and less than 20 words.

OPTIONS RULES:
- Provide exactly 4 options: A, B, C, D
- Each option MUST be less than 5 words
- Each option MUST be a string

FORMATTING RULES:
- Use Unicode for Superscripts, Subscripts, Greek letters, symbols, and Math.
- Use Markdown for **bold**, *italic*, and ***bold italic*** to highlight most important words in value of key stem of mcq and the values of keys learning_gap, high_yield_facts

CORRECT_ANSWER RULES:
✔ MUST be string  
✔ MUST use uppercase letters only (A, B, C, D)  
✔ NO commas, no lowercase, no symbols  

OUTPUT FORMAT (STRICT JSON ONLY):

[
  {
    "stem": "string",
    "uuid": "string",
    "mcq_key": "string",
    "options": {
      "A": "string",
      "B": "string",
      "C": "string",
      "D": "string"
    },
    "learning_gap": "string",
    "correct_answer": "string",
    "high_yield_facts": "string"
  }
]

INPUT:
${JSON.stringify(conceptJson)}

No explanation.
No markdown.
No commentary.
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

function extractJson(text) {
  if (!text) throw new Error("Empty model response");

  let cleaned = text.replace(/```json/gi, "").replace(/```/g, "").trim();

  const first = cleaned.indexOf("[");
  const last  = cleaned.lastIndexOf("]");

  if (first === -1 || last === -1)
    throw new Error("No valid JSON detected");

  return JSON.parse(cleaned.substring(first, last + 1));
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }]
    });

    return resp.choices?.[0]?.message?.content?.trim();

  } catch (e) {
    if (isRetryable(e) && attempt <= 2) {
      await sleep(800 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

// ─────────────────────────────────────────────
// CLAIM ROWS
// ─────────────────────────────────────────────
async function claimRows(limit) {

  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // release stale locks
  await supabase
    .from(TABLE_NAME)
    .update({ [LOCK_COL]: null, [LOCK_AT_COL]: null })
    .lt(LOCK_AT_COL, cutoff);

  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(`id, ${IN_COL}`)
    .not(IN_COL, "is", null)
    .is(OUT_COL, null)
    .eq("is_mcq_image_type", false)
    .gte("exam_serial", 31)
    .lte("exam_serial", 100)
    .is(LOCK_COL, null)
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  const { data: locked } = await supabase
    .from(TABLE_NAME)
    .update({
      [LOCK_COL]: WORKER_ID,
      [LOCK_AT_COL]: new Date().toISOString()
    })
    .in("id", ids)
    .is(LOCK_COL, null)
    .select(`id, ${IN_COL}`);

  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  try {

    const raw  = await callOpenAI(buildPrompt(row[IN_COL]));
    const json = extractJson(raw);

    await supabase
      .from(TABLE_NAME)
      .update({
        [OUT_COL]: json,
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id);

    console.log("✅ Saved:", row.id);

  } catch (err) {

    console.error("❌ Failed:", row.id, err.message);

    await supabase
      .from(TABLE_NAME)
      .update({
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id);
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {

  while (true) {

    const rows = await claimRows(LIMIT);

    if (!rows.length) {
      await sleep(SLEEP_MS);
      continue;
    }

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      await Promise.allSettled(batch.map(processRow));
    }
  }
})();
