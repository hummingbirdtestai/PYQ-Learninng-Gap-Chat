require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.IMAGE_MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.IMAGE_MCQ_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.IMAGE_MCQ_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.IMAGE_MCQ_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.IMAGE_MCQ_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `image-mcq-worker-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

const TABLE_NAME   = "mock_tests_phases";
const LOCK_COL     = "image_job_lock";
const LOCK_AT_COL  = "image_job_lock_at";

console.log("🚀 IMAGE MCQ WORKER STARTED:", WORKER_ID);

// ─────────────────────────────────────────────
// PROMPT (EXACTLY AS PROVIDED)
// ─────────────────────────────────────────────
function buildPrompt(imageDescription) {
  return `
You are a 30-year experienced NEET-PG / INI-CET paper setter, examiner, and question bank architect.

Your task is to generate a NEETPG-style Integrated Image-Based MCQ.

✔ Tests process understanding  
✔ Requires 3-step reasoning chain  
✔ Must be a typical MCQ asked in NEETPG Exam where the image can fit into the Clinical Case Vignette  

STEM RULES:
- Stem MUST be more than 15 words and less than 20 words.
- MUST include the exact phrase: "Check the Image given."
- MUST NOT contain any imaging/radiology description or keywords.
- The stem MUST contain ONLY the phrase "Check the Image given." for image reference.

OPTIONS RULES:
- Provide exactly 4 options: A, B, C, D
- Each option MUST be less than 5 words
- Each option MUST be a string
- Options must be clinically distinct

IMAGE HANDLING RULE (HIGHEST PRIORITY):
- NEVER describe, summarize, or hint at the image inside the "stem".
- The full image description MUST be placed ONLY in the "image_description" field.
- The model MUST NOT leak any part of "image_description" into the stem.

OUTPUT VALIDATION RULE:
✔ Stem contains ONLY "Check the Image given."
✔ No radiology/imaging keywords present in stem
✔ Image description exists ONLY in "image_description"

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
    "high_yield_facts": "string",
    "image_description": "${imageDescription}"
  }
]


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
    throw new Error("No valid JSON array detected");

  return JSON.parse(cleaned.substring(first, last + 1));
}

function validateOutput(json) {
  if (!Array.isArray(json) || json.length !== 1)
    throw new Error("Output must be single-item array");

  const mcq = json[0];

  if (!mcq.stem.includes("Check the Image given."))
    throw new Error("Stem missing required phrase");

  const words = mcq.stem.split(/\s+/).length;
  if (words <= 15 || words >= 20)
    throw new Error("Stem word count invalid");

  if (!["A","B","C","D"].includes(mcq.correct_answer))
    throw new Error("Invalid correct_answer");

  if (typeof mcq.options !== "object" || Object.keys(mcq.options).length !== 4)
    throw new Error("Options invalid");

  return true;
}

// ─────────────────────────────────────────────
// OPENAI CALL
// ─────────────────────────────────────────────
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

  await supabase
    .from(TABLE_NAME)
    .update({ [LOCK_COL]: null, [LOCK_AT_COL]: null })
    .lt(LOCK_AT_COL, cutoff);

  const { data } = await supabase
    .from(TABLE_NAME)
    .select("id, image_description")
    .not("image_description", "is", null)
    .is("image_mcqs", null)
    .is(LOCK_COL, null)
    .limit(limit);

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
    .select("id, image_description");

  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  try {
    const prompt = buildPrompt(row.image_description);
    const raw = await callOpenAI(prompt);
    const json = extractJson(raw);
    validateOutput(json);

    await supabase
      .from(TABLE_NAME)
      .update({
        image_mcqs: json,
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
