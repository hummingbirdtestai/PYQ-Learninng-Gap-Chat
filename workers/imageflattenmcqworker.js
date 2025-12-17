require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

//──────────────────────────────────────────────
// SETTINGS (ENV-DRIVEN)
//──────────────────────────────────────────────
const MODEL        = process.env.IMAGE_MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.IMAGE_MCQ_LIMIT || "100", 10);
const CONCURRENCY  = parseInt(process.env.IMAGE_MCQ_CONCURRENCY || "8", 10);
const SLEEP_MS     = parseInt(process.env.IMAGE_MCQ_LOOP_SLEEP_MS || "300", 10);
const LOCK_TTL_MIN = parseInt(process.env.IMAGE_MCQ_LOCK_TTL_MIN || "10", 10);
const WORKER_ID    = process.env.WORKER_ID ||
  `img-mcq-${process.pid}-${Math.random().toString(36).slice(2, 6)}`;

//──────────────────────────────────────────────
// PROMPT (AS-IS — DO NOT MODIFY)
//──────────────────────────────────────────────
function buildPrompt(imageKeyword) {
  return `
You are an expert NEET-PG / USMLE / NBME question writer with 30+ years experience in creating image based MCQs

Task:
Using the given long-tail image keyword, generate ONE , USMLE-level clinical case vignette based MCQ whose answer is derived only by interpreting the image.

ABSOLUTE RULES (Do NOT violate):
Output ONLY valid JSON.
Do NOT describe or mention image findings.
Use only the phrase → “Based on the given image” (once).
MCQs must be USMLE-style, clinically complex, moderate–hard.
Do NOT use phrases like image-based MCQ, CT, radiology, scan, findings, shows, demonstrates.
Use Markup bold, italic, Unicode arrows (→ ↑ ↓), subscripts/superscripts (₁₂³⁺⁻), Greek (α β Δ μ), and minimal emojis (✅ ❌ 💡) wherever relevant.
"stem": Real NEET-PG–style question.
"feedback": Concise NEET-PG hack-style explanation.
No “EXCEPT” questions.
Output exactly 1 MCQ.

JSON TEMPLATE:
{
  "stem": "",
  "options": { "A": "", "B": "", "C": "", "D": "" },
  "feedback": "",
  "correct_answer": ""
}

IMAGE KEYWORD:
${imageKeyword}
`.trim();
}

//──────────────────────────────────────────────
// HELPERS
//──────────────────────────────────────────────
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

function isRetryable(e) {
  return /429|timeout|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(
    String(e?.message || e)
  );
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages,
      response_format: { type: "json_object" }
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(500 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw e;
  }
}

//──────────────────────────────────────────────
// CLAIM ROWS (image_mcq IS NULL)
//──────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // 1️⃣ Release stale locks
  await supabase
    .from("images_flatten")
    .update({ two_images_lock: null, two_images_lock_at: null })
    .lt("two_images_lock_at", cutoff);

  // 2️⃣ Pick unlocked, unprocessed rows
  const { data: rows, error } = await supabase
    .from("images_flatten")
    .select("id, image")
    .is("image_mcq", null)
    .is("two_images_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  // 3️⃣ Lock rows
  const ids = rows.map(r => r.id);

  const { data: locked, error: lockErr } = await supabase
    .from("images_flatten")
    .update({
      two_images_lock: WORKER_ID,
      two_images_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .select("id, image");

  if (lockErr) throw lockErr;
  return locked || [];
}

//──────────────────────────────────────────────
// PROCESS SINGLE ROW
//──────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.image);

  const raw = await callOpenAI([
    { role: "user", content: prompt }
  ]);

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    console.error("❌ INVALID JSON:", raw);
    throw new Error("Invalid JSON from OpenAI");
  }

  await supabase
    .from("images_flatten")
    .update({
      image_mcq: parsed,
      two_images_lock: null,
      two_images_lock_at: null
    })
    .eq("id", row.id);
}

//──────────────────────────────────────────────
// CONCURRENCY POOL
//──────────────────────────────────────────────
async function processBatch(rows) {
  let idx = 0;

  async function worker() {
    while (idx < rows.length) {
      const row = rows[idx++];
      try {
        await processRow(row);
      } catch (e) {
        console.error(`❌ Row failed: ${row.id}`, e);
        await supabase
          .from("images_flatten")
          .update({ two_images_lock: null, two_images_lock_at: null })
          .eq("id", row.id);
      }
    }
  }

  await Promise.all(
    Array.from({ length: CONCURRENCY }, worker)
  );
}

//──────────────────────────────────────────────
// MAIN LOOP
//──────────────────────────────────────────────
(async function main() {
  console.log(
    `🟦 Image-MCQ Worker Started | batch=${LIMIT} | concurrency=${CONCURRENCY} | worker=${WORKER_ID}`
  );

  while (true) {
    try {
      const rows = await claimRows(LIMIT);
      if (!rows.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      await processBatch(rows);
      console.log(`✅ Generated MCQs for ${rows.length} images`);
    } catch (e) {
      console.error("❌ Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
