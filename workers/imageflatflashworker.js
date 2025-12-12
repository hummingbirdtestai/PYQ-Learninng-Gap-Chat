require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

//──────────────────────────────────────────────
// SETTINGS
//──────────────────────────────────────────────
const MODEL        = process.env.IMAGE_FLASH_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.IMAGE_FLASH_LIMIT || "150", 10);
const CONCURRENCY  = parseInt(process.env.IMAGE_FLASH_CONCURRENCY || "10", 10);
const SLEEP_MS     = parseInt(process.env.IMAGE_FLASH_LOOP_SLEEP_MS || "200", 10);
const LOCK_TTL_MIN = parseInt(process.env.IMAGE_FLASH_LOCK_TTL_MIN || "10", 10);
const WORKER_ID    = process.env.WORKER_ID || `img-flash-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

//──────────────────────────────────────────────
// PROMPT (AS-IS)
//──────────────────────────────────────────────
function buildPrompt(imageKeyword) {
  return `
You are an expert NEET-PG / USMLE / NBME question writer with 30+ years experience in creating image based MCQs

Task:
Using the given long-tail image keyword, generate ONE flash-card–styled, USMLE-level clinical case vignette whose answer is derived only by interpreting the image.

ABSOLUTE RULES (Do NOT violate):
Output ONLY valid JSON.
Output ONLY TWO keys: "Question" and "Answer".
NO arrays.
NO nested objects.
NO explanations.
NO comments.
Do NOT describe or mention image findings.
Use only the phrase → “Based on the given image” (once).
Question must be USMLE-style, clinically complex, moderate–hard.
Answer must be 2–3 words only.
Use bold, italic, arrows → ↑ ↓, Unicode ₁₂³⁺⁻, Greek α β Δ μ, and minimal emojis (✅ ❌ 💡) where relevant.
Do NOT use phrases like image-based MCQ, CT, radiology, scan, findings, shows, demonstrates.
Do NOT add headings or extra text.

FINAL OUTPUT FORMAT (exact):
{
  "Question": "USMLE-style clinical vignette ending with a single question, including the phrase **Based on the given image**.",
  "Answer": "2–3 word diagnosis"
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
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(
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
      await sleep(400 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw e;
  }
}

//──────────────────────────────────────────────
// CLAIM ROWS (image_flash_card IS NULL)
//──────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // Release stale locks
  await supabase
    .from("images_flatten")
    .update({ two_images_lock: null, two_images_lock_at: null })
    .lt("two_images_lock_at", cutoff);

  // Pick rows
  const { data: rows, error } = await supabase
    .from("images_flatten")
    .select("id, image")
    .is("image_flash_card", null)
    .is("two_images_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // Lock them
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
// PROCESS ONE ROW
//──────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.image);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    console.error("❌ Invalid JSON:", raw);
    throw new Error("Invalid JSON from OpenAI");
  }

  await supabase
    .from("images_flatten")
    .update({
      image_flash_card: parsed,
      two_images_lock: null,
      two_images_lock_at: null
    })
    .eq("id", row.id);
}

//──────────────────────────────────────────────
// CONCURRENCY POOL
//──────────────────────────────────────────────
async function processBatch(rows) {
  let index = 0;

  async function worker() {
    while (index < rows.length) {
      const row = rows[index++];
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

  await Promise.all(Array.from({ length: CONCURRENCY }, worker));
}

//──────────────────────────────────────────────
// MAIN LOOP
//──────────────────────────────────────────────
(async function main() {
  console.log(`🟦 Image Flash-Card Worker Started | batch=${LIMIT} | concurrency=${CONCURRENCY}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      await processBatch(claimed);
      console.log(`✅ Processed ${claimed.length} image_flash_card rows`);
    } catch (e) {
      console.error("❌ Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
