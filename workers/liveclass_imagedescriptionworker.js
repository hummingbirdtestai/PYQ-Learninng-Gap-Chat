require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.LIVE_BUCKET_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.LIVE_BUCKET_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.LIVE_BUCKET_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.LIVE_BUCKET_LOOP_SLEEP_MS || "300", 10);
const LOCK_TTL_MIN = parseInt(process.env.LIVE_BUCKET_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `live-bucket-worker-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

const TABLE_NAME = "live_class_schedule";
const IN_COL = "bucket";
const OUT_COL = "buket_image_description";
const LOCK_COL = "buket_image_description_lock";
const LOCK_AT_COL = "buket_image_description_lock_at";

// ─────────────────────────────────────────────
// PROMPT (USE EXACTLY AS GIVEN)
// ─────────────────────────────────────────────
function buildPrompt(bucketJson) {
  return `
you are 30 Year expert NEETPG and USMLE EXPERT , expert in framing Image based MCQs where a clinical image is a clue to diagnosis . The following are a List of new_topics , each with 5 Buckets of topics and each topic has 10 High Yield facts most tested in NEETPG . Create 5 Image descriptions to be searched in Google images and classical Clinical Case Vigneete styled image based MCQ Stem and answer asked , based on each each new_topic . Each new_topic has id Give the output as JSON as id, new_topic , image_1 , Clinical Case Vignette_1 , answer_1 ....image_5 ., Clinical Case Vignette_5 , answer_5Cover all new_topic without missing any in the JSON . The image should be most classical images often tested in images as clies in Image based MCQs in USMLE , NEETPG Exams and should be of the standard of Amboss, uworld , NBME, First-Aid.

You MUST return STRICT VALID JSON ONLY.
Do NOT return markdown.
Do NOT return explanation.
Do NOT return commentary.
Do NOT return code fences.
Do NOT omit any new_topic.
Do NOT change field names.
Do NOT add extra fields.

The response MUST follow EXACTLY this schema:

{
  "topics": [
    {
      "id": "string (uuid exactly as provided)",
      "new_topic": "string (exactly as provided)",
      "images": [
        {
          "image_description": "string (classical, searchable Google image description)",
          "clinical_case_vignette": "string (USMLE/NEETPG style stem, 3–6 lines)",
          "question": "string (single best answer question)",
          "answer": "string (single correct diagnosis or concept)"
        }
      ]
    }
  ]
}

Rules:
- Exactly 5 images per new_topic.
- Exactly 1 answer per image.
- Cover ALL new_topics provided in input.
- Preserve id exactly.
- Preserve new_topic text exactly.
- No missing topics.
- No extra topics.
- No reordering unless required.
- JSON must be valid and parsable by JSON.parse().
- No trailing commas.

INPUT DATA:
${JSON.stringify(bucketJson)}
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

  let cleaned = text
    .replace(/```json/gi, "")
    .replace(/```/g, "")
    .trim();

  const firstBrace = cleaned.indexOf("{");
  const lastBrace  = cleaned.lastIndexOf("}");

  if (firstBrace !== -1 && lastBrace !== -1) {
    cleaned = cleaned.substring(firstBrace, lastBrace + 1);
  }

  return JSON.parse(cleaned);
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }],
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
async function clearExpiredLocks() {
  const cutoffIso = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  await supabase
    .from(TABLE_NAME)
    .update({ [LOCK_COL]: null, [LOCK_AT_COL]: null })
    .lt(LOCK_AT_COL, cutoffIso);
}

async function claimRows(limit) {
  await clearExpiredLocks();

  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(`id, ${IN_COL}`)
    .not(IN_COL, "is", null)
    .is(OUT_COL, null)
    .is(LOCK_COL, null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  const { data: locked, error: err2 } = await supabase
    .from(TABLE_NAME)
    .update({
      [LOCK_COL]: WORKER_ID,
      [LOCK_AT_COL]: new Date().toISOString()
    })
    .in("id", ids)
    .is(LOCK_COL, null)
    .select(`id, ${IN_COL}`);

  if (err2) throw err2;

  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  try {
    const raw = await callOpenAI(buildPrompt(row[IN_COL]));
    const json = extractJson(raw);

    await supabase
      .from(TABLE_NAME)
      .update({
        [OUT_COL]: json,
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id);

    return true;

  } catch (err) {
    console.error("❌ Row failed:", row.id);
    console.error(err.message);

    await supabase
      .from(TABLE_NAME)
      .update({
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id);

    throw err;
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🚀 LIVE BUCKET IMAGE WORKER STARTED | ${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`📥 Picked ${claimed.length} rows`);

      for (let i = 0; i < claimed.length; i += BATCH_SIZE) {
        const batch = claimed.slice(i, i + BATCH_SIZE);

        const results = await Promise.allSettled(batch.map(processRow));

        const success = results.filter(r => r.status === "fulfilled").length;
        const failed  = results.filter(r => r.status === "rejected").length;

        console.log(`⚙️ Batch done | Success: ${success} | Failed: ${failed}`);
      }

    } catch (e) {
      console.error("❌ Worker loop error:", e.message);
      await sleep(1200);
    }
  }
})();
