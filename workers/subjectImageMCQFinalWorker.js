// /app/workers/subjectImageMCQFinalWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ───────────────────────────────────────────────
// SETTINGS
// ───────────────────────────────────────────────
const MODEL        = process.env.SUBJECT_IMAGE_MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.SUBJECT_IMAGE_MCQ_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.SUBJECT_IMAGE_MCQ_BLOCK_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.SUBJECT_IMAGE_MCQ_SLEEP_MS || "1000", 10);
const LOCK_TTL_MIN = parseInt(process.env.SUBJECT_IMAGE_MCQ_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `subject-image-mcq-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ───────────────────────────────────────────────
// PROMPT BUILDER
// ───────────────────────────────────────────────
function buildPrompt(mcqJson) {
  const text = typeof mcqJson === "string" ? mcqJson : JSON.stringify(mcqJson, null, 2);
  return `
You are expert NEETPG Paper setter.  
In the Stem there is a detailed image description.  
Remove that description from the stem and add it as a **separate key** in JSON called "image_description".  
Return the JSON as-is, valid JSON object only (no arrays, no extra text, no markdown fences).

Input JSON:
${text}
  `.trim();
}

// ───────────────────────────────────────────────
// HELPERS
// ───────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(s);
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "text" },
    });
    return resp.choices?.[0]?.message?.content?.trim() || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

function safeParseJson(raw, id) {
  try {
    const cleaned = raw
      .trim()
      .replace(/^```json\s*/i, "")
      .replace(/^```/, "")
      .replace(/```$/i, "");
    return JSON.parse(cleaned);
  } catch (err) {
    throw new Error(`❌ Failed to parse JSON for id=${id}: ${err.message}. Raw snippet: ${raw.slice(0, 200)}`);
  }
}

// ───────────────────────────────────────────────
// LOCK MANAGEMENT
// ───────────────────────────────────────────────
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();
  await supabase
    .from("subject_images_flatten")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .is("image_mcq_final", null)
    .lt("mcq_lock_at", cutoff);
}

async function claimRows(limit) {
  await freeStaleLocks();

  const { data: candidates, error } = await supabase
    .from("subject_images_flatten")
    .select("id, mcq_image")
    .not("mcq_image", "is", null)
    .is("image_mcq_final", null)
    .is("mcq_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  const { data: locked, error: e2 } = await supabase
    .from("subject_images_flatten")
    .update({
      mcq_lock: WORKER_ID,
      mcq_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("image_mcq_final", null)
    .is("mcq_lock", null)
    .select("id, mcq_image");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("subject_images_flatten")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .in("id", ids);
}

// ───────────────────────────────────────────────
// PROCESSING
// ───────────────────────────────────────────────
async function processRow(row) {
  try {
    const prompt = buildPrompt(row.mcq_image);
    const raw = await callOpenAI(prompt);
    const parsed = safeParseJson(raw, row.id);

    const { error } = await supabase
      .from("subject_images_flatten")
      .update({
        image_mcq_final: parsed,
        mcq_lock: null,
        mcq_lock_at: null,
        updated_at: new Date().toISOString(),
      })
      .eq("id", row.id);

    if (error) throw new Error(`Update failed for id=${row.id}: ${error.message}`);
    return { updated: 1 };
  } catch (e) {
    console.error(`❌ Error processing id=${row.id}:`, e.message || e);
    await clearLocks([row.id]);
    return { updated: 0 };
  }
}

async function processBatch(rows) {
  const chunks = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE)
    chunks.push(rows.slice(i, i + BATCH_SIZE));

  let updated = 0;
  for (const chunk of chunks) {
    const results = await Promise.allSettled(chunk.map(processRow));
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (r.status === "fulfilled") updated += r.value.updated;
      else {
        console.error(r.reason?.message || r.reason);
        await clearLocks([chunk[i].id]);
      }
    }
  }
  return updated;
}

// ───────────────────────────────────────────────
// MAIN LOOP
// ───────────────────────────────────────────────
(async function main() {
  console.log(`🧠 Subject Image MCQ Final Worker ${WORKER_ID} | model=${MODEL}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows`);
      const updated = await processBatch(claimed);
      console.log(`✅ Updated=${updated} / ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
