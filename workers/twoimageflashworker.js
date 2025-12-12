require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

//──────────────────────────────────────────────
// SETTINGS
//──────────────────────────────────────────────
const MODEL        = process.env.FLASH_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.FLASH_LIMIT || "150", 10);
const CONCURRENCY  = parseInt(process.env.FLASH_CONCURRENCY || "10", 10);
const SLEEP_MS     = parseInt(process.env.FLASH_LOOP_SLEEP_MS || "200", 10);
const LOCK_TTL_MIN = parseInt(process.env.FLASH_LOCK_TTL_MIN || "10", 10);
const WORKER_ID    = process.env.WORKER_ID || `vsc-flash-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

//──────────────────────────────────────────────
// PROMPT (AS-IS)
//──────────────────────────────────────────────
function buildPrompt(topic) {
  return `
Create exactly 3 Google Image Search LONG-TAIL keyword searches for the given topic that yield images commonly tested in NEETPG, INICET, and USMLE exams.

OUTPUT RULES:
- Output ONLY valid JSON.
- Output a SINGLE JSON OBJECT.
- The object must contain exactly 3 keys: "1", "2", "3".
- Each key must map to ONE long-tail keyword string.
- Do NOT output arrays.
- Do NOT output nested objects.
- Do NOT output explanations or comments.

FINAL OUTPUT EXAMPLE (FORMAT ONLY):
{
  "1": "long tail keyword phrase 1",
  "2": "long tail keyword phrase 2",
  "3": "long tail keyword phrase 3"
}

TOPIC: ${topic}
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
// CLAIM ROWS (flash_card IS NULL)
//──────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // Release stale locks
  await supabase
    .from("video_subject_chapters")
    .update({ two_images_lock: null, two_images_lock_at: null })
    .lt("two_images_lock_at", cutoff);

  // Pick rows
  const { data: rows, error } = await supabase
    .from("video_subject_chapters")
    .select("id, topic")
    .is("flash_card", null)
    .is("two_images_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // Lock them
  const { data: locked, error: lockErr } = await supabase
    .from("video_subject_chapters")
    .update({
      two_images_lock: WORKER_ID,
      two_images_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .select("id, topic");

  if (lockErr) throw lockErr;
  return locked || [];
}

//──────────────────────────────────────────────
// PROCESS ONE ROW
//──────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.topic);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    console.error("❌ Invalid JSON:", raw);
    throw new Error("Invalid JSON from OpenAI");
  }

  // Save into flash_card
  await supabase
    .from("video_subject_chapters")
    .update({
      flash_card: parsed,
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
          .from("video_subject_chapters")
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
  console.log(`🟦 Flash-Card Worker Started | batch=${LIMIT} | concurrency=${CONCURRENCY}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      await processBatch(claimed);
      console.log(`✅ Processed ${claimed.length} flash_card rows`);
    } catch (e) {
      console.error("❌ Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
