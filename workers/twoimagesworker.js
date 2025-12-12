require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

//──────────────────────────────────────────────
// SETTINGS (from .env)
//──────────────────────────────────────────────
const MODEL        = process.env.TWO_IMAGES_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.TWO_IMAGES_LIMIT || "150", 10);
const CONCURRENCY  = parseInt(process.env.TWO_IMAGES_CONCURRENCY || "10", 10);
const SLEEP_MS     = parseInt(process.env.TWO_IMAGES_LOOP_SLEEP_MS || "200", 10);
const LOCK_TTL_MIN = parseInt(process.env.TWO_IMAGES_LOCK_TTL_MIN || "10", 10);
const WORKER_ID    = process.env.WORKER_ID || `two-images-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

//──────────────────────────────────────────────
// PROMPT (EXACTLY AS PROVIDED — NO CHANGES)
//──────────────────────────────────────────────
function buildPrompt(topic) {
  return `
Create 2 Google Image Search Long tail Key words on this Topic that YIELD iMAGES are MOST often tested in NEETPG INICET USMLE Exams give as 2 objects in JSON

TOPIC: ${topic}
`.trim();
}

//──────────────────────────────────────────────
// HELPERS
//──────────────────────────────────────────────
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

function isRetryable(e) {
  return /timeout|429|temporar|unavailable|reset|ETIMEDOUT/i.test(String(e?.message || e));
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages,
      response_format: { type: "json_object" }
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (err) {
    if (isRetryable(err) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw err;
  }
}

//──────────────────────────────────────────────
// CLAIM ROWS (two_images IS NULL)
//──────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // Release stale locks
  await supabase
    .from("video_subject_chapters")
    .update({ two_images_lock: null, two_images_lock_at: null })
    .lt("two_images_lock_at", cutoff);

  // Claim available rows
  const { data: rows, error } = await supabase
    .from("video_subject_chapters")
    .select("id, topic")
    .is("two_images", null)
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
      two_images_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .select("id, topic");

  if (lockErr) throw lockErr;

  return locked || [];
}

//──────────────────────────────────────────────
// CLEAR LOCKS ON FAILURE
//──────────────────────────────────────────────
async function clearLocks(ids) {
  if (!ids.length) return;

  await supabase
    .from("video_subject_chapters")
    .update({
      two_images_lock: null,
      two_images_lock_at: null
    })
    .in("id", ids);
}

//──────────────────────────────────────────────
// PROCESS ONE ROW
//──────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.topic);

  const raw = await callOpenAI([{ role: "user", content: prompt }]);

  let json;
  try {
    json = JSON.parse(raw);
  } catch (err) {
    console.error("❌ INVALID JSON RECEIVED:\n", raw);
    throw new Error("OpenAI returned invalid JSON");
  }

  // Save output
  await supabase
    .from("video_subject_chapters")
    .update({
      two_images: json,
      two_images_lock: null,
      two_images_lock_at: null
    })
    .eq("id", row.id);

  return true;
}

//──────────────────────────────────────────────
// CONCURRENCY EXECUTION
//──────────────────────────────────────────────
async function processWithConcurrency(rows) {
  let index = 0;
  let success = 0;

  async function workerThread() {
    while (index < rows.length) {
      const item = rows[index++];
      try {
        await processRow(item);
        success++;
      } catch (err) {
        console.error(`❌ Failed for row ${item.id}`, err);
        await clearLocks([item.id]);
      }
    }
  }

  const workers = [];
  for (let i = 0; i < CONCURRENCY; i++) {
    workers.push(workerThread());
  }

  await Promise.all(workers);
  return success;
}

//──────────────────────────────────────────────
// MAIN LOOP — RUNS FOREVER
//──────────────────────────────────────────────
(async function main() {
  console.log(`🚀 TWO-IMAGES Worker Started | model=${MODEL} | concurrency=${CONCURRENCY} | batch=${LIMIT} | worker=${WORKER_ID}`);

  while (true) {
    try {
      const rows = await claimRows(LIMIT);

      if (!rows.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${rows.length} rows…`);

      const success = await processWithConcurrency(rows);

      console.log(`✅ Completed ${success}/${rows.length} items`);
    } catch (err) {
      console.error("❌ LOOP ERROR:", err);
      await sleep(1000);
    }
  }
})();
