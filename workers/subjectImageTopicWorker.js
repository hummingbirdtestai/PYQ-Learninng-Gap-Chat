// /app/workers/subjectImageTopicWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.SUBJECT_IMAGE_TOPIC_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.SUBJECT_IMAGE_TOPIC_LIMIT || "100", 10);
const BLOCK_SIZE   = parseInt(process.env.SUBJECT_IMAGE_TOPIC_BLOCK_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.SUBJECT_IMAGE_TOPIC_SLEEP_MS || "800", 10);
const LOCK_TTL_MIN = parseInt(process.env.SUBJECT_IMAGE_TOPIC_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `subjimgtopic-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt Builder ----------
function buildPrompt(imageData) {
  const rawText = typeof imageData === "string"
    ? imageData
    : JSON.stringify(imageData, null, 2);

  return `
You are Expert NEETPG Teacher and Paper Setter , mention in 1-2 words , the High Yield topic into thich this can be classified

${rawText}
  `.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(s);
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages,
    });
    return resp.choices?.[0]?.message?.content?.trim() || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw e;
  }
}

// ---------- Locking & Claim ----------
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // Free stale locks
  await supabase
    .from("subject_images_flatten")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .is("topic", null)
    .lt("mcq_lock_at", cutoff);

  // Claim unlocked rows
  const { data: candidates, error: e1 } = await supabase
    .from("subject_images_flatten")
    .select("id, image_data")
    .is("topic", null)
    .order("created_at", { ascending: true })
    .limit(limit);
  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  const { data: locked, error: e2 } = await supabase
    .from("subject_images_flatten")
    .update({
      mcq_lock: WORKER_ID,
      mcq_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("topic", null)
    .is("mcq_lock", null)
    .select("id, image_data");
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

// ---------- Process Block ----------
async function processBlock(block) {
  const updates = [];

  for (const row of block) {
    try {
      const prompt = buildPrompt(row.image_data);
      const topic = await callOpenAI([{ role: "user", content: prompt }]);
      updates.push({ id: row.id, data: { topic } });
    } catch (e) {
      console.error(`❌ Error processing row ${row.id}:`, e.message || e);
      await clearLocks([row.id]);
    }
  }

  for (const u of updates) {
    const { error: upErr } = await supabase
      .from("subject_images_flatten")
      .update(u.data)
      .eq("id", u.id);
    if (upErr) throw upErr;
  }

  await clearLocks(block.map(r => r.id));
  return { updated: updates.length, total: block.length };
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧵 Subject Image Topic Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | block=${BLOCK_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);
      let updated = 0;

      for (let i = 0; i < claimed.length; i += BLOCK_SIZE) {
        const block = claimed.slice(i, i + BLOCK_SIZE);
        try {
          const r = await processBlock(block);
          updated += r.updated;
          console.log(`   block ${i / BLOCK_SIZE + 1}: updated ${r.updated}/${r.total}`);
        } catch (e) {
          console.error("   block error:", e.message || e);
          await clearLocks(block.map(r => r.id));
        }
      }

      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
