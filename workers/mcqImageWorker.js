// workers/mcqImageWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");

// ✅ Node 18+ built-in fetch — NO "node-fetch"
const fetch = global.fetch;

// ---------------- Settings ----------------
const LIMIT = parseInt(process.env.MCQ_IMAGE_LIMIT || "200", 10);
const BLOCK = parseInt(process.env.MCQ_IMAGE_BLOCK || "20", 10);
const SLEEP_MS = parseInt(process.env.MCQ_IMAGE_SLEEP_MS || "800", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_IMAGE_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `mcqimg-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;

const BUCKET = "mcq-images";

// ---------------- Helpers ----------------
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(s);
}

// ---------------- Claim rows ----------------
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // 1. Free stale locks
  await supabase
    .from("mock_tests_phases")
    .update({ image_job_lock: null, image_job_lock_at: null })
    .not("image_job_lock", "is", null)
    .lt("image_job_lock_at", cutoff);

  // 2. Select candidates
  const { data: candidates, error } = await supabase
    .from("mock_tests_phases")
    .select("id, image_raw")
    .is("mcq_image", null)
    .not("image_raw", "is", null)
    .is("image_job_lock", null)
    .order("id", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!candidates?.length) return [];

  const ids = candidates.map((r) => r.id);

  // 3. Lock rows
  const { data: locked, error: lockErr } = await supabase
    .from("mock_tests_phases")
    .update({
      image_job_lock: WORKER_ID,
      image_job_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("mcq_image", null)
    .is("image_job_lock", null)
    .select("id, image_raw");

  if (lockErr) throw lockErr;

  return locked || [];
}

// ---------------- Clear locks ----------------
async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("mock_tests_phases")
    .update({ image_job_lock: null, image_job_lock_at: null })
    .in("id", ids);
}

// ---------------- Process a single row ----------------
async function processRow(row) {
  const id = row.id;
  const url = row.image_raw;

  try {
    console.log(`📥 Downloading ${url}`);

    const resp = await fetch(url);
    if (!resp.ok) throw new Error("Failed to download image");

    // ✅ Node-native conversion
    const buffer = Buffer.from(await resp.arrayBuffer());

    // detect extension
    const ext = url.split(".").pop().split("?")[0].toLowerCase();
    const fileExt = ["jpg", "jpeg", "png", "webp"].includes(ext) ? ext : "jpg";

    const fileName = `${id}.${fileExt}`;

    console.log(`⬆ Uploading to bucket ${BUCKET}...`);

    // upload
    const { error: upErr } = await supabase.storage
      .from(BUCKET)
      .upload(fileName, buffer, {
        upsert: true,
        contentType: fileExt === "png" ? "image/png" : "image/jpeg",
      });

    if (upErr) throw upErr;

    // get public URL
    const {
      data: { publicUrl },
    } = supabase.storage.from(BUCKET).getPublicUrl(fileName);

    // update row
    const { error: updErr } = await supabase
      .from("mock_tests_phases")
      .update({ mcq_image: publicUrl })
      .eq("id", id);

    if (updErr) throw updErr;

    console.log(`✅ Image processed for ID ${id}`);
    return true;
  } catch (err) {
    console.error(`❌ Error row ${id}:`, err);
    return false;
  }
}

// ---------------- Process block ----------------
async function processBlock(block) {
  let done = 0;

  for (const row of block) {
    const ok = await processRow(row);
    if (ok) done++;
    await sleep(200);
  }

  await clearLocks(block.map((r) => r.id));
  return done;
}

// ---------------- Main Loop ----------------
(async function main() {
  console.log(`🧵 MCQ Image Worker ${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        console.log("😴 No rows — sleeping…");
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed = ${claimed.length}`);

      let totalDone = 0;
      for (let i = 0; i < claimed.length; i += BLOCK) {
        const block = claimed.slice(i, i + BLOCK);
        const done = await processBlock(block);
        totalDone += done;
        console.log(`🧩 Block done: ${done}/${block.length}`);
      }

      console.log(`🎉 Loop finished → ${totalDone}/${claimed.length}`);
    } catch (e) {
      console.error("🔥 Loop error:", e);
      await sleep(1000);
    }
  }
})();
