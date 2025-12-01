// ─────────────────────────────────────────────
// GOOGLE IMAGE WORKER FOR concept_phase_final
// Finds BEST MATCHING DIRECT IMAGE URL (Google Ranking + Resolution)
// Source: image_description → Google Custom Search → Direct URL
// ─────────────────────────────────────────────

require("dotenv").config();
const { supabase } = require("../config/supabaseClient");

// ❌ REMOVE THIS (node-fetch)
// const fetch = require("node-fetch");
// ✅ Node 20+ has global fetch — nothing needed

// SETTINGS
const API_KEY      = process.env.GOOGLE_IMAGE_API_KEY;
const CX           = process.env.GOOGLE_CX;
const LIMIT        = parseInt(process.env.IMAGE_LIMIT || "40", 10);
const SLEEP_MS     = parseInt(process.env.IMAGE_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.IMAGE_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `google-img-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

function isRetryable(e) {
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(String(e?.message || e));
}

async function googleSearch(desc, attempt = 1) {
  try {
    const query = encodeURIComponent(desc);

    const url =
      `https://www.googleapis.com/customsearch/v1?q=${query}&key=${API_KEY}&cx=${CX}` +
      `&searchType=image&num=5`;

    // ✅ Using built-in fetch
    const resp = await fetch(url);
    const json = await resp.json();

    if (!json.items?.length) throw new Error("No images found");

    return json.items;

  } catch (err) {
    if (isRetryable(err) && attempt <= 3) {
      await sleep(300 * attempt);
      return googleSearch(desc, attempt + 1);
    }
    throw err;
  }
}

// Multi-level scoring
function pickBest(items) {
  const scored = items.map((item, index) => {
    const relevanceScore = (5 - index) * 5;  // Google rank

    const w = item.image?.width || 0;
    const h = item.image?.height || 0;
    const resolutionScore = w * h;

    let fileTypeScore = 0;
    if (item.link.endsWith(".png")) fileTypeScore = 5;
    if (item.link.endsWith(".svg")) fileTypeScore = 4;
    if (item.link.endsWith(".jpg") || item.link.endsWith(".jpeg")) fileTypeScore = 3;

    const finalScore =
      relevanceScore * 5 +
      resolutionScore * 1 +
      fileTypeScore * 10;

    return {
      url: item.link,
      finalScore
    };
  });

  scored.sort((a, b) => b.finalScore - a.finalScore);

  return scored[0]?.url;
}

// ─────────────────────────────────────────────
// LOCKING SYSTEM
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  await supabase
    .from("concept_phase_final")
    .update({ mentor_lock: null, mentor_lock_at: null })
    .lt("mentor_lock_at", cutoff);

  const { data: rows, error } = await supabase
    .from("concept_phase_final")
    .select("id, image_description")
    .not("image_description", "is", null)
    .is("image_url", null)
    .is("mentor_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  const { data: locked, error: lockErr } = await supabase
    .from("concept_phase_final")
    .update({
      mentor_lock: WORKER_ID,
      mentor_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("image_url", null)
    .is("mentor_lock", null)
    .select("id, image_description");

  if (lockErr) throw lockErr;

  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("concept_phase_final")
    .update({ mentor_lock: null, mentor_lock_at: null })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS SINGLE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const desc = row.image_description;

  // 1. Get top Google images
  const items = await googleSearch(desc);

  // 2. Pick best-scoring image
  const bestUrl = pickBest(items);

  if (!bestUrl) throw new Error("No valid best image URL");

  // 3. Store in DB
  await supabase
    .from("concept_phase_final")
    .update({ image_url: bestUrl })
    .eq("id", row.id);

  await clearLocks([row.id]);

  return { updated: 1 };
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🟩 Google Image Worker Started | worker=${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows`);

      const results = await Promise.allSettled(
        claimed.map(row => processRow(row))
      );

      let updated = 0;
      results.forEach((r, i) => {
        if (r.status === "fulfilled") {
          updated += r.value.updated;
          console.log(`   ✅ Row ${i+1} updated`);
        } else {
          console.error(`   ❌ Row ${i+1} error:`, r.reason);
          clearLocks([claimed[i].id]);
        }
      });

      console.log(`🔁 Batch = updated ${updated}/${claimed.length}`);

    } catch (err) {
      console.error("Loop error:", err);
      await sleep(1000);
    }
  }
})();
