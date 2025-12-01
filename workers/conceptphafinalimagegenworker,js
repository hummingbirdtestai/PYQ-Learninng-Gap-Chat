// ─────────────────────────────────────────────
// IMAGE SCRAPER WORKER FOR concept_phase_final
// Fills: image_url
// Source: image_description → OpenAI → Direct Wikimedia URL
// ─────────────────────────────────────────────

require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// SETTINGS
const MODEL        = process.env.IMAGE_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.IMAGE_LIMIT || "40", 10);
const SLEEP_MS     = parseInt(process.env.IMAGE_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.IMAGE_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `image-scraper-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ─────────────────────────────────────────────
// PROMPT BUILDER
// ─────────────────────────────────────────────
function buildPrompt(desc) {
  return `
You are a medical image sourcing assistant.

Your task is to search ONLY Wikimedia Commons and return the **single best matching DIRECT image file URL** (ending with .jpg / .png / .svg) for NEET-PG style medical image-based MCQs.

Follow these strict rules:

1. ALWAYS return a **direct downloadable file link**, not a webpage.
   - Must end with .jpg / .jpeg / .png / .svg
   - Example pattern:
     https://upload.wikimedia.org/...

2. Search ONLY on:
   - Wikimedia Commons
   - Wikipedia media files

3. Output ONLY the final direct file URL.  
   NO description, NO text, NO markdown.

4. The image must be exam-relevant, high-yield, and visually clear.

5. If multiple exist, pick the highest resolution.

---------------------
NOW SEARCH FOR THIS IMAGE:
“${desc}”
---------------------
  `.trim();
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

function isRetryable(e) {
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(String(e?.message || e));
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages
    });
    return resp.choices?.[0]?.message?.content?.trim() || "";
  } catch (err) {
    if (isRetryable(err) && attempt <= 3) {
      await sleep(300 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw err;
  }
}

function extractUrl(raw) {
  const cleaned = raw
    .replace(/^```.*$/gm, "")   // remove code fences
    .trim();

  const match = cleaned.match(/https?:\/\/\S+\.(jpg|jpeg|png|svg)/i);
  if (!match) throw new Error("No direct image URL found in output.");

  return match[0];
}

// ─────────────────────────────────────────────
// LOCKING SYSTEM
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // release old locks
  await supabase
    .from("concept_phase_final")
    .update({ mentor_lock: null, mentor_lock_at: null })
    .lt("mentor_lock_at", cutoff);

  // fetch jobs
  const { data: rows, error } = await supabase
    .from("concept_phase_final")
    .select("id, image_description")
    .not("image_description", "is", null)
    .is("image_url", null)                      // only rows without image_url
    .is("mentor_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // lock them
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
// PROCESS ONE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const desc = row.image_description;
  const prompt = buildPrompt(desc);

  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const url = extractUrl(raw);

  await supabase
    .from("concept_phase_final")
    .update({ image_url: url })
    .eq("id", row.id);

  await clearLocks([row.id]);

  return { updated: 1 };
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🟩 Image Scraper Worker Started | worker=${WORKER_ID} | model=${MODEL}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} image rows`);

      const results = await Promise.allSettled(
        claimed.map(row => processRow(row))
      );

      let updated = 0;

      results.forEach((r, idx) => {
        if (r.status === "fulfilled") {
          updated += r.value.updated;
          console.log(`   ✅ Processed row #${idx + 1}`);
        } else {
          console.error(`   ❌ Error row #${idx + 1}:`, r.reason);
          clearLocks([claimed[idx].id]);
        }
      });

      console.log(`🔁 Batch complete → updated=${updated}/${claimed.length}`);

    } catch (err) {
      console.error("Loop Error:", err);
      await sleep(1000);
    }
  }
})();
