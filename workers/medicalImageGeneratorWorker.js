// =======================================================================
// AI MEDICAL IMAGE GENERATOR WORKER  (ChatGPT 5.1 Image + Supabase Upload)
// Generates HD medical images from description → uploads to bucket → saves URL
// =======================================================================

require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const OpenAI = require("openai");

// ---------------------- SETTINGS -------------------------------------
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

const LIMIT        = parseInt(process.env.IMAGE_LIMIT || "40", 10);
const SLEEP_MS     = parseInt(process.env.IMAGE_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.IMAGE_LOCK_TTL_MIN || "20", 10);
const WORKER_ID    = process.env.WORKER_ID || `imggen-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

const BUCKET = process.env.SUPABASE_IMAGE_BUCKET || "medical-images";

// ---------------------- HELPERS --------------------------------------
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

function isRetryable(e) {
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(String(e?.message || e));
}

// Master Prompt (NEVER CHANGE THIS)
function buildPrompt(desc) {
  return `
You are a NEET-PG medical image generator.
Generate **ONE** medically accurate HD image based on this description:

"${desc}"

STRICT RULES:
- Do NOT add ANY labels, arrows, annotations, letters, or text on the image.
- If lesion/clinical: produce a real HD clinical photograph.
- If anatomy/histology/pathology/embryology/pharmacology mechanism: produce a realistic diagram (NO LABELS).
- Resolution: 1024x1024 or higher.
- No borders, no watermarks, no branding.
  `;
}

// ---------------------- CLAIM ROWS --------------------------------------
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // Release old locks
  await supabase
    .from("concept_phase_final")
    .update({ mentor_lock: null, mentor_lock_at: null })
    .lt("mentor_lock_at", cutoff);

  // Select rows that need image generation
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

  // Lock them
  const { data: locked, error: lockErr } = await supabase
    .from("concept_phase_final")
    .update({
      mentor_lock: WORKER_ID,
      mentor_lock_at: new Date().toISOString()
    })
    .in("id", ids)
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

// ---------------------- GENERATE IMAGE USING OPENAI -------------------------
async function generateImage(desc) {
  const prompt = buildPrompt(desc);

  const response = await openai.images.generate({
    model: "gpt-image-1",
    prompt,
    size: "1024x1024",
    quality: "high",
    n: 1
  });

  const base64 = response.data[0].b64_json;
  if (!base64) throw new Error("No image generated");

  return Buffer.from(base64, "base64");
}

// ---------------------- UPLOAD TO SUPABASE BUCKET -------------------------
async function uploadToBucket(buffer, filename) {
  const path = `${filename}.png`;

  const { data, error } = await supabase.storage
    .from(BUCKET)
    .upload(path, buffer, {
      contentType: "image/png",
      upsert: true
    });

  if (error) throw error;

  // Get public URL
  const { data: publicUrl } = supabase.storage
    .from(BUCKET)
    .getPublicUrl(path);

  return publicUrl.publicUrl;
}

// ---------------------- PROCESS ONE ROW -------------------------
async function processRow(row) {
  const desc = row.image_description;

  // 1. Generate image
  const imageBuffer = await generateImage(desc);

  // 2. Upload to bucket
  const publicUrl = await uploadToBucket(
    imageBuffer,
    `concept_${row.id}_${Date.now()}`
  );

  // 3. Save URL in DB
  await supabase
    .from("concept_phase_final")
    .update({ image_url: publicUrl })
    .eq("id", row.id);

  // 4. Clear lock
  await clearLocks([row.id]);

  return { updated: 1 };
}

// ---------------------- MAIN LOOP -------------------------
(async function main() {
  console.log(`🟩 Medical Image Generator Worker Started | ${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows for image generation`);

      const results = await Promise.allSettled(
        claimed.map(row => processRow(row))
      );

      let updated = 0;
      results.forEach((r, i) => {
        if (r.status === "fulfilled") {
          updated += r.value.updated;
          console.log(`   ✅ Row ${i+1} image generated`);
        } else {
          console.error(`   ❌ Row ${i+1} error:`, r.reason);
          clearLocks([claimed[i].id]);
        }
      });

      console.log(`🔁 Batch Completed → Updated ${updated}/${claimed.length}`);

    } catch (err) {
      console.error("Loop Error:", err);
      await sleep(1500);
    }
  }
})();
