require("dotenv").config();
const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");

//────────────────────────────────────────
// INIT SUPABASE
//────────────────────────────────────────
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

//────────────────────────────────────────
// CONFIG
//────────────────────────────────────────
const TABLE  = "image_concept_phase_final";
const BUCKET = "feed-posts";          // change if you want a new bucket
const FOLDER = "image-concept";       // folder inside bucket
const LIMIT  = 20;

//────────────────────────────────────────
// DOWNLOAD IMAGE
//────────────────────────────────────────
async function downloadImage(url) {
  try {
    const response = await axios.get(url, {
      responseType: "arraybuffer",
      timeout: 20000,
    });
    return Buffer.from(response.data);
  } catch (err) {
    console.error("❌ Download failed:", url, err.message);
    return null;
  }
}

//────────────────────────────────────────
// MAIN WORKER
//────────────────────────────────────────
async function startWorker() {
  console.log("🚀 image_concept_phase_final → Supabase image migration started");

  const { data: rows, error } = await supabase
    .from(TABLE)
    .select("id, image_url")
    .not("image_url", "is", null)
    .is("supabase_image_url", null)
    .limit(LIMIT);

  if (error) {
    console.error("❌ DB fetch error:", error);
    process.exit(1);
  }

  console.log(`📌 ${rows.length} rows found`);

  for (const row of rows) {
    console.log(`➡️ Processing row: ${row.id}`);

    if (!row.image_url || row.image_url.trim() === "") {
      console.log("⚠️ Empty image_url, skipped");
      continue;
    }

    const buffer = await downloadImage(row.image_url);
    if (!buffer) {
      console.log("⚠️ Image download failed, skipped");
      continue;
    }

    // STORAGE PATH
    const fileName = `${row.id}-${Date.now()}.jpg`;
    const storagePath = `${FOLDER}/${fileName}`;

    // UPLOAD
    const { error: uploadErr } = await supabase.storage
      .from(BUCKET)
      .upload(storagePath, buffer, {
        contentType: "image/jpeg",
        upsert: false,
      });

    if (uploadErr) {
      console.error("❌ Upload failed:", uploadErr);
      continue;
    }

    // PUBLIC URL
    const { data: publicUrl } = supabase.storage
      .from(BUCKET)
      .getPublicUrl(storagePath);

    const newUrl = publicUrl.publicUrl;

    // UPDATE TABLE
    const { error: updateErr } = await supabase
      .from(TABLE)
      .update({ supabase_image_url: newUrl })
      .eq("id", row.id);

    if (updateErr) {
      console.error("❌ Update failed:", updateErr);
      continue;
    }

    console.log(`✅ Migrated: ${row.id}`);
  }

  console.log("🎉 Migration batch complete");
  process.exit(0);
}

startWorker();
