// workers/feedImageMigrationWorker.js
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

const BUCKET = "feed-posts"; // your bucket name

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
// MAIN WORKER LOOP
//────────────────────────────────────────
async function startImageMigrationWorker() {
  console.log("🚀 feed_posts → image migration started...");

  const { data: rows, error } = await supabase
    .from("feed_posts")
    .select("id, image_url")
    .not("image_url", "is", null)
    .is("image_url_supabase", null)
    .limit(10);

  if (error) {
    console.error("❌ DB fetch error:", error);
    process.exit(1);
  }

  console.log(`📌 ${rows.length} rows to migrate`);

  for (const row of rows) {
    const imgUrl = row.image_url;

    console.log(`➡️ Processing id: ${row.id}`);

    if (!imgUrl || imgUrl.trim() === "") {
      console.log("⚠️ Empty URL skipped:", row.id);
      continue;
    }

    const buffer = await downloadImage(imgUrl);
    if (!buffer) {
      console.log("⚠️ Skipping due to failed download");
      continue;
    }

    // STORAGE PATH
    const fileName = `${row.id}-${Date.now()}.jpg`;
    const storagePath = `feed/${fileName}`;

    // UPLOAD TO SUPABASE STORAGE
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

    // GET PUBLIC URL
    const { data: publicUrl } = supabase.storage
      .from(BUCKET)
      .getPublicUrl(storagePath);

    const newUrl = publicUrl.publicUrl;

    // UPDATE ROW
    const { error: updateErr } = await supabase
      .from("feed_posts")
      .update({ image_url_supabase: newUrl })
      .eq("id", row.id);

    if (updateErr) {
      console.error("❌ Update failed:", updateErr);
      continue;
    }

    console.log(`✅ Migrated row: ${row.id}`);
  }

  console.log("🎉 Image migration completed!");
  process.exit(0);
}

startImageMigrationWorker();
