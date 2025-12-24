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

  while (true) {
    const { data: rows, error } = await supabase
      .from(TABLE)
      .select("id, image_url")
      .not("image_url", "is", null)
      .is("supabase_image_url", null)
      .limit(LIMIT);

    if (error) {
      console.error("❌ DB fetch error:", error);
      await sleep(2000);
      continue;
    }

    if (!rows.length) {
      console.log("⏸️ No rows left, sleeping...");
      await sleep(3000);
      continue;
    }

    console.log(`📌 ${rows.length} rows found`);

    for (const row of rows) {
      console.log(`➡️ Processing row: ${row.id}`);

      const buffer = await downloadImage(row.image_url);
      if (!buffer) continue;

      const fileName = `${row.id}-${Date.now()}.jpg`;
      const storagePath = `${FOLDER}/${fileName}`;

      const { error: uploadErr } = await supabase.storage
        .from(BUCKET)
        .upload(storagePath, buffer, {
          contentType: "image/jpeg",
          upsert: false,
        });

      if (uploadErr) continue;

      const { data: publicUrl } = supabase.storage
        .from(BUCKET)
        .getPublicUrl(storagePath);

      await supabase
        .from(TABLE)
        .update({ supabase_image_url: publicUrl.publicUrl })
        .eq("id", row.id);

      console.log(`✅ Migrated: ${row.id}`);
    }
  }
}

startWorker();
