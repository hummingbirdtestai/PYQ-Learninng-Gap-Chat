
"use strict";

require("dotenv").config();

const crypto = require("node:crypto");
const { toFile } = require("openai/uploads");

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

const TABLE = "neetpg_infographics";
const BUCKET = process.env.NEETPG_IMAGE_OUTPUT_BUCKET || "infographic-generated";

const MODEL = process.env.NEETPG_IMAGE_MODEL || "gpt-image-1.5";
const SIZE = process.env.NEETPG_IMAGE_SIZE || "1024x1536";
const QUALITY = process.env.NEETPG_IMAGE_QUALITY || "high";

const LIMIT = Number(process.env.NEETPG_IMAGE_LIMIT || 1);
const CONCURRENCY = Number(process.env.NEETPG_IMAGE_BATCH_SIZE || 1);
const RETRIES = Number(process.env.NEETPG_IMAGE_API_RETRIES || 0);
const SLEEP_MS = Number(process.env.NEETPG_IMAGE_LOOP_SLEEP_MS || 3000);

const TEST_MODE = process.env.NEETPG_IMAGE_TEST_MODE !== "false";
const TEST_JOB_ID = process.env.NEETPG_IMAGE_TEST_JOB_ID || "";

const WORKER_ID =
  process.env.NEETPG_IMAGE_WORKER_ID ||
  `neetpg-image-${process.pid}-${crypto.randomUUID().slice(0, 8)}`;

const REFERENCE_URL =
  process.env.NEETPG_IMAGE_REFERENCE_URL ||
  "https://heqetxfwzoefgghrmtia.supabase.co/storage/v1/object/public/sample%20image/sickle%20cell%20sample%20image.png";

// Your exact image-generation System Prompt.
const SYSTEM_PROMPT = `Create ONE premium, publication-quality, A4 PORTRAIT (210 × 297 mm), print-ready medical education infographic, matching the artistic style, colour palette, visual hierarchy and hand-scripted presentation of the supplied reference infographic 
Use a beautiful, classic hand-painted medical atlas + Picmonic + handwritten medical mind-map aesthetic.
- Warm ivory-white paper background.
- Large, exceptionally legible, handwritten-style lettering.
- Scientifically accurate, colourful anatomical illustrations with fine ink outlines and delicate watercolour shading.
- Royal blue, violet, coral red, teal, amber and yellow highlights.
- Hand-sketched diagnostic arrows, illustrated mechanisms, comparison tables and colour-coded information panels.
- Illustrations must directly explain the medical concepts, not merely decorate the page.
- Boldly highlight key clinical buzzwords, enzymes, diagnoses and treatment.
- Generous spacing and large readable text throughout.
Avoid photorealistic clinical photography, overcrowding, tiny fonts, excessive decorative elements and generic PowerPoint styling.
.
- Strict A4 portrait aspect ratio (1:√2).
- Target 2480 × 3508 pixels at 300 DPI or higher.
- Large, readable handwritten-style fonts.
- Correct spelling of all medical terms.
- Scientifically accurate heart, lysosome and histology illustrations.
- Clearly separated sections numbered 
- Colour-highlighted clinical buzzwords.
- No text overlapping illustrations.
- No cropped headings or incomplete arrows.
- Preserve every essential diagnostic distinction.
- The completed infographic should resemble a beautifully hand-illustrated medical textbook page, not a conventional presentation slide.
Generate the complete infographic as one cohesive, print-ready A4 portrait illustration.`;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function errorText(error) {
  return String(error?.message || error || "Unknown error");
}

function isCreditError(error) {
  return /insufficient_quota|billing|credit balance|no credits/i
    .test(errorText(error));
}

function isRetryable(error) {
  const status = Number(error?.status || 0);
  return [408, 409, 429].includes(status) ||
    status >= 500 ||
    /timeout|temporar|ECONNRESET|ETIMEDOUT/i.test(errorText(error));
}

function validateConfiguration() {
  if (!Number.isInteger(LIMIT) || LIMIT < 1 || LIMIT > 100) {
    throw new Error("Invalid NEETPG_IMAGE_LIMIT");
  }

  if (!Number.isInteger(CONCURRENCY) ||
      CONCURRENCY < 1 || CONCURRENCY > 20) {
    throw new Error("Invalid NEETPG_IMAGE_BATCH_SIZE");
  }

  if (!Number.isInteger(RETRIES) || RETRIES < 0 || RETRIES > 3) {
    throw new Error("Invalid NEETPG_IMAGE_API_RETRIES");
  }

  if (!["1024x1024", "1024x1536", "1536x1024", "auto"]
      .includes(SIZE)) {
    throw new Error("Unsupported image size");
  }

  if (!["low", "medium", "high", "auto"].includes(QUALITY)) {
    throw new Error("Unsupported image quality");
  }
}

function buildPrompt(row) {
  const content = row.image_content;

  if (!content ||
      typeof content !== "object" ||
      Array.isArray(content) ||
      !content.title ||
      !Array.isArray(content.illustrated_panels) ||
      content.illustrated_panels.length === 0) {
    throw new Error("Invalid image_content JSON");
  }

  return [
    SYSTEM_PROMPT,
    "",
    `SUBJECT: ${row.subject}`,
    `TOPIC: ${row.topic}`,
    `PYT SERIAL NUMBER: ${row.serial_number}`,
    `IMAGE NUMBER: ${row.image_serial}`,
    "",
    "IMAGE-SPECIFIC MEDICAL CONTENT:",
    JSON.stringify(content, null, 2),
    "",
    "Generate exactly ONE infographic using this image object.",
    "Preserve all essential medical facts, headings and diagnostic distinctions."
  ].join("\n");
}

// Download reference image once per batch.
async function loadReferenceImage() {
  const response = await fetch(REFERENCE_URL, {
    signal: AbortSignal.timeout(30000)
  });

  if (!response.ok) {
    throw new Error(`Reference download HTTP ${response.status}`);
  }

  const mime = (response.headers.get("content-type") || "")
    .split(";")[0].trim();

  if (!["image/png", "image/jpeg", "image/webp"].includes(mime)) {
    throw new Error(`Unsupported reference MIME: ${mime}`);
  }

  const bytes = Buffer.from(await response.arrayBuffer());

  if (!bytes.length || bytes.length > 20 * 1024 * 1024) {
    throw new Error("Invalid reference image size");
  }

  const filename =
    mime === "image/jpeg" ? "reference.jpg" :
    mime === "image/webp" ? "reference.webp" :
    "reference.png";

  console.log(`Reference loaded: ${bytes.length} bytes`);

  return { bytes, mime, filename };
}

async function verifySchema() {
  const { error } = await supabase
    .from(TABLE)
    .select(
      "id,subject,topic,serial_number,image_serial," +
      "image_content,status,generation_lock," +
      "generation_locked_at,worker_id,image_storage_path"
    )
    .limit(1);

  if (error) throw error;
}

// Conditional update prevents two workers from claiming
// the same pending row.
async function lockRow(id) {
  const lockedAt = new Date().toISOString();

  const { data, error } = await supabase
    .from(TABLE)
    .update({
      status: "processing",
      generation_lock: true,
      generation_locked_at: lockedAt,
      worker_id: WORKER_ID,
      started_at: lockedAt
    })
    .eq("id", id)
    .eq("status", "pending")
    .eq("generation_lock", false)
    .is("image_storage_path", null)
    .select(
      "id,source_id,subject,topic,serial_number," +
      "image_serial,image_content,generation_locked_at"
    )
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function claimRows(limit) {
  let query = supabase
    .from(TABLE)
    .select("id")
    .eq("status", "pending")
    .eq("generation_lock", false)
    .is("image_storage_path", null)
    .order("subject", { ascending: true })
    .order("serial_number", { ascending: true })
    .order("image_serial", { ascending: true })
    .limit(limit);

  if (TEST_JOB_ID) {
    query = query.eq("id", TEST_JOB_ID);
  }

  const { data, error } = await query;

  if (error) throw error;

  const rows = [];

  for (const candidate of data || []) {
    const row = await lockRow(candidate.id);
    if (row) rows.push(row);
  }

  return rows;
}

// Every request gets the same reference image and prompt,
// but a different image_content object.
// Each image-generation request receives:
// 1. Your exact System Prompt
// 2. One image_content JSON object
// 3. The shared reference image

async function generateImage(row, reference) {
  let lastError;

  for (
    let attempt = 0;
    attempt <= RETRIES;
    attempt++
  ) {
    try {
      // Convert the reference image bytes
      // into an OpenAI-compatible upload file.
      const file = await toFile(
        reference.bytes,
        reference.filename,
        {
          type: reference.mime,
        }
      );

      console.log(
        `Generating image for ${row.topic} | Image ${row.image_serial}`
      );

      const response = await openai.images.edit({
        model: MODEL,
        image: file,
        prompt: buildPrompt(row),
        size: SIZE,
        quality: QUALITY,
        output_format: "png",
      });

      const base64 = response.data?.[0]?.b64_json;

      if (!base64) {
        throw new Error(
          "OpenAI returned no image data"
        );
      }

      const imageBuffer = Buffer.from(
        base64,
        "base64"
      );

      if (!imageBuffer.length) {
        throw new Error(
          "Generated image buffer is empty"
        );
      }

      console.log(
        `Image generated successfully: ${imageBuffer.length} bytes`
      );

      return imageBuffer;

    } catch (error) {
      lastError = error;

      console.error(
        `Image generation attempt ${attempt + 1} failed:`,
        errorText(error)
      );

      if (
        isCreditError(error) ||
        attempt === RETRIES ||
        !isRetryable(error)
      ) {
        break;
      }

      const delay = 1500 * 2 ** attempt;

      console.log(
        `Retrying after ${delay} ms`
      );

      await sleep(delay);
    }
  }

  throw lastError || new Error(
    "Image generation failed"
  );
}

function storagePath(row) {
  const safeSubject = row.subject
    .replace(/[^a-z0-9_-]/gi, "_");

  return [
    "neetpg",
    safeSubject,
    row.source_id,
    `image_${row.image_serial}`,
    `${row.id}.png`
  ].join("/");
}

async function uploadImage(row, imageBytes) {
  const objectPath = storagePath(row);

  const { error } = await supabase.storage
    .from(BUCKET)
    .upload(objectPath, imageBytes, {
      contentType: "image/png",
      upsert: false
    });

  if (error) throw error;

  return objectPath;
}

function ownedQuery(query, row) {
  return query
    .eq("id", row.id)
    .eq("status", "processing")
    .eq("generation_lock", true)
    .eq("worker_id", WORKER_ID)
    .eq("generation_locked_at", row.generation_locked_at);
}

async function markCompleted(row, objectPath) {
  const query = supabase.from(TABLE).update({
    image_storage_path: objectPath,
    model_used: MODEL,
    status: "completed",
    completed_at: new Date().toISOString(),
    generation_lock: false,
    generation_locked_at: null,
    worker_id: null,
    error_message: null
  });

  const { data, error } = await ownedQuery(query, row)
    .is("image_storage_path", null)
    .select("id")
    .maybeSingle();

  if (error) throw error;

  if (!data) {
    throw new Error("Completion rejected: job lock changed");
  }
}

async function markFailed(row, error) {
  const query = supabase.from(TABLE).update({
    status: "failed",
    error_message: errorText(error).slice(0, 1000),
    generation_lock: false,
    generation_locked_at: null,
    worker_id: null
  });

  const { error: updateError } = await ownedQuery(query, row);

  if (updateError) {
    console.error("Failure update error:", updateError.message);
  }
}

async function releaseUnprocessed(row) {
  const query = supabase.from(TABLE).update({
    status: "pending",
    generation_lock: false,
    generation_locked_at: null,
    worker_id: null
  });

  const { error } = await ownedQuery(query, row);

  if (error) {
    console.error("Release error:", error.message);
  }
}

async function processRow(row, reference) {
  console.log(
    `START | ${row.subject} | PYT ${row.serial_number} | ` +
    `Image ${row.image_serial}`
  );

  let uploadedPath = null;

  try {
    const bytes = await generateImage(row, reference);

    uploadedPath = await uploadImage(row, bytes);

    await markCompleted(row, uploadedPath);

    console.log(
      `COMPLETED | ${row.id} | ${BUCKET}/${uploadedPath}`
    );

    return { success: true, creditExhausted: false };

  } catch (error) {
    console.error(`FAILED | ${row.id} | ${errorText(error)}`);

    if (uploadedPath) {
      console.error(
        `UPLOADED BUT NOT FINALIZED: ${BUCKET}/${uploadedPath}`
      );
    }

    await markFailed(row, error);

    return {
      success: false,
      creditExhausted: isCreditError(error)
    };
  }
}

async function processBatch(rows, reference) {
  let next = 0;
  let stop = false;
  let completed = 0;

  async function runner() {
    while (next < rows.length && !stop) {
      const row = rows[next++];
      const result = await processRow(row, reference);

      if (result.success) completed++;
      if (result.creditExhausted) stop = true;
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.min(CONCURRENCY, rows.length) },
      runner
    )
  );

  // Release rows claimed but not processed.
  for (const row of rows.slice(next)) {
    await releaseUnprocessed(row);
  }

  return { completed, stop };
}

async function main() {
  validateConfiguration();
  await verifySchema();

  console.log("NEET-PG IMAGE WORKER STARTED");
  console.log(`Worker: ${WORKER_ID}`);
  console.log(`Model: ${MODEL}`);
  console.log(`Batch limit: ${LIMIT}`);
  console.log(`Concurrency: ${CONCURRENCY}`);
  console.log(`Test mode: ${TEST_MODE}`);

  while (true) {
    const rows = await claimRows(TEST_MODE ? 1 : LIMIT);

    if (!rows.length) {
      console.log("No pending image jobs");

      if (TEST_MODE) break;

      await sleep(SLEEP_MS);
      continue;
    }

    let reference;

    try {
      reference = await loadReferenceImage();
    } catch (error) {
      for (const row of rows) {
        await releaseUnprocessed(row);
      }
      throw error;
    }

    const result = await processBatch(rows, reference);

    console.log(
      `Batch finished: ${result.completed}/${rows.length}`
    );

    if (TEST_MODE || result.stop) break;

    await sleep(SLEEP_MS);
  }

  console.log("WORKER STOPPED");
}

main().catch(error => {
  console.error("FATAL:", errorText(error));
  process.exitCode = 1;
});
