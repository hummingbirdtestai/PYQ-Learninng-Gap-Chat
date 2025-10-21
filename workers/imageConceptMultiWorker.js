require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ---------- Settings ----------
const MODEL        = process.env.IMAGE_CONCEPT_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.IMAGE_CONCEPT_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.IMAGE_CONCEPT_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.IMAGE_CONCEPT_LOOP_SLEEP_MS || "1000", 10);
const LOCK_TTL_MIN = parseInt(process.env.IMAGE_CONCEPT_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `image-concept-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(subject, chapter) {
  return `
You are an expert NEET-PG / USMLE / NBME question-bank creator. 

INPUT: A JSON object containing the current subject and chapter:
{ "subject": "${subject}", "chapter": "${chapter}" }

TASK: Using the CHAPTER context, generate a JSON array with exactly 25 UNIQUE elements. 
Each element represents a high-yield, image-based question concept relevant to that chapter.

Each element must strictly follow this structure:
{
  "subject": "${subject}",
  "keyword": "<precise 2–4 word image search phrase>",
  "concept": "<one-line tested concept>",
  "image_description": "<vignette-style clinical image description>"
}

RULES:
- The “subject” must always match the input subject.
- The “keyword” must be specific, concise, and image-searchable (e.g., "Cherry red spot fundus").
- The “concept” should represent the exact tested principle.
- The “image_description” should be exam-style, short, and vignette-ready.
- Avoid repetition across 25 objects.
- Output pure JSON array only — no commentary, markdown, or numbering.
`.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function isRetryable(e) {
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(String(e?.message || e));
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "text" },
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

function safeParseJson(raw, id) {
  try {
    const cleaned = raw.trim()
      .replace(/^```json\s*/i, "")
      .replace(/^```/, "")
      .replace(/```$/, "");
    return JSON.parse(cleaned);
  } catch (err) {
    throw new Error(`❌ JSON parse error id=${id}: ${err.message}\nRaw:\n${raw.slice(0,200)}`);
  }
}

// ---------- Lock Management ----------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();
  await supabase
    .from("subject_wise_images")
    .update({ image_json_lock: null, image_json_lock_at: null })
    .is("image_json", null)
    .lt("image_json_lock_at", cutoff);
}

async function claimRows(limit) {
  await freeStaleLocks();
  const { data, error } = await supabase
    .from("subject_wise_images")
    .select("id, subject_name, subject_chapter_json")
    .is("image_json", null)
    .is("image_json_lock", null)
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);
  const { data: locked, error: e2 } = await supabase
    .from("subject_wise_images")
    .update({
      image_json_lock: WORKER_ID,
      image_json_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("image_json", null)
    .is("image_json_lock", null)
    .select("id, subject_name, subject_chapter_json");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("subject_wise_images")
    .update({ image_json_lock: null, image_json_lock_at: null })
    .in("id", ids);
}

// ---------- Core Process ----------
async function processRow(row) {
  const subject = row.subject_name;
  const chapters = Array.isArray(row.subject_chapter_json)
    ? row.subject_chapter_json
    : [];

  const results = [];

  for (const chapterObj of chapters) {
    const chapter = chapterObj.chapter || chapterObj.name || "General";
    const prompt = buildPrompt(subject, chapter);
    const raw = await callOpenAI(prompt);
    const parsed = safeParseJson(raw, row.id);
    results.push({ chapter, concepts: parsed });
  }

  const { error } = await supabase
    .from("subject_wise_images")
    .update({
      image_json: results,
      image_json_lock: null,
      image_json_lock_at: null,
      updated_at: new Date().toISOString(),
    })
    .eq("id", row.id);

  if (error) throw new Error(`❌ Update failed for id=${row.id}: ${error.message}`);
  return { updated: 1 };
}

async function processBatch(rows) {
  const chunks = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE)
    chunks.push(rows.slice(i, i + BATCH_SIZE));

  let updated = 0;
  for (const chunk of chunks) {
    const results = await Promise.allSettled(chunk.map(processRow));
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (r.status === "fulfilled") updated += r.value.updated;
      else {
        console.error(r.reason?.message || r.reason);
        await clearLocks([chunk[i].id]);
      }
    }
  }
  return updated;
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧠 Image Concept Multiworker ${WORKER_ID} | model=${MODEL}`);
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }
      console.log(`⚙️ claimed=${claimed.length}`);
      const updated = await processBatch(claimed);
      console.log(`✅ updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
