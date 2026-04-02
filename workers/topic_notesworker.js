require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.TOPIC_NOTES_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.TOPIC_NOTES_LIMIT || "20", 10);
const BATCH_SIZE   = parseInt(process.env.TOPIC_NOTES_BATCH_SIZE || "2", 10);
const SLEEP_MS     = parseInt(process.env.TOPIC_NOTES_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.TOPIC_NOTES_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `topic-notes-worker-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

const TABLE = "topic_notes";
const LOCK_COL = "notes_lock";
const LOCK_AT  = "notes_lock_at";

console.log("🚀 TOPIC NOTES WORKER STARTED:", WORKER_ID);

// ─────────────────────────────────────────────
// PROMPT (USE EXACTLY AS PROVIDED)
// ─────────────────────────────────────────────
function buildPrompt(topic) {
  return `{
  "task": "Generate a structured medical question bank with Level 1–4 difficulty distribution",
  "instructions": {
    "topic": "${topic}",
    "exam_focus": "NEET PG, INI-CET, UWorld, AMBOSS, First Aid high-yield concepts",
    "difficulty_distribution": {
      "Level_1": 50,
      "Level_2": 30,
      "Level_3": 20,
      "Level_4": 10
    },
    "output_format": {
      "type": "JSON",
      "schema": {
        "Level_1": { "count": 50, "items": [{ "question": "", "answer": "" }] },
        "Level_2": { "count": 30, "items": [{ "question": "", "answer": "" }] },
        "Level_3": { "count": 20, "items": [{ "question": "", "answer": "" }] },
        "Level_4": { "count": 10, "items": [{ "question": "", "answer": "" }] }
      }
    },
    "rules": [
      "STRICTLY output valid JSON only",
      "DO NOT include any explanation outside JSON",
      "Each item must contain ONLY: question, answer",
      "Generate exact counts as specified",
      "Do NOT use MCQ format; only Question → Answer format"
    ]
  }
}`;
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i
    .test(String(e?.message || e));
}

function extractJson(text) {
  if (!text) throw new Error("Empty model response");

  let cleaned = text
    .replace(/```json/gi, "")
    .replace(/```/g, "")
    .trim();

  const firstBrace = cleaned.indexOf("{");
  const lastBrace  = cleaned.lastIndexOf("}");

  if (firstBrace !== -1 && lastBrace !== -1) {
    cleaned = cleaned.substring(firstBrace, lastBrace + 1);
  }

  return JSON.parse(cleaned);
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }]
    });

    return resp.choices?.[0]?.message?.content?.trim();
  } catch (e) {
    if (isRetryable(e) && attempt <= 2) {
      await sleep(800 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

// ─────────────────────────────────────────────
// CLAIM ROWS
// ─────────────────────────────────────────────
async function claimRows(limit) {

  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // Release stale locks
  await supabase
    .from(TABLE)
    .update({ [LOCK_COL]: null, [LOCK_AT]: null })
    .lt(LOCK_AT, cutoff);

  const { data, error } = await supabase
    .from(TABLE)
    .select("id, topic")
    .or("notes.is.null,notes.eq.{}")
    .is(LOCK_COL, null)
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  const { data: locked } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: WORKER_ID,
      [LOCK_AT]: new Date().toISOString()
    })
    .in("id", ids)
    .is(LOCK_COL, null)
    .select("id, topic");

  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  try {

    const raw = await callOpenAI(buildPrompt(row.topic));
    const json = extractJson(raw);

    await supabase
      .from(TABLE)
      .update({
        notes: json,
        [LOCK_COL]: null,
        [LOCK_AT]: null
      })
      .eq("id", row.id);

    console.log("✅ Notes saved:", row.topic);

  } catch (err) {

    console.error("❌ Failed:", row.topic, err.message);

    await supabase
      .from(TABLE)
      .update({
        [LOCK_COL]: null,
        [LOCK_AT]: null
      })
      .eq("id", row.id);
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {

  console.log(`🧠 TOPIC NOTES WORKER RUNNING | ${WORKER_ID}`);

  while (true) {
    try {

      const rows = await claimRows(LIMIT);

      if (!rows.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);
        await Promise.allSettled(batch.map(processRow));
      }

    } catch (e) {
      console.error("❌ Worker loop error:", e.message);
      await sleep(1500);
    }
  }
})();
