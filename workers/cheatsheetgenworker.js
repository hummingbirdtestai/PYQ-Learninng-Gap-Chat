require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.CHEATSHEET_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CHEATSHEET_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.CHEATSHEET_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.CHEATSHEET_LOOP_SLEEP_MS || "300", 10);
const LOCK_TTL_MIN = parseInt(process.env.CHEATSHEET_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `cheatsheet-worker-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

const TABLE_NAME = "mcq_hyf_list";
const LOCK_COL   = "mcq_json_lock";
const LOCK_AT_COL= "mcq_json_lock_at";

// ─────────────────────────────────────────────
// PROMPT (USE EXACTLY AS PROVIDED — NO CHANGE)
// ─────────────────────────────────────────────
function buildPrompt(subject, topic, number) {
  return `
IYou are a 30-year experienced NEET-PG / INI-CET paper setter, examiner, and question bank architect.

Your task is to generate a high-yield “Exam Traps & Patterns Cheat Sheet” for a given medical topic.

Create NEETPG Examiner in Question Bank of the number specified of Exam Traps and Patterns Cheatsheet in Topic given below.

Focus ONLY on exam traps, common confusions, and pattern-based thinking.

Do NOT write textbook notes.

Do NOT explain in paragraphs.

Output must be ultra-concise, high-yield, and exam-oriented.

Create as JSON where each exam trap or Pattern , serial_number, heading to whch it is classified.

Formatting : Use Markdown for Bold italic of Key words in exam trap Key value , Unicode for Superscripts , Subscripts, Symbols , Greek letters , Math.

Subject : ${subject}

Topic : ${topic}

Number : ${number}
`;
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

  // Clear expired locks
  await supabase
    .from(TABLE_NAME)
    .update({ [LOCK_COL]: null, [LOCK_AT_COL]: null })
    .lt(LOCK_AT_COL, cutoff);

  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select("id, subject, new_topic, number")
    .not("number", "is", null)
    .is("cheat_sheet", null)
    .is(LOCK_COL, null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  const { data: locked, error: err2 } = await supabase
    .from(TABLE_NAME)
    .update({
      [LOCK_COL]: WORKER_ID,
      [LOCK_AT_COL]: new Date().toISOString()
    })
    .in("id", ids)
    .is(LOCK_COL, null)
    .select("id, subject, new_topic, number");

  if (err2) throw err2;
  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  try {
    const raw = await callOpenAI(
      buildPrompt(row.subject, row.new_topic, row.number)
    );
    const json = extractJson(raw);

    await supabase
      .from(TABLE_NAME)
      .update({
        cheat_sheet: json,
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id);

    return true;

  } catch (err) {
    console.error("❌ Row failed:", row.id);

    await supabase
      .from(TABLE_NAME)
      .update({
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id);

    throw err;
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 CHEATSHEET WORKER STARTED | ${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`📥 Picked ${claimed.length} rows`);

      for (let i = 0; i < claimed.length; i += BATCH_SIZE) {
        const batch = claimed.slice(i, i + BATCH_SIZE);
        const results = await Promise.allSettled(batch.map(processRow));

        const success = results.filter(r => r.status === "fulfilled").length;
        const failed  = results.filter(r => r.status === "rejected").length;

        console.log(`⚙️ Batch done | Success: ${success} | Failed: ${failed}`);
      }

    } catch (e) {
      console.error("❌ Worker loop error:", e.message);
      await sleep(1200);
    }
  }
})();
