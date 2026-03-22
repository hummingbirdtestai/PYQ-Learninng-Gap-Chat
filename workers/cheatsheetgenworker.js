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

console.log("────────────────────────────────────────────");
console.log("🚀 WORKER BOOTING");
console.log("Model:", MODEL);
console.log("Limit:", LIMIT);
console.log("Batch Size:", BATCH_SIZE);
console.log("Lock TTL:", LOCK_TTL_MIN);
console.log("Worker ID:", WORKER_ID);
console.log("────────────────────────────────────────────");

// ─────────────────────────────────────────────
// PROMPT (UNCHANGED)
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

You MUST return STRICT VALID JSON ONLY.
No explanation.
No markdown.
No commentary.
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

function extractJson(text, rowId) {
  if (!text) throw new Error("Empty model response");

  let cleaned = text
    .replace(/```json/gi, "")
    .replace(/```/g, "")
    .trim();

  const firstBrace = cleaned.indexOf("{");
  const lastBrace  = cleaned.lastIndexOf("}");

  if (firstBrace === -1 || lastBrace === -1) {
    console.error("❌ JSON boundaries not found for row:", rowId);
    console.error("RAW OUTPUT:\n", text);
    throw new Error("No JSON object found");
  }

  cleaned = cleaned.substring(firstBrace, lastBrace + 1);

  try {
    return JSON.parse(cleaned);
  } catch (e) {
    console.error("❌ JSON PARSE FAILED for row:", rowId);
    console.error("CLEANED OUTPUT:\n", cleaned);
    throw e;
  }
}

async function callOpenAI(prompt, rowId, attempt = 1) {
  try {
    console.log(`🧠 Calling OpenAI | Row: ${rowId} | Attempt: ${attempt}`);

    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }],
    });

    const content = resp.choices?.[0]?.message?.content?.trim();

    if (!content) {
      console.error("❌ Empty response from OpenAI for row:", rowId);
    }

    return content;

  } catch (e) {
    console.error(`❌ OpenAI error | Row: ${rowId} | Attempt: ${attempt}`);
    console.error("Error:", e.message);

    if (isRetryable(e) && attempt <= 2) {
      console.log("🔁 Retrying...");
      await sleep(800 * attempt);
      return callOpenAI(prompt, rowId, attempt + 1);
    }
    throw e;
  }
}

// ─────────────────────────────────────────────
// CLAIM ROWS
// ─────────────────────────────────────────────
async function claimRows(limit) {
  console.log("🔍 Checking for expired locks...");

  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  await supabase
    .from(TABLE_NAME)
    .update({ [LOCK_COL]: null, [LOCK_AT_COL]: null })
    .lt(LOCK_AT_COL, cutoff);

  console.log("🔎 Fetching eligible rows...");

  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select("id, subject, new_topic, number")
    .not("number", "is", null)
    .is("cheat_sheet", null)
    .is(LOCK_COL, null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) {
    console.error("❌ Claim error:", error);
    throw error;
  }

  if (!data?.length) {
    console.log("🟡 No rows available");
    return [];
  }

  console.log(`📦 Found ${data.length} eligible rows`);

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

  if (err2) {
    console.error("❌ Lock error:", err2);
    throw err2;
  }

  console.log(`🔒 Locked ${locked?.length || 0} rows`);
  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  console.log("────────────────────────────");
  console.log("▶ Processing Row:", row.id);
  console.log("Subject:", row.subject);
  console.log("Topic:", row.new_topic);
  console.log("Number:", row.number);

  try {
    const prompt = buildPrompt(row.subject, row.new_topic, row.number);

    console.log("📤 Prompt Preview:", prompt.substring(0, 200), "...");

    const raw = await callOpenAI(prompt, row.id);

    console.log("📥 Raw Model Output Preview:", raw?.substring(0, 200));

    const json = extractJson(raw, row.id);

    console.log("✅ JSON Parsed Successfully for row:", row.id);

    const { error } = await supabase
      .from(TABLE_NAME)
      .update({
        cheat_sheet: json,
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id);

    if (error) {
      console.error("❌ Supabase update error:", error);
      throw error;
    }

    console.log("💾 Saved successfully:", row.id);
    return true;

  } catch (err) {
    console.error("❌ Row failed:", row.id);
    console.error("Reason:", err.message);

    await supabase
      .from(TABLE_NAME)
      .update({
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id);

    console.log("🔓 Lock released for:", row.id);
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

      for (let i = 0; i < claimed.length; i += BATCH_SIZE) {
        const batch = claimed.slice(i, i + BATCH_SIZE);

        console.log(`⚙️ Processing batch of ${batch.length}`);

        const results = await Promise.allSettled(batch.map(processRow));

        const success = results.filter(r => r.status === "fulfilled").length;
        const failed  = results.filter(r => r.status === "rejected").length;

        console.log(`📊 Batch Result | Success: ${success} | Failed: ${failed}`);
      }

    } catch (e) {
      console.error("❌ Worker loop error:", e.message);
      await sleep(1500);
    }
  }
})();
