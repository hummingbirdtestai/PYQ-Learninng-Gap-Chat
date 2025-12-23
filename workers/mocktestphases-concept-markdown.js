/**
 * mocktestphases-concept-markdown.js
 *
 * Reads  phase_json
 * Writes concept_markdown
 * Uses  image_job_lock / image_job_lock_at
 */

require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

//──────────────────────────────────────────────
// SETTINGS
//──────────────────────────────────────────────
const MODEL        = process.env.HY_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.HY_LIMIT || "20", 10);
const SLEEP_MS     = parseInt(process.env.HY_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.HY_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    =
  process.env.WORKER_ID ||
  `mocktest-concept-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;

//──────────────────────────────────────────────
// PROMPT (UNCHANGED — AS REQUESTED)
//──────────────────────────────────────────────
function buildPrompt(phaseJson) {
  return `
You are 30 Years experienced NEETPG INICET Exam Paper Setter 

Give 10 High Yiedl facts most frequently tested based on which MCQs are asked in NEETPG Exam 
Each High Yiedl fact is less than 8 Words 
At the end of High Yield fact List , give a Table of Rapid Revision Reckoner to remember like in Amboss, Usmle world, First Aid, Marrow, PrepLadder kind of Resources 
Be precisely NEETPG INICET Exam Specific 
Use Unicode for Super Scripts , Subscripts , Symbols , Math , emojis , Bullets and MarkUp to High light Bold and Italic of important Key words in High Yiedl facts List and also Table 
• Highlight key words using ONLY GitHub-Flavored Markdown: 
  - Bold = **text** 
  - Italic = *text* 
  - Bold+Italic = ***text*** 
• Do NOT use any underscore formatting (no _text_, __text__, **_text_**). 
Give the entire content as MarkUp Code Block with # and ## used as Section divider for the High Yiedl facts Section and Tabular Section

INPUT CONTENT:
${JSON.stringify(phaseJson)}
`.trim();
}

//──────────────────────────────────────────────
// HELPERS
//──────────────────────────────────────────────
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

function isRetryable(err) {
  const m = String(err?.message || err);
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(m);
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const res = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }],
    });
    return res.choices?.[0]?.message?.content || "";
  } catch (err) {
    if (isRetryable(err) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw err;
  }
}

//──────────────────────────────────────────────
// LOCKING — mock_tests_phases
//──────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(
    Date.now() - LOCK_TTL_MIN * 60_000
  ).toISOString();

  // 1️⃣ Release stale locks
  await supabase
    .from("mock_tests_phases")
    .update({
      image_job_lock: null,
      image_job_lock_at: null,
    })
    .lt("image_job_lock_at", cutoff);

  // 2️⃣ Fetch unlocked + unprocessed rows
  const { data: rows, error } = await supabase
    .from("mock_tests_phases")
    .select("id, phase_json")
    .is("concept_markdown", null)
    .is("image_job_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map((r) => r.id);

  // 3️⃣ Lock rows
  const { data: locked, error: lockErr } = await supabase
    .from("mock_tests_phases")
    .update({
      image_job_lock: WORKER_ID,
      image_job_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .select("id, phase_json");

  if (lockErr) throw lockErr;

  return locked || [];
}

async function releaseLock(id) {
  await supabase
    .from("mock_tests_phases")
    .update({
      image_job_lock: null,
      image_job_lock_at: null,
    })
    .eq("id", id);
}

//──────────────────────────────────────────────
// PROCESS SINGLE ROW
//──────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.phase_json);

  const markdown = await callOpenAI(prompt);

  await supabase
    .from("mock_tests_phases")
    .update({
      concept_markdown: markdown,
      image_job_lock: null,
      image_job_lock_at: null,
    })
    .eq("id", row.id);

  return true;
}

//──────────────────────────────────────────────
// MAIN LOOP
//──────────────────────────────────────────────
(async function main() {
  console.log(
    `🧠 MockTest Concept Worker Started | model=${MODEL} | worker=${WORKER_ID}`
  );

  while (true) {
    try {
      const rows = await claimRows(LIMIT);

      if (!rows.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${rows.length} rows`);

      const results = await Promise.allSettled(
        rows.map((row) => processRow(row))
      );

      results.forEach((res, i) => {
        if (res.status === "fulfilled") {
          console.log(`   ✅ Row ${i + 1} processed`);
        } else {
          console.error(`   ❌ Row ${i + 1} failed`, res.reason);
          releaseLock(rows[i].id);
        }
      });

      console.log(`🔁 Batch complete`);
    } catch (err) {
      console.error("❌ Worker Loop Error:", err);
      await sleep(1000);
    }
  }
})();
