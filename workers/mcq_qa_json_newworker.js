require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.MCQ_QA_NEW_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_QA_NEW_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.MCQ_QA_NEW_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.MCQ_QA_NEW_LOOP_SLEEP_MS || "300", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_QA_NEW_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `mcq-qa-new-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

const TABLE_NAME = "mcq_hyf_list";
const IN_COL     = "mcq_qa_json";
const OUT_COL    = "mcq_qa_json_new";
const LOCK_COL   = "mcq_json_lock";
const LOCK_AT_COL= "mcq_json_lock_at";

// ─────────────────────────────────────────────
// PROMPT (USE EXACTLY AS PROVIDED — NO CHANGE)
// ─────────────────────────────────────────────
function buildPrompt(mcqJson) {
  return `
In the given JSON ,

THERE ARE A sET OF mcqS which are PYQs in NEETPG Exam .

In each MCQ ,

the string in high_yield_facts ,

should be 5 sets of Question and Answer like Flash cards

of most hIGH yIEDLF ACTS IN NEETPG Expected in upcoming NEETPG Exam .

While creating Falsh Card style Question Answer Set ,

,Think like NEEPTG Paper Setter with a Cheat sheet of NEETPG Question Bank

The stem of MCQ is not fully framed ,

reframe it with NEETPG exam styled

making ditto ditto without changing the objective of Stem as original .

aLSO CHECK THE cORRECT ANSWER AND OPTIONS

AND MAKE TEM ORIGINAL LIKE IN ACTUAL neetpg eXAM .

Maintain Mark Up to bold and high light the Key words

and Unicode for Superscripts , Subscripts , Greek letters , Symbols , Math .

dONT MISS even a Single MCQ in JSON

and complete all MCQs in JSON

and give the output as JSON
StriCT rULES : 1) dONT MENTION ORIGINAL STEM OR DONT MENTION REFRAMED STEM EXPLICITY 
2) kEEP THE neetpg year where the MCQ asked in paranthesis , which is there in original mcq AS is at the end of reframed Stem 
3) Flashcard styled Questions , should be less than 20 words long and answer only 1-3 words 
INPUT JSON:
${JSON.stringify(mcqJson)}
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

  const firstBrace = cleaned.indexOf("[");
  const lastBrace  = cleaned.lastIndexOf("]");

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
    .select(`id, ${IN_COL}`)
    .not(IN_COL, "is", null)
    .is(OUT_COL, null)
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
    .select(`id, ${IN_COL}`);

  if (err2) throw err2;
  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  try {
    const raw = await callOpenAI(buildPrompt(row[IN_COL]));
    const json = extractJson(raw);

    await supabase
      .from(TABLE_NAME)
      .update({
        [OUT_COL]: json,
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
  console.log(`🧠 MCQ_QA → MCQ_QA_NEW WORKER STARTED | ${WORKER_ID}`);

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
