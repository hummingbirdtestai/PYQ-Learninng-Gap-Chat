require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.MCQ_TABLES_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_TABLES_LIMIT || "10", 10);
const BATCH_SIZE   = parseInt(process.env.MCQ_TABLES_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.MCQ_TABLES_LOOP_SLEEP_MS || "300", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_TABLES_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `mcq-pattern-tables-worker-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

const TABLE = "mcq_hyf_list";
const LOCK_COL = "mcq_json_lock";
const LOCK_AT  = "mcq_json_lock_at";

console.log("🚀 MCQ PATTERN TABLES WORKER STARTED:", WORKER_ID);

// ─────────────────────────────────────────────
// PROMPT (USE EXACTLY AS GIVEN)
// ─────────────────────────────────────────────
function buildPrompt(mcqJson) {
  return `
NEETPG Exam is based on Pattern that repeats . These are the PYQs in NEETPG UNDER THIS TOPIC CONVERTED TO single liners and grouped under common Headings . This should give you clue to you as 20 Years Experienced NEETPG Paper Setter , what is depth of Knowledge base of High Yeild facts a NEETPG Aspirant should be ready and Know the Predictable Patter of Examiner of all the Possible Comparitive Tables , HYF base , Exam Traps of the NEETPG Exam from which MCQs are Set . Based on above what is expected Repeatedly asked Pattern of High Yiedl facts as Tables of UWORLD , FIRST AID STYLE NEED TO BE REMEBEBERED pATTERN IN THE SENSE , IF THEY ASKED A SIDE EFFECT OF emla , side effect of all anaesthetics worth remembering for NEETPG SHOULD BE A table . Generate NEETPG HIGH-YIELD PATTERN TABLES in UWORLD / FIRST AID/AMBOSS/NBE STANDARD of rapid-revision style PATTERN EXPANSION) : .YOU NEED TO COVER THE WHOLE CONTENT and Expand Pattern Tables Horizontally , vertically , quick comparision tables to remember on the EXPECTED FUTURE QUESTIONS PYQs are converted into HYFs and grouped under Headings . Make Sure you Cover in the tables , 100% of ALL THE HEADINGS . Give Title of Table also . Give as Mark Up code of only Tables Use Unicode for Superscripts , Subscripts, Symbols , Math , Greek Letters Give as Code

INPUT:
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
    .select("id, mcq_json")
    .not("mcq_json", "is", null)
    .is("tables", null)
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
    .select("id, mcq_json");

  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  try {

    const output = await callOpenAI(buildPrompt(row.mcq_json));

    await supabase
      .from(TABLE)
      .update({
        tables: output,
        [LOCK_COL]: null,
        [LOCK_AT]: null
      })
      .eq("id", row.id);

    console.log("✅ Tables saved:", row.id);

  } catch (err) {

    console.error("❌ Failed:", row.id, err.message);

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

  console.log(`🧠 MCQ PATTERN TABLES WORKER RUNNING | ${WORKER_ID}`);

  while (true) {

    const rows = await claimRows(LIMIT);

    if (!rows.length) {
      await sleep(SLEEP_MS);
      continue;
    }

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      await Promise.allSettled(batch.map(processRow));
    }
  }
})();
