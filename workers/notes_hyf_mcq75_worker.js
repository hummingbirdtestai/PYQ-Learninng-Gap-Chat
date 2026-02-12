require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.NOTES_HYF_MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.NOTES_HYF_MCQ_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.NOTES_HYF_MCQ_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.NOTES_HYF_MCQ_LOOP_SLEEP_MS || "300", 10);
const LOCK_TTL_MIN = parseInt(process.env.NOTES_HYF_MCQ_LOCK_TTL_MIN || "15", 10);

// filter
const HYF_NUMBER   = Number(process.env.NOTES_HYF_MCQ_HYF_NUMBER || "75");

const WORKER_ID =
  process.env.WORKER_ID ||
  `notes-hyf-mcq-${process.pid}-${Math.random().toString(36).slice(2, 6)}`;

// target table + columns
const TABLE_NAME = "all_subjects_raw";
const LOCK_COL   = "concept_mcq_json_lock";
const LOCK_AT_COL= "concept_mcq_json_lock_at";
const IN_COL     = "notes_hyf";
const OUT_COL    = "notes_hyf_mcq";

// ─────────────────────────────────────────────
// PROMPT (USE EXACTLY AS GIVEN)
// ─────────────────────────────────────────────
function buildPrompt(notesHyf) {
  return `
Each Topic has 5 Buckets. Create one mcq per Bucket on the High Yield facts created.

The JSON MUST contain EXACTLY 5 Bucket objects, named ONLY:
"bucket_1" through "bucket_5"

📝 MCQ RULES (CRITICAL)

Each bucket’s "mcq" array MUST contain EXACTLY ONE MCQ object.

Each MCQ object MUST contain EXACTLY the following keys IN THIS ORDER:

1. "stem"
- USMLE-style clinical case vignette
- Must clearly imply History → Examination → Investigation
- Paragraph style (not bullets)
- Each MCQ be of High Quality and Standard and tone of First Aid / AMBOSS / UWorld /NBME tone

2. "options"
- Object with EXACTLY 4 keys:
"A", "B", "C", "D"

3. "correct_answer"
- MUST be exactly "A", "B", "C", or "D"

4. "exam_trap"
5. "answer"
- What is the Common Exam Traps examiner sets in framing the MCQ and what is Correct answer and how to remember

Give output Strictly as JSON

HIGH YIELD FACTS:
${JSON.stringify(notesHyf)}
`;
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function isRetryable(e) {
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(
    String(e?.message || e)
  );
}

function extractJson(text) {
  if (!text) throw new Error("Empty model response");

  let cleaned = text
    .replace(/```json/gi, "")
    .replace(/```/g, "")
    .trim();

  const firstBrace = cleaned.indexOf("{");
  const lastBrace = cleaned.lastIndexOf("}");

  if (firstBrace !== -1 && lastBrace !== -1) {
    cleaned = cleaned.substring(firstBrace, lastBrace + 1);
  }

  return JSON.parse(cleaned);
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }],
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
async function clearExpiredLocks() {
  const cutoffIso = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // Clear expired locks (only those with a lock timestamp older than cutoff)
  const { error } = await supabase
    .from(TABLE_NAME)
    .update({ [LOCK_COL]: null, [LOCK_AT_COL]: null })
    .lt(LOCK_AT_COL, cutoffIso);

  if (error) throw error;
}

async function claimRows(limit) {
  await clearExpiredLocks();

  // Pick candidates
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(`id, ${IN_COL}`)
    .eq("hyf_number", HYF_NUMBER)
    .not(IN_COL, "is", null)
    .is(OUT_COL, null)
    .is(LOCK_COL, null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map((r) => r.id);

  // Try to lock them (race-safe: only lock where still unlocked)
  const { data: locked, error: err2 } = await supabase
    .from(TABLE_NAME)
    .update({
      [LOCK_COL]: WORKER_ID,
      [LOCK_AT_COL]: new Date().toISOString(),
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

    const { error } = await supabase
      .from(TABLE_NAME)
      .update({
        [OUT_COL]: json,
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null,
      })
      .eq("id", row.id);

    if (error) throw error;
    return true;
  } catch (err) {
    console.error("❌ Row failed:", row.id);
    console.error(err?.message || err);

    // unlock so it can be retried later
    await supabase
      .from(TABLE_NAME)
      .update({ [LOCK_COL]: null, [LOCK_AT_COL]: null })
      .eq("id", row.id);

    throw err;
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 NOTES HYF → MCQ WORKER STARTED | ${WORKER_ID}`);
  console.log(`🎯 Table=${TABLE_NAME} | hyf_number=${HYF_NUMBER}`);
  console.log(`🔒 Lock=${LOCK_COL}/${LOCK_AT_COL} | IN=${IN_COL} | OUT=${OUT_COL}`);

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

        const success = results.filter((r) => r.status === "fulfilled").length;
        const failed = results.filter((r) => r.status === "rejected").length;

        console.log(`⚙️ Batch done | Success: ${success} | Failed: ${failed}`);
      }
    } catch (e) {
      console.error("❌ Worker loop error:", e?.message || e);
      await sleep(1200);
    }
  }
})();
