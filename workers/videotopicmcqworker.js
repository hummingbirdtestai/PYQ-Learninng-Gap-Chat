require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

//──────────────────────────────────────────────
// SETTINGS
//──────────────────────────────────────────────
const MODEL        = process.env.MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_LIMIT || "150", 10);   // Claim big batch
const CONCURRENCY  = parseInt(process.env.MCQ_CONCURRENCY || "10", 10); // Process 10 at a time
const SLEEP_MS     = parseInt(process.env.MCQ_LOOP_SLEEP_MS || "200", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_LOCK_TTL_MIN || "10", 10);
const WORKER_ID    = process.env.WORKER_ID || `video-mcq-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

//──────────────────────────────────────────────
// PROMPT BUILDER
//──────────────────────────────────────────────
function buildPrompt(topic) {
  return `
You are an expert NEET-PG and USMLE question writer with 30 years of experience creating official-standard, high-yield MCQs (NEET-PG / USMLE / NBME level = moderate-hard).

🧩 FORMATTING RULES (apply globally to the entire output):
•⁠ ⁠Use Markup bold, italic, Unicode arrows (→ ↑ ↓), subscripts/superscripts (₁₂³⁺⁻), Greek (α β Δ μ), and minimal emojis (✅ ❌ 💡) wherever relevant.
•⁠ ⁠"stem": Real NEET-PG–style question (clinical case vignette style or Single Liner High Yield fact testing style MCQs depending on the topic).
•⁠ ⁠"feedback": "Concise reasoning of explanatory answer with emphasis in what to be remembered as Hack in NEETPG Exam to crack this topic".
•⁠ ⁠Maintain NEET-PG moderate-to-hard difficulty.
•⁠ ⁠No “EXCEPT” questions.
•⁠ ⁠Output only VALID JSON.
•⁠ ⁠Output exactly 1 MCQ.

JSON TEMPLATE (for ONE MCQ):
{
  "stem": "",
  "options": {
    "A": "",
    "B": "",
    "C": "",
    "D": ""
  },
  "feedback": "",
  "correct_answer": ""
}

TOPIC: ${topic}
`.trim();
}

//──────────────────────────────────────────────
// HELPERS
//──────────────────────────────────────────────
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

function isRetryable(e) {
  return /timeout|429|temporar|unavailable|RESET|ETIMEDOUT/i.test(String(e?.message || e));
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages,
      response_format: { type: "json_object" }
    });

    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw e;
  }
}

//──────────────────────────────────────────────
// CLAIM ROWS (mcq_json IS NULL)
//──────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // Release stale locks
  await supabase
    .from("video_table")
    .update({ phase_concept_lock: null, phase_concept_lock_at: null })
    .lt("phase_concept_lock_at", cutoff);

  // Pick rows
  const { data: rows, error } = await supabase
    .from("video_table")
    .select("id, topic")
    .is("mcq_json", null)
    .is("phase_concept_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // Lock them
  const { data: locked, error: lockErr } = await supabase
    .from("video_table")
    .update({
      phase_concept_lock: WORKER_ID,
      phase_concept_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .select("id, topic");

  if (lockErr) throw lockErr;

  return locked || [];
}

//──────────────────────────────────────────────
// CLEAR LOCKS (rollback)
//──────────────────────────────────────────────
async function clearLocks(ids) {
  if (!ids.length) return;

  await supabase
    .from("video_table")
    .update({
      phase_concept_lock: null,
      phase_concept_lock_at: null
    })
    .in("id", ids);
}

//──────────────────────────────────────────────
// PROCESS ONE ROW
//──────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.topic);

  const raw = await callOpenAI([{ role: "user", content: prompt }]);

  let mcqObj;
  try {
    mcqObj = JSON.parse(raw);
  } catch (e) {
    console.error("❌ INVALID JSON FROM OPENAI:", raw);
    throw new Error("OpenAI returned invalid JSON");
  }

  // Save to DB
  await supabase
    .from("video_table")
    .update({
      mcq_json: mcqObj,
      phase_concept_lock: null,
      phase_concept_lock_at: null
    })
    .eq("id", row.id);

  return true;
}

//──────────────────────────────────────────────
// RUN WITH CONCURRENCY
//──────────────────────────────────────────────
async function processWithConcurrency(rows) {
  let index = 0;
  let success = 0;

  async function workerThread() {
    while (index < rows.length) {
      const current = rows[index++];
      try {
        await processRow(current);
        success++;
      } catch (err) {
        console.error(`❌ Error processing row ${current.id}:`, err);
        await clearLocks([current.id]);
      }
    }
  }

  const workers = [];
  for (let i = 0; i < CONCURRENCY; i++) {
    workers.push(workerThread());
  }

  await Promise.all(workers);
  return success;
}

//──────────────────────────────────────────────
// MAIN LOOP
//──────────────────────────────────────────────
(async function main() {
  console.log(`🔥 Video MCQ Worker Running | concurrency=${CONCURRENCY} | batch=${LIMIT}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows`);

      const success = await processWithConcurrency(claimed);

      console.log(`✅ Completed ${success}/${claimed.length} MCQs`);
    } catch (err) {
      console.error("❌ Fatal Loop Error:", err);
      await sleep(1000);
    }
  }
})();
