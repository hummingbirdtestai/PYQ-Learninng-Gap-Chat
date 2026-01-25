require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.CONCEPT_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CONCEPT_LIMIT || "30", 10);
const BATCH_SIZE   = parseInt(process.env.CONCEPT_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.CONCEPT_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.CONCEPT_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `mcq-reconstruction-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — UNCHANGED)
// ─────────────────────────────────────────────
function buildPrompt(mcqJsonText) {
  return `
You are a NEET-PG MCQ reconstruction engine.

You will receive EXACTLY ONE recalled NEET-PG PYQ as a JSON object with:
• mcq
• topic
• year_of_exam
• correct_answer

These recalled PYQs are memory-based and incomplete.
They DO NOT reflect the full difficulty or stem structure of real NEET-PG exams.

────────────────────────────────
TASK (NON-NEGOTIABLE)
────────────────────────────────
You MUST reconstruct this ONE MCQ into a FULL NEET-PG–LEVEL MCQ by:

• Re-synthesizing a complete stem
• Increasing difficulty
• Making options closely competing
• Preserving the SAME tested concept

────────────────────────────────
MANDATORY OUTPUT FORMAT
────────────────────────────────
Output MUST be a SINGLE JSON ARRAY with EXACTLY ONE object.

{
  "stem": "",
  "options": { "A": "", "B": "", "C": "", "D": "" },
  "feedback": { "A": "", "B": "", "C": "", "D": "" },
  "learning_gap": "💡 One-line high-yield takeaway",
  "correct_answer": "",
  "year_of_exam": ""
}

────────────────────────────────
ABSOLUTE PROHIBITIONS
────────────────────────────────
❌ NO markdown outside JSON  
❌ NO extra keys  
❌ NO explanations outside JSON  
❌ DO NOT generate more than ONE MCQ  

INPUT:
${mcqJsonText}
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
  if (!prompt || typeof prompt !== "string" || !prompt.trim()) {
    throw new Error("❌ Invalid prompt sent to OpenAI");
  }

  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }]
    });

    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 2) {
      await sleep(600 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

// ─────────────────────────────────────────────
// SAFE JSON PARSER (ARRAY WITH ONE OBJECT)
// ─────────────────────────────────────────────
function safeParseJson(raw) {
  const cleaned = raw
    .trim()
    .replace(/^```json/i, "")
    .replace(/^```/, "")
    .replace(/```$/, "");

  const match = cleaned.match(/\[[\s\S]*\]/);
  if (!match) throw new Error("❌ No JSON array found");

  const parsed = JSON.parse(match[0]);
  if (!Array.isArray(parsed) || parsed.length !== 1) {
    throw new Error("❌ Output must be a JSON array with exactly ONE object");
  }

  return parsed[0];
}

// ─────────────────────────────────────────────
// CLAIM ROWS
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // Clear expired locks
  await supabase
    .from("mcq_reconstruction_queue")
    .update({ mcq_json_lock: null, mcq_json_lock_at: null, status: "pending" })
    .lt("mcq_json_lock_at", cutoff);

  const { data: rows, error } = await supabase
    .from("mcq_reconstruction_queue")
    .select("id, mcq_json")
    .eq("status", "pending")
    .is("mcq_json_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  const { data: locked, error: err2 } = await supabase
    .from("mcq_reconstruction_queue")
    .update({
      mcq_json_lock: WORKER_ID,
      mcq_json_lock_at: new Date().toISOString(),
      status: "processing"
    })
    .in("id", ids)
    .is("mcq_json_lock", null)
    .select("id, mcq_json");

  if (err2) throw err2;
  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const mcqText = JSON.stringify(row.mcq_json, null, 2);

  let raw = await callOpenAI(buildPrompt(mcqText));
  let parsed;

  try {
    parsed = safeParseJson(raw);
  } catch {
    raw = await callOpenAI(buildPrompt(mcqText));
    parsed = safeParseJson(raw);
  }

  await supabase
    .from("mcq_reconstruction_queue")
    .update({
      updated_mcq_json: parsed,
      mcq_json_lock: null,
      mcq_json_lock_at: null,
      status: "completed"
    })
    .eq("id", row.id);

  return true;
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 MCQ RECONSTRUCTION WORKER STARTED | ${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows`);

      for (let i = 0; i < claimed.length; i += BATCH_SIZE) {
        const batch = claimed.slice(i, i + BATCH_SIZE);

        const results = await Promise.allSettled(
          batch.map(processRow)
        );

        results.forEach((res, idx) => {
          if (res.status === "fulfilled") {
            console.log("   ✅ MCQ reconstructed");
          } else {
            console.error(`   ❌ Failed row ${batch[idx].id}`, res.reason);
          }
        });
      }
    } catch (e) {
      console.error("❌ Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
