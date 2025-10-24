require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.MCQ_GEN_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_GEN_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.MCQ_GEN_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.MCQ_GEN_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_GEN_LOCK_TTL_MIN || "15", 10);
const SUBJECT      = process.env.MCQ_GEN_SUBJECT || "NEET-PG";
const WORKER_ID    = process.env.WORKER_ID || `mcq-gen-worker-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(concept) {
  return `
You are an expert NEET-PG and USMLE question writer with 30 years of experience creating official-standard, high-yield MCQs (NEET-PG / USMLE / NBME level = moderate-hard).
From the given Concept & Explanation, write exactly *2* MCQs that look and read like real NEET-PG PYQs.
🎯 Output valid JSON only — no text outside.

[
  {
    "stem": "Real NEET-PG–style question (case or applied fact) ending with 'Which of the following…?'. Use *bold*, _italic_, arrows (→ ↑ ↓), subscripts/superscripts (₁₂³⁺⁻), minimal emojis (✅ ❌ 💡).",
    "mcq_key": "mcq_1",
    "options": {"A": "...","B": "...","C": "...","D": "..."},
    "feedback": {
      "wrong": "❌ Brief rationale using **bold**, _italic_, arrows.",
      "correct": "✅ Concise reasoning with **bold**, _italic_, arrows, subscripts/superscripts."
    },
    "learning_gap": "💡 One-line high-yield takeaway with **bold**, _italic_.",
    "correct_answer": "A"
  },
  {
    "stem": "...",
    "mcq_key": "mcq_2",
    "options": {"A": "...","B": "...","C": "...","D": "..."},
    "feedback": {"wrong": "...","correct": "..."},
    "learning_gap": "...",
    "correct_answer": "B"
  }
]

🧩 Guidelines:
• Match real NEET-PG phrasing, tone, and difficulty seen in PYQs.
• Let the question form (clinical, applied, data-based, etc.) emerge naturally from the Concept.
• Ensure 1 correct + 3 plausible distractors.
• Avoid “except / all of the following”; prefer “Which of the following is most likely…”.
• Use concise, exam-oriented language — no AI or textbook tone.

Concept JSON:
${JSON.stringify(concept)}
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
    throw new Error(`❌ Failed to parse JSON for id=${id}: ${err.message}. Raw: ${raw.slice(0,200)}`);
  }
}

// ---------- Locks ----------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();
  await supabase
    .from("mock_test_mcqs_raw")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .is("mcq_json_raw", null)
    .lt("mcq_lock_at", cutoff);
}

async function claimRows(limit) {
  await freeStaleLocks();
  const { data, error } = await supabase
    .from("mock_test_mcqs_raw")
    .select("id, phase_json")
    .is("mcq_json_raw", null)
    .is("mcq_lock", null)
    .limit(limit);
  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);
  const { data: locked, error: e2 } = await supabase
    .from("mock_test_mcqs_raw")
    .update({ mcq_lock: WORKER_ID, mcq_lock_at: new Date().toISOString() })
    .in("id", ids)
    .is("mcq_json_raw", null)
    .is("mcq_lock", null)
    .select("id, phase_json");
  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("mock_test_mcqs_raw")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .in("id", ids);
}

// ---------- Process ----------
async function processRow(row) {
  const prompt = buildPrompt(row.phase_json);
  const raw = await callOpenAI(prompt);
  const jsonOut = safeParseJson(raw, row.id);

  if (!Array.isArray(jsonOut)) {
    throw new Error(`❌ Expected array of MCQs for id=${row.id}`);
  }

  const { error } = await supabase
    .from("mock_test_mcqs_raw")
    .update({
      mcq_json_raw: jsonOut,
      mcq_lock: null,
      mcq_lock_at: null,
      updated_at: new Date().toISOString(),
    })
    .eq("id", row.id);

  if (error) throw new Error(`Update failed id=${row.id}: ${error.message}`);
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

// ---------- Main ----------
(async function main() {
  console.log(`🧩 MCQ Generator Worker ${WORKER_ID} | model=${MODEL}`);
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
