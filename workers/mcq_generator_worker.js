// /app/workers/mcq_generator_worker.js
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
You are a **NEET-PG Exam paper setter with 30 years of experience**, deeply familiar with actual NEET-PG PYQs and question framing patterns seen in **AMBOSS, UWorld, NBME, and FIRST AID**.

From the following Concept & Explanation, create **exactly 3 MCQs** (moderate-to-severe difficulty, NEET-PG / NBME style).  
Each MCQ must be a **clinical case vignette** testing the most **high-yield concept** from the explanation.

🎯 STRICT OUTPUT RULES — Output valid JSON only, no extra text outside.

[
  {
    "mcq_key": "mcq_1",
    "stem": "Clinical vignette ending with 'Which of the following...?' Use **bold**, _italic_, Unicode arrows (→ ↑ ↓), subscripts/superscripts (₁₂³⁺⁻), and medical symbols (Δ, α, β, μ, etc.).",
    "options": {
      "A": "...",
      "B": "...",
      "C": "...",
      "D": "..."
    },
    "correct_answer": "A",
    "high_yield_facts": "✅ Concise explanation covering the concept tested — key reasoning, one-line fact pearls (as seen in real NEET-PG review books). Use **bold**, _italic_, and Unicode.",
    "learning_gap": "💡 Explain the most common confusion or trap leading to wrong answers, and how to avoid it in the real exam."
  },
  {
    "mcq_key": "mcq_2",
    "stem": "...",
    "options": {"A": "...", "B": "...", "C": "...", "D": "..."},
    "correct_answer": "B",
    "high_yield_facts": "...",
    "learning_gap": "..."
  },
  {
    "mcq_key": "mcq_3",
    "stem": "...",
    "options": {"A": "...", "B": "...", "C": "...", "D": "..."},
    "correct_answer": "C",
    "high_yield_facts": "...",
    "learning_gap": "..."
  }
]

🧩 Guidelines:
• Follow NEET-PG exam phrasing — “Which of the following is most likely… / best next step… / most accurate statement…”.
• Avoid “EXCEPT” or “All of the following”.
• Each stem must be clinical, including **patient age, symptoms, investigations,** or **biochemical clues**.
• Use crisp, professional exam tone — no AI or textbook verbosity.
• Correct answer must be a single alphabet (A–D).
• Each MCQ should test different aspects of the same concept.

Concept JSON:
${JSON.stringify(concept)}
`.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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
    const cleaned = raw
      .trim()
      .replace(/^```json\s*/i, "")
      .replace(/^```/, "")
      .replace(/```$/, "");
    return JSON.parse(cleaned);
  } catch (err) {
    const preview = raw ? raw.slice(0, 200).replace(/\n/g, "\\n") : "";
    console.error("❌ Failed to parse JSON for id", id, err.message);
    console.error("Raw:", preview);
    throw new Error("Failed to parse JSON for id=" + id + ": " + err.message);
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

  const ids = data.map((r) => r.id);
  const { data: locked, error: e2 } = await supabase
    .from("mock_test_mcqs_raw")
    .update({
      mcq_lock: WORKER_ID,
      mcq_lock_at: new Date().toISOString(),
    })
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
    throw new Error("Expected array of MCQs for id=" + row.id);
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

  if (error) throw new Error("Update failed id=" + row.id + ": " + error.message);
  return { updated: 1 };
}

async function processBatch(rows) {
  const chunks = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    chunks.push(rows.slice(i, i + BATCH_SIZE));
  }

  let updated = 0;
  for (const chunk of chunks) {
    const results = await Promise.allSettled(chunk.map(processRow));
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (r.status === "fulfilled") {
        updated += r.value.updated;
      } else {
        console.error(r.reason?.message || r.reason);
        await clearLocks([chunk[i].id]);
      }
    }
  }
  return updated;
}

// ---------- Main Loop ----------
(async function main() {
  console.log("🧩 MCQ Generator Worker", WORKER_ID, "| model =", MODEL);
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }
      console.log("⚙️ claimed =", claimed.length);
      const updated = await processBatch(claimed);
      console.log("✅ updated =", updated, "of", claimed.length);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
