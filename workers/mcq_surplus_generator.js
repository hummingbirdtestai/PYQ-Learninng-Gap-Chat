// /app/workers/mcq_surplus_generator.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.MCQ_GEN_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_GEN_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.MCQ_GEN_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.MCQ_GEN_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_GEN_LOCK_TTL_MIN || "15", 10);

// ✅ Runs for both subjects — matches DB spelling exactly
const SUBJECTS     = ["General Surgery", "Obstetrics and Gynaecology"];

const WORKER_ID =
  process.env.WORKER_ID ||
  `mcq-gen-worker-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;

// ---------- Prompt ----------
function buildPrompt(concept) {
  return `
You are a **NEET-PG Exam paper setter with 30 years of experience**, deeply familiar with actual NEET-PG PYQs and question-framing patterns seen in **AMBOSS, UWorld, NBME, and FIRST AID**.

From the following Concept & Explanation, create **exactly 1 MCQ** (moderate-to-severe difficulty, NEET-PG / NBME style).  
Each MCQ must be a **clinical case vignette** testing the most **high-yield concept** from the explanation.

🎯 STRICT OUTPUT RULES — Output valid JSON only (no Markdown, no array, no text outside).  
The output must be **one single JSON object** like this example:

{
  "stem": "A 12-year-old girl presents for school medical clearance but has no vaccination records. On exam there is a healed 4–5 mm scar over the left deltoid consistent with prior intradermal injection. Parents report the child received a vaccine as a neonate but cannot recall which one. Regarding the observed scar and BCG vaccination, **which of the following...?**",
  "mcq_key": "mcq_3",
  "options": {
    "A": "Presence of the deltoid scar excludes the need for any further TB testing because it proves lifelong immunity.",
    "B": "Absence of a scar reliably excludes prior BCG vaccination and indicates the child was not immunized.",
    "C": "A healed intradermal scar is commonly used as evidence of prior *BCG* vaccination, but absence of a scar does not definitively exclude prior vaccination.",
    "D": "A BCG scar indicates vaccine failure and predicts higher risk of severe childhood TB."
  },
  "learning_gap": "💡 Trap: Interpreting the scar too rigidly. Do not assume absence of scar = no vaccination, nor assume scar = complete protection. Use scar as supportive history; combine with exposure, symptoms, and testing when deciding further evaluation.",
  "correct_answer": "C",
  "high_yield_facts": "✅ Typical local reaction: intradermal papule → ulcer → healed **scar**. The scar is **commonly used as evidence** of prior BCG, but **no scar does not reliably rule out** past vaccination. Scar presence ≠ guaranteed sterilizing immunity."
}

🧩 Guidelines:
• Each stem must form a complete, self-contained vignette ending naturally with “Which of the following…?”.  
• Avoid “EXCEPT” or “All of the following”.  
• Include patient details, investigations, and clues.  
• Use crisp professional exam tone — no AI or textbook verbosity.  
• Correct answer must be a single letter A–D.  

Concept JSON:
${JSON.stringify(concept)}
`.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function isRetryable(e) {
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(
    String(e?.message || e)
  );
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
    .is("more_surplus_mcq_json", null)  // ✅ changed column here
    .lt("mcq_lock_at", cutoff);
}

async function claimRows(limit) {
  await freeStaleLocks();

  const { data, error } = await supabase
    .from("mock_test_mcqs_raw")
    .select("id, phase_json, subject")
    .in("subject", SUBJECTS)
    .is("more_surplus_mcq_json", null)   // ✅ changed column here
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
    .is("more_surplus_mcq_json", null)   // ✅ changed column here
    .is("mcq_lock", null)
    .select("id, phase_json, subject");

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

  if (typeof jsonOut !== "object" || Array.isArray(jsonOut)) {
    throw new Error("Expected single JSON object for id=" + row.id);
  }

  const { error } = await supabase
    .from("mock_test_mcqs_raw")
    .update({
      more_surplus_mcq_json: jsonOut, // ✅ writes to new column
      mcq_lock: null,
      mcq_lock_at: null,
      updated_at: new Date().toISOString(),
    })
    .eq("id", row.id);

  if (error)
    throw new Error("Update failed id=" + row.id + ": " + error.message);
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
  console.log("🧩 MCQ Surplus Generator Worker", WORKER_ID, "| model =", MODEL);
  console.log("🎯 Target subjects:", SUBJECTS.join(", "));
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }
      console.log("⚙️ Claimed =", claimed.length);
      const updated = await processBatch(claimed);
      console.log("✅ Updated =", updated, "of", claimed.length);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
