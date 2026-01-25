require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.CONCEPT_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CONCEPT_LIMIT || "20", 10);
const BATCH_SIZE   = parseInt(process.env.CONCEPT_BATCH_SIZE || "3", 10);
const SLEEP_MS     = parseInt(process.env.CONCEPT_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.CONCEPT_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `mcq-hyf-normalizer-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — DO NOT MODIFY)
// ─────────────────────────────────────────────
function buildPrompt(mcqText) {
  return `
Remove the Repeats and derive Unique lIST OF Questions Combine , Convert the unique list of MCQs into High Yield facts less than 10 Words . Group the into High Yield facts under Core Sub-Topic , if they belong to same topic but different aspects . Give me the Final List as , JSON the Keys Subtopic : HYF 1 : , HYF 2 : ..... , Subtopic : HYF 1 : , HYF 2 : ..... Give deduplicated, combined, and cleaned final list, with closely related questions merged into a single comprehensive exam-worthy question.

👉 No entries missed  
👉 No repetition  
👉 Only meaningful consolidation  

Formatting Rules (STRICT)  
Use **bold**, *italic*  
Use Unicode FOR arrows (→ ↑ ↓)  
Use subscripts/superscripts (₁₂³⁺⁻)  
Use Greek letters (α β Δ μ)

✔ No repetition  
✔ All PYQ angles covered  
✔ Each HYF is <10 words  
✔ Deduplicated • Combined • Cleaned  

STEP 1 — DEDUPLICATED & MERGED CORE CONCEPT  
Only one core disease entity here → Botulism  
Different questions = different aspects → merged cleanly.  

STEP 2 — HIGH-YIELD FACTS (≤10 words)  
Grouped under Core Sub-Topic | Exam-ready  

MCQs:
${JSON.stringify(mcqText, null, 2)}
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
// SAFE JSON PARSE
// ─────────────────────────────────────────────
function safeParseJson(raw) {
  const txt = raw
    .trim()
    .replace(/^```json/i, "")
    .replace(/^```/, "")
    .replace(/```$/, "");

  const match = txt.match(/\{[\s\S]*\}/);
  if (!match) throw new Error("❌ No JSON object found");

  return JSON.parse(match[0]);
}

// ─────────────────────────────────────────────
// CLAIM ROWS (MCQ HYF TABLE)
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // 1️⃣ Clear expired locks
  await supabase
    .from("mcq_hyf_list")
    .update({ mcq_json_lock: null, mcq_json_lock_at: null })
    .lt("mcq_json_lock_at", cutoff);

  // 2️⃣ Fetch eligible rows
  const { data: rows, error } = await supabase
    .from("mcq_hyf_list")
    .select("id, mcq")
    .not("mcq", "is", null)
    .is("mcq_json", null)
    .is("mcq_json_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // 3️⃣ Lock rows
  const { data: locked, error: err2 } = await supabase
    .from("mcq_hyf_list")
    .update({
      mcq_json_lock: WORKER_ID,
      mcq_json_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("mcq_json_lock", null)
    .select("id, mcq");

  if (err2) throw err2;

  console.log(`⚙️ Claimed ${locked.length} rows`);
  return locked || [];
}

// ─────────────────────────────────────────────
// CLEAR LOCKS
// ─────────────────────────────────────────────
async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("mcq_hyf_list")
    .update({ mcq_json_lock: null, mcq_json_lock_at: null })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  let raw = await callOpenAI(buildPrompt(row.mcq));
  let parsed;

  try {
    parsed = safeParseJson(raw);
  } catch {
    raw = await callOpenAI(buildPrompt(row.mcq));
    parsed = safeParseJson(raw);
  }

  await supabase
    .from("mcq_hyf_list")
    .update({
      mcq_json: parsed,
      mcq_json_lock: null,
      mcq_json_lock_at: null
    })
    .eq("id", row.id);

  return true;
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 MCQ → HYF NORMALIZER STARTED | ${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      for (let i = 0; i < claimed.length; i += BATCH_SIZE) {
        const batch = claimed.slice(i, i + BATCH_SIZE);

        const results = await Promise.allSettled(
          batch.map(processRow)
        );

        results.forEach((res, idx) => {
          if (res.status === "fulfilled") {
            console.log("   ✅ mcq_json generated");
          } else {
            console.error(`   ❌ Failed row ${batch[idx].id}`, res.reason);
            clearLocks([batch[idx].id]);
          }
        });
      }
    } catch (e) {
      console.error("❌ Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
