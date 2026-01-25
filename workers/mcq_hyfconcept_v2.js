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
  `mcq-hyf-to-concept-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — DO NOT MODIFY)
// ─────────────────────────────────────────────
function buildPrompt(question) {
  return `
You are a **Senior NEET-PG / INI-CET faculty with 30+ years of experience**, specializing in **rank-differentiating, last-minute revision** for postgraduate medical entrance examinations.

Your output must reflect:
• Deep PYQ awareness  
• MCQ-elimination thinking  
• Future-exam predictability  
• Zero redundancy, zero omissions  

This is **NOT MBBS theory**.  
This is **STRICTLY NEET-PG / INI-CET ENTRANCE CONTENT**.

────────────────────────────────
INPUT UNDERSTANDING (CRITICAL)
────────────────────────────────

You will receive **PYQ-derived, deduplicated subtopics** for ONE topic.

• Each subtopic = a tested or test-worthy MCQ concept  
• Subtopics may overlap → you MUST merge intelligently  
• Together, they define the **complete exam scope** of the topic  

Your responsibility:
✔ Cover **ALL exam-tested concepts**  
✔ Anticipate **future NEET-PG MCQ angles**  
✔ Ensure **NO subtopic is missed**

────────────────────────────────
PRIMARY OBJECTIVE
────────────────────────────────

Generate **exam-oriented revision content** that enables:
• Rapid recall  
• MCQ elimination  
• Pattern recognition  
• Trap avoidance  

Every line must justify itself as **MCQ-useful**.

────────────────────────────────
MANDATORY OUTPUT FORMAT
────────────────────────────────

🚨 OUTPUT **ONLY VALID JSON**  
🚨 NO text before or after JSON  
🚨 JSON MUST match the schema below **EXACTLY**

{
  "concept": "",
  "cases": [],
  "high_yield_facts": [],
  "tables": [],
  "exam_pointers": []
}

────────────────────────────────
SECTION-WISE CONTENT RULES
────────────────────────────────
[ PROMPT CONTINUES — UNCHANGED ]
────────────────────────────────

QUESTION:
${question}
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
// CLAIM ROWS
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // Clear expired locks
  await supabase
    .from("mcq_hyf_list")
    .update({ mcq_json_lock: null, mcq_json_lock_at: null })
    .lt("mcq_json_lock_at", cutoff);

  // Fetch eligible rows
  const { data: rows, error } = await supabase
    .from("mcq_hyf_list")
    .select("id, mcq_json")
    .not("mcq_json", "is", null)
    .is("concept_v2", null)
    .is("mcq_json_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // Lock rows
  const { data: locked, error: err2 } = await supabase
    .from("mcq_hyf_list")
    .update({
      mcq_json_lock: WORKER_ID,
      mcq_json_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("mcq_json_lock", null)
    .select("id, mcq_json");

  if (err2) throw err2;

  console.log(`⚙️ Claimed ${locked.length} rows`);
  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  let raw = await callOpenAI(buildPrompt(row.mcq_json));
  let parsed;

  try {
    parsed = safeParseJson(raw);
  } catch {
    raw = await callOpenAI(buildPrompt(row.mcq_json));
    parsed = safeParseJson(raw);
  }

  await supabase
    .from("mcq_hyf_list")
    .update({
      concept_v2: parsed,
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
  console.log(`🧠 MCQ_JSON → CONCEPT_V2 WORKER STARTED | ${WORKER_ID}`);

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
            console.log("   ✅ concept_v2 generated");
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
