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

1️⃣ \`concept\` — Central Exam Concepts

• Single **markdown string**
• Bullet points only (`-`)
• Focus on:
  – Core mechanisms  
  – Classifications  
  – Thresholds / cut-offs  
  – Key associations & exclusions  
• Merge overlapping ideas  
• Use ***bold + italic*** ONLY for recall-critical anchors  
• ❌ NO tables inside this field  
• ❌ NO numbered lists  

────────────────────────────────

2️⃣ \`cases\` — Clinical MCQ-Style Vignettes

• Generate **as many cases as needed** to cover ALL subtopics  
• Each case MUST be wrapped EXACTLY as:

{
  "Case X — Diagnosis": {
    "Clinical history": "",
    "Physical examination": "",
    "Investigations": "",
    "Differential": "",
    "Treatment": ""
  }
}

STRICT RULES:
• All **5 keys are mandatory** (spelling must match exactly)  
• Paragraph style (NEET-PG MCQ depth)  
• Reflect real exam vignettes  
• ❌ No bullet lists inside values  
• ❌ No teaching narration  

────────────────────────────────

3️⃣ \`high_yield_facts\` — CORE EXAM SECTION

• Generate **ALL HYFs required** to cover every subtopic  
• ❌ DO NOT cap or limit the number  
• Each HYF must:
  – Be ≤ **6 words**  
  – Be a single factual statement  
  – Be reversible (Q ↔️ A)  
  – Not repeat another HYF  

STRICT FORMATTING RULES:
• Use ***bold + italic*** on **ONLY 1–2 recall-critical words**  
• ❌ NEVER bold the entire sentence  
• ❌ No numbering or prefixes  
• Unicode allowed:
  → ↑ ↓ ± ≥ ≤  
  α β Δ μ  
  ₁₂³ ⁺ ⁻  

Example (VALID):
"***Subgaleal*** hemorrhage → hypovolemic shock"

Example (INVALID):
"***Subgaleal hemorrhage causes hypovolemic shock***"

────────────────────────────────

4️⃣ \`tables\` — Synoptic Revision Tables

Each table MUST be formatted EXACTLY as:

{
  "title": "Table X — Clear Exam Purpose",
  "markdown": "| Column A | Column B | Column C |\n|---|---:|---|\n| Row | Data | Data |"
}

STRICT TABLE RULES (NON-NEGOTIABLE):
• Pure markdown table only  
• Header row + separator row mandatory  
• ❌ No blank lines inside table  
• ❌ No bullets or line breaks inside cells  
• Inline markdown allowed inside cells  
• 2–4 columns ONLY  
• Every table must enable **MCQ elimination**

Recommended table intents:
• Differentiation  
• Classification / staging  
• Investigation → next step  
• Risk → consequence mapping  

────────────────────────────────

5️⃣ \`exam_pointers\` — Optional but Recommended

• Short, actionable exam pearls  
• Practical MCQ tips  
• ❌ No repetition of HYFs  
• One string = one pointer  

────────────────────────────────
GLOBAL NON-NEGOTIABLE RULES
────────────────────────────────

• NEET-PG / INI-CET relevance ONLY  
• No textbook narration  
• No filler content  
• No repetition across sections  
• No missing subtopic  
• No invented facts  
• FAIL the output if schema or rules are violated  

────────────────────────────────
FORMATTING & RENDERING RULES
────────────────────────────────

• JSON must be parseable without modification  
• Markdown + Unicode only  
• ❌ No HTML  
• ❌ No LaTeX  
• ❌ No outer code fences  
• Inline emphasis ONLY (***bold + italic***)  

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
  // 🚨 ABSOLUTE GUARANTEE — NEVER SEND NULL TO OPENAI
  if (typeof prompt !== "string" || !prompt.trim()) {
    throw new Error("❌ callOpenAI received invalid prompt");
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
// PROCESS ONE ROW (FIXED)
// ─────────────────────────────────────────────
async function processRow(row) {
  // 🚨 HARD GUARD — PREVENT NULL / EMPTY JSON FROM HITTING OPENAI
  if (
    !row.mcq_json ||
    typeof row.mcq_json !== "object" ||
    Array.isArray(row.mcq_json) ||
    Object.keys(row.mcq_json).length === 0
  ) {
    console.warn(`⚠️ Skipping invalid mcq_json | row=${row.id}`);

    await supabase
      .from("mcq_hyf_list")
      .update({
        mcq_json_lock: null,
        mcq_json_lock_at: null
      })
      .eq("id", row.id);

    return false;
  }

  // ✅ ALWAYS PASS STRING TO OPENAI
  const questionText = JSON.stringify(row.mcq_json, null, 2);

  if (!questionText || typeof questionText !== "string") {
    console.warn(`⚠️ Invalid questionText | row=${row.id}`);
  
    await supabase
      .from("mcq_hyf_list")
      .update({
        mcq_json_lock: null,
        mcq_json_lock_at: null
      })
      .eq("id", row.id);
  
    return false;
  }

  let raw = await callOpenAI(buildPrompt(questionText));
  let parsed;

  try {
    parsed = safeParseJson(raw);
  } catch {
    raw = await callOpenAI(buildPrompt(questionText));
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
          if (res.status === "fulfilled" && res.value) {
            console.log("   ✅ concept_v2 generated");
          } else if (res.status === "rejected") {
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
