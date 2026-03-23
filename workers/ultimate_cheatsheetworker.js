require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.BLUEPRINT_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.BLUEPRINT_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.BLUEPRINT_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.BLUEPRINT_LOOP_SLEEP_MS || "400", 10);
const LOCK_TTL_MIN = parseInt(process.env.BLUEPRINT_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `blueprint-worker-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

const TABLE_NAME   = "mcq_hyf_list";
const IN_COL       = "mcq_json";
const TOPIC_COL    = "new_topic";
const OUT_COL      = "ultimate_cheatsheet";
const LOCK_COL     = "mcq_json_lock";
const LOCK_AT_COL  = "mcq_json_lock_at";

console.log("🚀 BLUEPRINT WORKER STARTED:", WORKER_ID);

// ─────────────────────────────────────────────
// PROMPT (USE EXACTLY AS PROVIDED)
// ─────────────────────────────────────────────
function buildPrompt(inputJson) {
  return `
You are a 30-year experienced NEET-PG / INI-CET paper setter, examiner, and blueprint architect.

INPUT:
You will receive a JSON containing PYQ-derived High Yield Facts (HYFs) for a specific topic.

TASK:
Transform the input into a COMPLETE "Blueprint Cheat Sheet" JSON for NEET-PG preparation.

OUTPUT REQUIREMENTS (STRICT):
1. Output ONLY valid JSON. No explanations. No markdown outside JSON.
2. Preserve the topic and expand it into a FULL syllabus-level coverage for NEET-PG.
3. Keep original HYFs but ENRICH with:
   - Missing high-yield facts
   - Repeated PYQ patterns
   - Exam traps (VERY IMPORTANT)
   - Clinical correlations (only high-yield)
4. Structure MUST be hierarchical:
   {
     "Topic Name": {
       "Subtopic": {
         "HYF 1": "...",
         "HYF 2": "...",
         "EXAM TRAP": "...",
         ...
       }
     }
   }
5. Use BUZZWORD style:
   - Keep each line short (≤15 words)
   - Arrow format: "→"
   - Avoid sentences, use fragments

FORMATTING RULES:
6. Use Markdown styling INSIDE JSON strings:
   - ***bold italic*** for key terms
7. Use Unicode symbols wherever useful:
   - Superscripts: ¹²³
   - Subscripts: T₄, T₁₀
   - Greek: α, β
   - Arrows: →
8. Keep numbers explicit (e.g., "15–20 ducts", "3ʳᵈ–6ᵗʰ")

EXAM INTELLIGENCE RULES:
9. Add "EXAM TRAP" in EVERY subtopic:
   - Focus on common confusions
   - Reversal tricks
   - PYQ distractor patterns
10. Add "Rapid Fire Memory Grid" at end:
   - 5–10 ultra-short recall bullets

CONTENT RULES:
11. Cover entire topic, NOT just given HYFs
12. Include:
   - Anatomy basics
   - Variations frequently asked
   - Surface anatomy
   - Clinical procedures (only exam-relevant)

CONSTRAINTS:
13. Do NOT repeat same fact across sections
14. Do NOT exceed depth beyond NEET-PG relevance
15. Keep output compact but complete

FAIL CONDITIONS (STRICTLY AVOID):
- No prose explanation
- No missing EXAM TRAP
- No invalid JSON
- No long paragraphs

GOAL:
Produce a high-density, exam-oriented, trap-focused blueprint that enables instant recall and prevents mistakes in NEET-PG MCQs.

INPUT JSON:
${JSON.stringify(inputJson)}
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

function extractJson(text) {
  if (!text) throw new Error("Empty response");

  let cleaned = text.replace(/```json/gi, "").replace(/```/g, "").trim();
  const first = cleaned.indexOf("{");
  const last  = cleaned.lastIndexOf("}");

  if (first === -1 || last === -1)
    throw new Error("Invalid JSON structure");

  return JSON.parse(cleaned.substring(first, last + 1));
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

  // release stale locks
  await supabase
    .from(TABLE_NAME)
    .update({ [LOCK_COL]: null, [LOCK_AT_COL]: null })
    .lt(LOCK_AT_COL, cutoff);

  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(`id, ${IN_COL}, ${TOPIC_COL}`)
    .not(IN_COL, "is", null)
    .is(OUT_COL, null)
    .is(LOCK_COL, null)
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  const { data: locked } = await supabase
    .from(TABLE_NAME)
    .update({
      [LOCK_COL]: WORKER_ID,
      [LOCK_AT_COL]: new Date().toISOString()
    })
    .in("id", ids)
    .is(LOCK_COL, null)
    .select(`id, ${IN_COL}, ${TOPIC_COL}`);

  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  try {

    const raw  = await callOpenAI(buildPrompt(row[IN_COL]));
    const json = extractJson(raw);

    await supabase
      .from(TABLE_NAME)
      .update({
        [OUT_COL]: json,
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id);

    console.log("✅ Blueprint saved:", row.id);

  } catch (err) {

    console.error("❌ Failed:", row.id, err.message);

    await supabase
      .from(TABLE_NAME)
      .update({
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id);
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {

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
