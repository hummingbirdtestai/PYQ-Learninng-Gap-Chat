require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.MCQ_QA_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_QA_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.MCQ_QA_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.MCQ_QA_LOOP_SLEEP_MS || "300", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_QA_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `mcq-qa-worker-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

const TABLE_NAME = "mcq_hyf_list";
const IN_COL     = "mcq";
const OUT_COL    = "mcq_qa_json";
const LOCK_COL   = "mcq_json_lock";
const LOCK_AT_COL= "mcq_json_lock_at";

// ─────────────────────────────────────────────
// PROMPT (USE EXACTLY AS PROVIDED — NO CHANGE)
// ─────────────────────────────────────────────
function buildPrompt(mcqJson) {
  return `
You are a strict NEET-PG MCQ formatter and an expert NEET-PG examiner with 30+ years of experience.

Follow ALL rules EXACTLY. No deviation allowed.

--------------------------------------------------
1. OUTPUT FORMAT
--------------------------------------------------

Return a SINGLE JSON array.

Each MCQ MUST contain EXACTLY these keys:

stem

options

correct_answer

high_yield_facts

❌ DO NOT add or remove any keys
❌ DO NOT include extra fields
--------------------------------------------------
1A. COVERAGE RULE (MANDATORY — NO SKIPPING)
--------------------------------------------------

✔ PROCESS ALL MCQs present in the input JSON array

✔ DO NOT skip any MCQ under any circumstance

✔ EACH MCQ must generate EXACTLY 5 Q&A pairs in "high_yield_facts"

✔ Total output MCQs MUST be equal to input MCQs

❌ DO NOT reduce number of MCQs
❌ DO NOT partially process MCQs
❌ DO NOT split output into multiple responses
--------------------------------------------------
2. STEM RULE (ABSOLUTE — NO MODIFICATION)
--------------------------------------------------

PRESERVE the original MCQ stem EXACTLY

DO NOT rewrite

DO NOT paraphrase

✔ ONLY IF clearly incomplete:
→ Add MINIMAL words to complete meaning

✔ REMOVE:

Question numbering (e.g., 1., 24., 130.)

✔ DO NOT INCLUDE:

Options inside the stem

--------------------------------------------------
3. YEAR APPENDING RULE (MANDATORY)
--------------------------------------------------

Each MCQ will contain: "year_of_exam"

Append at END of stem:

(NEETPG - <year_of_exam>)

RULES:

Append ONLY ONCE

DO NOT duplicate

Always at absolute end

--------------------------------------------------
4. OPTIONS
--------------------------------------------------

Keep EXACTLY same options (A–D)

DO NOT modify wording

DO NOT reorder

--------------------------------------------------
5. CORRECT ANSWER
--------------------------------------------------

Keep EXACTLY same

Preserve format (A / B / C / D or given format)

--------------------------------------------------
6. HIGH_YIELD_FACTS = EXAMINER QUESTIONS (CRITICAL)
--------------------------------------------------

These are NOT facts OR SINGLE LINERS 

They MUST be:

👉 Most probable FUTURE NEET-PG QUESTIONS that can surface in real exam
👉 Based on PYQ patternand should be moist probable to ask in future NEETPG Exam 
✔ Wherever possible → use Clinical Case Vignette style but limited to amximum 10 Word Sentence
👉 Derived from same concept

--------------------------------------------------
EXAMINER THINKING RULE (VERY IMPORTANT)
--------------------------------------------------

Each question MUST follow:

✔ Frequently asked concept OR repeat PYQ pattern
✔ Core concept that differentiates rank
✔ Common trap tested in NEETPG
✔ Directly testable in NEETPG  MCQ
✔ Clinically or conceptually high-yield


❌ DO NOT include:

Rare facts

Paragraph explanations

Theoretical descriptions

--------------------------------------------------
DIFFICULTY STANDARD
--------------------------------------------------

Questions MUST match:

👉 Actual NEETPG paper level
👉 Crisp, exam-style phrasing

--------------------------------------------------
FORMAT (STRICT)
--------------------------------------------------
**Question 1:** <clinical vignette / exam-level question>  
**Answer:** <precise answer>  

**Question 2:** <clinical vignette / exam-level question>  
**Answer:** <precise answer>  

**Question 3:** <clinical vignette / exam-level question>  
**Answer:** <precise answer>  

**Question 4:** <clinical vignette / exam-level question>  
**Answer:** <precise answer>  

**Question 5:** <clinical vignette / exam-level question>  
**Answer:** <precise answer>
--------------------------------------------------
STRICT RULES
--------------------------------------------------

EXACTLY 5 Q&A pairs

Questions MUST be directly askable in NEETPG

No explanations

No options

No extra text

--------------------------------------------------
EMPHASIS RULE
--------------------------------------------------

Use bold for key exam terms

Use italic ONLY for traps

--------------------------------------------------
SPACING RULE
--------------------------------------------------

Maintain exact formatting

Question → newline → Answer → blank line

--------------------------------------------------
7. STRICT JSON
--------------------------------------------------

Output MUST be valid JSON

No trailing commas

No extra text outside JSON

--------------------------------------------------
8. FAILURE CONDITIONS
--------------------------------------------------

Output is WRONG if:

Stem is modified other than to bmake a incomplete stem to become complete 

Options altered

Question numbering retained in stem

Options included in stem

Questions are not exam-level

Questions are not high-yield

Formatting incorrect

Keys missing

INPUT MCQ JSON:
${JSON.stringify(mcqJson)}
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
  if (!text) throw new Error("Empty model response");

  let cleaned = text
    .replace(/```json/gi, "")
    .replace(/```/g, "")
    .trim();

  const firstBrace = cleaned.indexOf("[");
  const lastBrace  = cleaned.lastIndexOf("]");

  if (firstBrace !== -1 && lastBrace !== -1) {
    cleaned = cleaned.substring(firstBrace, lastBrace + 1);
  }

  return JSON.parse(cleaned);
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

  await supabase
    .from(TABLE_NAME)
    .update({ [LOCK_COL]: null, [LOCK_AT_COL]: null })
    .lt(LOCK_AT_COL, cutoff);

  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(`id, ${IN_COL}`)
    .not(IN_COL, "is", null)
    .is(OUT_COL, null)
    .is(LOCK_COL, null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  const { data: locked, error: err2 } = await supabase
    .from(TABLE_NAME)
    .update({
      [LOCK_COL]: WORKER_ID,
      [LOCK_AT_COL]: new Date().toISOString()
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

    await supabase
      .from(TABLE_NAME)
      .update({
        [OUT_COL]: json,
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id);

    return true;

  } catch (err) {
    console.error("❌ Row failed:", row.id);
    console.error(err.message);

    await supabase
      .from(TABLE_NAME)
      .update({
        [LOCK_COL]: null,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id);

    throw err;
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 MCQ → QA WORKER STARTED | ${WORKER_ID}`);

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

        const success = results.filter(r => r.status === "fulfilled").length;
        const failed  = results.filter(r => r.status === "rejected").length;

        console.log(`⚙️ Batch done | Success: ${success} | Failed: ${failed}`);
      }

    } catch (e) {
      console.error("❌ Worker loop error:", e.message);
      await sleep(1200);
    }
  }
})();
