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
  `concept-normalizer-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — DO NOT TOUCH)
// ─────────────────────────────────────────────
function buildPrompt(conceptText) {
  return `
You are a strict CONTENT NORMALIZATION and JSON PACKING engine.

Your job is ONLY to:
- Remove instructional / formatting noise accidentally included in content
- Preserve the actual educational content EXACTLY
- Pack the cleaned content into a fixed JSON structure

You must NOT:
- Rewrite content
- Add explanations
- Change wording, tone, or order
- Enforce pedagogy rules
- Impose new constraints
- “Improve” clarity or style

----------------------------------
GLOBAL RULES
----------------------------------

1. PRESERVE REAL CONTENT AS-IS
- Keep all educational text exactly as written
- Preserve **bold**, *italic*, ***bold+italic***
- Preserve Unicode symbols, arrows, bullets, emojis
- Preserve markdown formatting that is part of the content itself

2. REMOVE FORMAT / PROMPT NOISE  
Remove ONLY lines that are clearly **instructional artifacts**, such as:
- “High-yield facts should be…”
- “STRICT RULES:”
- “Each item ≤ 6 words”
- “Purpose: …”
- “Do not explain…”
- “Examiner expects…”
- “Rules:”
- Any meta text explaining HOW the content was generated

These lines are NOT educational content and must be discarded.

3. REMOVE DECORATIVE SEPARATORS  
Remove divider-only lines such as:
- ------
- ______
- ********
- ==================

4. REMOVE CONTENT-GENERATION INSTRUCTIONS (ADDED RULE)  
Remove any line that:
- Explains HOW the medical content should be written or remembered
- Mentions stylistic limits (e.g., word limits, highlighting limits)
- Mentions “buzzwords”, “exam recall”, “high-yield rules”, or similar phrasing
- Describes formatting constraints rather than medical knowledge itself

Examples that MUST be removed:
- “25 Most High-Yield Facts (buzzwords — exam recall)”
- “Each line ≤6 words; bold+italic limited to…”
- Any sentence that describes rules for creating the content rather than the content

These are NOT medical subject matter and must NOT appear in output.

----------------------------------
MANDATORY OUTPUT (JSON ONLY)
----------------------------------

Return a SINGLE valid JSON object with ONLY these keys:

{
  "concept": "",
  "cases": [],
  "high_yield_facts": [],
  "tables": [],
  "exam_pointers": []
}

No extra keys.  
No explanations.  
No markdown outside JSON.

----------------------------------
KEY PACKING RULES
----------------------------------

### "concept"
- Pack the **entire cleaned concept section**
- Keep headings, bullets, emphasis exactly as in content
- Do NOT summarize or restructure
- Value is a STRING

----------------------------------

### "cases"
- Each case MUST be an OBJECT
- Use section headings already present (History, Examination, etc.)
- Do NOT invent fields
- Omit missing sections silently
- Preserve wording exactly

----------------------------------

### "high_yield_facts"
- Pack only the **actual medical fact lines**
- Do NOT include titles or rules describing how facts were written
- Each array item = one original medical line
- Preserve ***bold+italic*** exactly where present

----------------------------------

### "tables"
- Each table MUST be preserved EXACTLY
- Keep original markdown table formatting
- Each table is a separate OBJECT:

{
  "title": "<table title if present, else null>",
  "markdown": "<entire table markdown exactly as given>"
}

- Do NOT convert tables to rows/columns
- Do NOT summarize or merge tables

----------------------------------

### "exam_pointers"
- Pack examiner tips, mnemonics, warnings, notes
- Exclude instructional rules about writing answers
- Preserve wording and formatting

----------------------------------
FINAL CHECK
----------------------------------

- Output JSON only
- Educational content unchanged
- Instructional / generative noise removed
- Markdown preserved
- No commentary

CONTENT:
${conceptText}
`;
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(s);
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
    .from("all_subjects_raw")
    .update({ concept_lock: null, concept_lock_at: null })
    .lt("concept_lock_at", cutoff);

  // Fetch unlocked rows needing normalization
  const { data, error } = await supabase
    .from("all_subjects_raw")
    .select("id, concept")
    .not("concept", "is", null)
    .is("concept_v2_final", null)
    .is("concept_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  // Lock rows
  const { data: locked, error: err2 } = await supabase
    .from("all_subjects_raw")
    .update({
      concept_lock: WORKER_ID,
      concept_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("concept_lock", null)
    .select("id, concept");

  if (err2) throw err2;
  return locked || [];
}

// ─────────────────────────────────────────────
// CLEAR LOCKS
// ─────────────────────────────────────────────
async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("all_subjects_raw")
    .update({ concept_lock: null, concept_lock_at: null })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.concept);

  let raw = await callOpenAI(prompt);
  let parsed;

  try {
    parsed = safeParseJson(raw);
  } catch {
    raw = await callOpenAI(prompt);
    parsed = safeParseJson(raw);
  }

  await supabase
    .from("all_subjects_raw")
    .update({
      concept_v2_final: parsed,
      concept_lock: null,
      concept_lock_at: null
    })
    .eq("id", row.id);

  return true;
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧹 CONCEPT NORMALIZER STARTED | ${WORKER_ID}`);

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
          if (res.status !== "fulfilled") {
            clearLocks([batch[idx].id]);
          }
        });
      }
    } catch (e) {
      console.error("❌ Worker error:", e);
      await sleep(1000);
    }
  }
})();
