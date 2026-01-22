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
You are a STRICT CONTENT NORMALIZATION AND PACKING ENGINE.

Your job is ONLY to:
- Remove instructional / prompt / formatting noise accidentally included in content
- Preserve ONLY genuine medical subject content EXACTLY as written
- Pack the cleaned content into a renderer-safe fixed JSON structure

You must NOT:
- Rewrite, paraphrase, summarize, or reorder content
- Add explanations or new headings
- Improve clarity, style, pedagogy, or structure
- Introduce new fields or inferred labels

--------------------------------------------------
ABSOLUTE EXCLUSION RULE (CRITICAL)
--------------------------------------------------

The following MUST NEVER appear in output, even if present in input:

- Any prompt instructions
- Any generation rules
- Any meta descriptions of format
- Any constraints about how content was written
- Any buzzword rules, limits, or guidance

Examples that MUST be REMOVED completely:
- “25 Most High-Yield Facts (buzzwords — exam recall)”
- “(Each line ≤6 words; bold+italic limited to 1–2 words)”
- “STRICT RULES”
- “IMPORTANT FORMATTING RULES”
- “Examiner expects…”
- “Do not explain…”
- “Purpose: …”
- Any line explaining HOW content was generated rather than WHAT the medical content is

ONLY medical subject matter must remain.

--------------------------------------------------
GLOBAL PRESERVATION RULES
--------------------------------------------------

1. PRESERVE REAL MEDICAL CONTENT AS-IS
- Keep wording, tone, and order EXACTLY
- Preserve **bold**, *italic*, ***bold+italic***
- Preserve Unicode symbols, arrows, bullets, emojis
- Preserve markdown ONLY if it is part of medical content

2. REMOVE DECORATIVE SEPARATORS
Remove divider-only lines such as:
------
******
=====
______

--------------------------------------------------
MANDATORY OUTPUT (JSON ONLY)
--------------------------------------------------

Return ONE valid JSON object.

No explanations.
No markdown outside JSON.
No extra keys.

{
  "concept": "",
  "cases": [],
  "high_yield_facts": [],
  "tables": [],
  "exam_pointers": []
}

--------------------------------------------------
STRICT STRUCTURE RULES (RENDERER-SAFE)
--------------------------------------------------

"concept"
- Type: STRING
- Pack the entire cleaned concept section
- Keep headings, bullets, and emphasis exactly
- Do NOT summarize or restructure

--------------------------------------------------

"cases"  (STRICTLY ENFORCED)

Each case MUST be an OBJECT in this exact format:

{
  "Case title here": {
    "Clinical history": "...",
    "Physical exam": "...",
    "Investigations": "...",
    "Differential": "...",
    "Treatment": "..."
  }
}

Rules:
- Each case is ONE object
- Case title is mandatory
- Section names must come only from the content
- Do NOT invent missing sections
- Omit missing sections silently
- Preserve wording exactly

Flat or untitled cases are INVALID.

--------------------------------------------------

"high_yield_facts"
- Include ONLY actual medical buzzword lines
- Do NOT include section titles, rules, constraints, or meta commentary
- Each array item = one original medical fact line
- Preserve ***bold+italic*** exactly

--------------------------------------------------

"tables"

Each table MUST be a separate OBJECT:

{
  "title": "<table title if present, else null>",
  "markdown": "<ENTIRE table markdown EXACTLY as given>"
}

Rules:
- Preserve markdown verbatim
- Do NOT convert tables to rows or columns
- Do NOT merge, summarize, or reinterpret tables

--------------------------------------------------

"exam_pointers"
- Include exam tips, mnemonics, warnings, clinical pearls
- Exclude instructions on how to write answers
- Preserve wording and formatting exactly

--------------------------------------------------
FINAL VALIDATION CHECK
--------------------------------------------------

Before output, ensure:
- Output is JSON only
- Medical content is unchanged
- Prompt / meta / instructional content is fully removed
- All cases are titled objects
- No instructional headings exist inside arrays
- Structure is renderer-safe and deterministic
{
  "concept": "STRING — full cleaned medical concept content exactly as written, including headings, bullets, emphasis, and markdown that is part of the medical content.",
  "cases": [
    {
      "Case title here": {
        "Clinical history": "Exact medical text as written.",
        "Physical exam": "Exact medical text as written.",
        "Investigations": "Exact medical text as written.",
        "Differential": "Exact medical text as written.",
        "Treatment": "Exact medical text as written."
      }
    }
  ],
  "high_yield_facts": [
    "Exact medical buzzword line as written",
    "Another exact medical buzzword line"
  ],
  "tables": [
    {
      "title": "Table title exactly as written, or null if not present",
      "markdown": "| Entire table markdown preserved exactly |\n|---|---|\n| As provided | No changes |"
    }
  ],
  "exam_pointers": [
    "Exact exam tip, mnemonic, or clinical pearl as written"
  ]
}

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
