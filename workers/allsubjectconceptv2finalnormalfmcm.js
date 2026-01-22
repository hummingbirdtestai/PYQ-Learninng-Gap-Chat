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
  `concept-normalizer-cm-fm-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ✅ ONLY THESE SUBJECTS
const INCLUDED_SUBJECTS = [
  "Community Medicine",
  "Forensic Medicine"
];

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — DO NOT TOUCH)
// ─────────────────────────────────────────────
function buildPrompt(conceptText) {
  return `
You are a STRICT CONTENT NORMALIZATION AND PACKING ENGINE.

Your job is ONLY to:
- Remove instructional / prompt / formatting noise accidentally included in content
- Preserve ONLY genuine medical / forensic / public-health subject content EXACTLY as written
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
- Any instructional section titles describing HOW content should be written

Examples that MUST be REMOVED completely:
- “25 Most High-Yield Facts (buzzwords — exam recall)”
- “(Each line ≤6 words; bold-italic limited to 1–2 words)”
- “Applied Forensic Exam Scenarios (5) — Each scenario gives…”
- “STRICT RULES”
- “IMPORTANT FORMATTING RULES”
- “Examiner expects…”
- “Do not explain…”
- “Purpose: …”
- “This section includes…”
- Any line explaining HOW content was generated rather than WHAT the medical content is

ONLY subject-matter content (medical / forensic / public-health) must remain.

--------------------------------------------------
GLOBAL PRESERVATION RULES
--------------------------------------------------

1. PRESERVE REAL SUBJECT CONTENT AS-IS
- Keep wording, tone, and order EXACTLY
- Preserve **bold**, *italic*, ***bold+italic***
- Preserve Unicode symbols, arrows, bullets, emojis
- Preserve markdown ONLY if it is part of subject content

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
- Pack the entire cleaned **core theory / principles / definitions**
- Includes central concepts, mechanisms, approaches, principles
- Keep headings, bullets, and emphasis exactly
- Do NOT summarize or restructure

--------------------------------------------------

"cases"  (FORENSIC & COMMUNITY MEDICINE ADAPTATION)

IMPORTANT:
- This key is OPTIONAL for Forensic Medicine and Community Medicine
- Use it ONLY if the content contains **explicitly titled applied scenarios**

If used:
- Treat **Scenarios / Applied Forensic Scenarios / Applied CM Scenarios** as structured cases
- These are NOT patient case histories

Each entry MUST be an OBJECT in this format:

{
  "Scenario title here": {
    "Core concept / definition": "...",
    "Relevant principle": "...",
    "Identification features": "...",
    "Opinion expected": "...",
    "Practical/legal implication": "..."
  }
}

Rules:
- Scenario title is mandatory
- Section names must come ONLY from the content
- Do NOT invent missing sections
- Omit missing sections silently
- Preserve wording exactly

If NO scenarios exist → return an EMPTY ARRAY [].

--------------------------------------------------

"high_yield_facts"
- Include ONLY actual subject facts / buzzword lines
- Remove any heading that describes rules, limits, or formatting
- Each array item = one original factual line
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
- Include exam tips, mnemonics, interpretive guidance, forensic opinions
- Exclude instructions on how to write answers
- Exclude meta or formatting advice
- Preserve wording and formatting exactly

--------------------------------------------------
FINAL VALIDATION CHECK (MANDATORY)
--------------------------------------------------

Before output, ensure:
- Output is JSON only
- Subject content is unchanged
- Prompt / meta / instructional content is fully removed
- "cases" is EMPTY if no explicit scenarios exist
- No instructional headings appear inside arrays
- Structure is deterministic and renderer-safe

CONTENT:
${conceptText}
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
// CLAIM ROWS (ONLY CM + FM, WITH COUNT)
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // 1️⃣ Clear expired locks
  await supabase
    .from("all_subjects_raw")
    .update({ concept_lock: null, concept_lock_at: null })
    .lt("concept_lock_at", cutoff);

  // 2️⃣ Count remaining eligible rows
  const { count: remaining, error: countErr } = await supabase
    .from("all_subjects_raw")
    .select("id", { count: "exact", head: true })
    .not("concept", "is", null)
    .is("concept_v2_final", null)
    .in("subject", INCLUDED_SUBJECTS);

  if (countErr) throw countErr;

  console.log(`📊 Remaining Community/Forensic rows: ${remaining ?? 0}`);

  // 3️⃣ Fetch rows to process
  const { data: rows, error } = await supabase
    .from("all_subjects_raw")
    .select("id, concept")
    .not("concept", "is", null)
    .is("concept_v2_final", null)
    .is("concept_lock", null)
    .in("subject", INCLUDED_SUBJECTS)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // 4️⃣ Lock rows
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

  console.log(`⚙️ Claimed ${locked.length} rows`);
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
  let raw = await callOpenAI(buildPrompt(row.concept));
  let parsed;

  try {
    parsed = safeParseJson(raw);
  } catch {
    raw = await callOpenAI(buildPrompt(row.concept));
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
  console.log(`🧹 CM + FM CONCEPT NORMALIZER STARTED | ${WORKER_ID}`);

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
            console.log("   ✅ concept_v2_final generated");
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
