require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_LIMIT || "150", 10);
const BATCH_SIZE   = parseInt(process.env.MCQ_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.MCQ_LOOP_SLEEP_MS || "200", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_LOCK_TTL_MIN || "10", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `mcq-hyf-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — UNCHANGED)
// ─────────────────────────────────────────────
function buildPrompt(mcqJsonText) {
  return `
You are a DETERMINISTIC MEDICAL EXAM CONTENT ENGINE.

Your task is to generate LAST-MINUTE REVISION content for NEET-PG / INI-CET PYQs.

────────────────────────────────
ABSOLUTE OUTPUT SCHEMA (LOCKED)
────────────────────────────────

You MUST output ONLY a SINGLE VALID JSON object.

This JSON MUST contain EXACTLY THREE TOP-LEVEL KEYS — NO MORE, NO LESS:

1️⃣ "concept"  
2️⃣ "mnemonic"  
3️⃣ "tables"

❌ If ANY other key appears (e.g. feedback, learning_gap, explanation, notes, extras),
the output is INVALID.

❌ Do NOT nest alternative keys.
❌ Do NOT rename keys.
❌ Do NOT add metadata.

────────────────────────────────
KEY-WISE TYPE CONSTRAINTS (STRICT)
────────────────────────────────

1️⃣ "concept"
• MUST be an ARRAY of STRINGS
• EXACTLY **5 items** (no more, no less)
• Each item = ONE high-yield exam fact
• NEET-PG frequency focused
• Use **bold**, *italic*, Unicode arrows (→ ↑ ↓), subscripts/superscripts (₁₂³⁺⁻), Greek letters (α β γ)
• NO paragraphs, NO sub-bullets

2️⃣ "mnemonic"
• MUST be an ARRAY of STRINGS
• 3–6 mnemonics only
• Indian-context friendly + commonly used USMLE-style recall patterns
• Short, punchy, exam-recall focused
• May include Hinglish
• Use **bold / italic** and Unicode symbols
• NO explanations longer than 1–2 lines

3️⃣ "tables"
• MUST be an ARRAY of OBJECTS
• EACH object MUST contain EXACTLY:
  {
    "title": "string",
    "markdown": "string"
  }

────────────────────────────────
SYNOPTIC TABLE FORMAT (NON-NEGOTIABLE)
────────────────────────────────

Each table MUST follow this EXACT markdown pattern:

| Column A | Column B | Column C |
|---|---:|---|
| Data | Data | Data |

STRICT RULES:
• Pure markdown table ONLY
• Header row + separator row mandatory
• ❌ No blank lines inside table
• ❌ No bullet points inside cells
• ❌ No line breaks inside cells
• 2–4 columns ONLY
• Inline **bold / italic** allowed
• Every table MUST help MCQ elimination

────────────────────────────────
RECOMMENDED TABLE INTENTS (SUBJECT-AWARE)
────────────────────────────────

For **CLINICAL / PARA-CLINICAL SUBJECTS**
(e.g. Medicine, Surgery, Pediatrics, OBG, Pathology, Pharmacology):

• Differentiation  
• Classification / staging  
• Risk → consequence  
• Investigation → next step  
• Option elimination logic  

For **FORENSIC MEDICINE (FM)**:

• Injury type → weapon inference  
• Time since death → postmortem change  
• Legal section → punishment / implication  
• Poison → mechanism → antidote  
• Cause of death → manner of death  
• Option elimination logic  

For **COMMUNITY MEDICINE (PSM)**:

• Indicator → definition → formula  
• Program → target group → intervention  
• Agent → host → environment  
• Risk factor → disease burden  
• Screening test → validity metric  
• Option elimination logic  

⚠️ Table intents are GUIDANCE, not additional output keys.
⚠️ NEVER write the intent name in the output.

────────────────────────────────
CONTENT RULES (HARD)
────────────────────────────────

• Focus ONLY on NEET-PG / INI-CET repeatedly tested facts
• Maintain USMLE-grade clarity but NEVER mention:
  USMLE World, First Aid, Amboss, NBME, Marrow (❌ forbidden words)
• Do NOT invent new diseases, laws, programs, or statistics
• Do NOT change schema between runs
• Do NOT add narrative explanations

────────────────────────────────
DETERMINISM & STABILITY RULES
────────────────────────────────

• Same input → SAME schema every time
• No creative restructuring
• No variable key counts
• If unsure, SIMPLIFY — never expand schema

────────────────────────────────
FAIL-SAFE INSTRUCTION
────────────────────────────────

If you are about to add ANY key other than:
"concept", "mnemonic", "tables"

→ STOP and REMOVE it.

OUTPUT ONLY THE JSON.
NO commentary.
NO markdown outside JSON.

INPUT:
${mcqJsonText}
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
  if (typeof prompt !== "string" || !prompt.trim()) {
    throw new Error("❌ Invalid prompt");
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
// SAFE JSON PARSE (OBJECT)
// ─────────────────────────────────────────────
function safeParseJson(raw) {
  const cleaned = raw
    .trim()
    .replace(/^```json/i, "")
    .replace(/^```/, "")
    .replace(/```$/, "");

  const match = cleaned.match(/\{[\s\S]*\}/);
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
    .from("mcq_reconstruction_queue")
    .update({
      updated_mcq_json_lock: null,
      updated_mcq_json_lock_at: null
    })
    .lt("updated_mcq_json_lock_at", cutoff);

  // Fetch eligible rows
  const { data, error } = await supabase
    .from("mcq_reconstruction_queue")
    .select("id, updated_mcq_json")
    .not("updated_mcq_json", "is", null)
    .is("updated_concept_json", null)
    .is("updated_mcq_json_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  const { data: locked, error: err2 } = await supabase
    .from("mcq_reconstruction_queue")
    .update({
      updated_mcq_json_lock: WORKER_ID,
      updated_mcq_json_lock_at: new Date().toISOString(),
      status: "processing"
    })
    .in("id", ids)
    .is("updated_mcq_json_lock", null)
    .select("id, updated_mcq_json");

  if (err2) throw err2;
  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const mcqText = JSON.stringify(row.updated_mcq_json, null, 2);

  let raw = await callOpenAI(buildPrompt(mcqText));
  let parsed;

  try {
    parsed = safeParseJson(raw);
  } catch {
    raw = await callOpenAI(buildPrompt(mcqText));
    parsed = safeParseJson(raw);
  }

  await supabase
    .from("mcq_reconstruction_queue")
    .update({
      updated_concept_json: parsed,
      updated_mcq_json_lock: null,
      updated_mcq_json_lock_at: null,
      status: "completed",
      updated_at: new Date().toISOString()
    })
    .eq("id", row.id);

  return true;
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 MCQ → HYF+MNEMONIC WORKER STARTED | ${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows`);

      for (let i = 0; i < claimed.length; i += BATCH_SIZE) {
        const batch = claimed.slice(i, i + BATCH_SIZE);

        const results = await Promise.allSettled(
          batch.map(processRow)
        );

        results.forEach((res, idx) => {
          if (res.status === "fulfilled") {
            console.log("   ✅ concept+mnemonic generated");
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
