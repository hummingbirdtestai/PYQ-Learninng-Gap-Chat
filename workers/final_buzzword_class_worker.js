require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS (FINAL BUZZWORD CLASS WORKER)
// ─────────────────────────────────────────────
const MODEL        = process.env.CLASS_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CLASS_LIMIT || "150", 10);
const BATCH_SIZE   = parseInt(process.env.CLASS_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.CLASS_LOOP_SLEEP_MS || "200", 10);
const LOCK_TTL_MIN = parseInt(process.env.CLASS_LOCK_TTL_MIN || "10", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `final-buzzword-class-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — UNCHANGED)
// ─────────────────────────────────────────────
function buildPrompt(conceptJson) {
  return `
You are a medical exam–content generation engine.

Your task is to generate a STRICTLY VALID JSON object for ONE medical topic.

This is an EXAM-FIRST, DATA-STRUCTURE–AWARE task designed for USMLE / NBME / AMBOSS / UWorld / NEET-PG–level testing.

⛔ OUTPUT RULES (STRICT — ZERO TOLERANCE)

1️⃣ Output format
- Output ONLY a single JSON object
- ❌ No markdown outside JSON
- ❌ No explanations
- ❌ No commentary
- ❌ No trailing text

2️⃣ Concept structure
The JSON MUST contain EXACTLY 10 concept objects, named ONLY:

"concept_1" through "concept_10"

3️⃣ REQUIRED KEYS PER CONCEPT (EXACT ORDER — DO NOT CHANGE)

Each concept object MUST contain EXACTLY the following keys IN THIS ORDER:

1. "concept" → integer 1–10 only
2. "title" → clean descriptive string (e.g., "Diagnostic Pitfalls")
3. "image" → object (see Image Rules below)
4. "high_yield_buzzwords" → array of EXACTLY 3 items
5. "exam_traps" → array of EXACTLY 3 objects
6. "mcqs" → array containing EXACTLY 1 MCQ object

❌ DO NOT add or remove keys
❌ DO NOT rename keys
❌ DO NOT add wrapper objects
❌ DO NOT add metadata

🖼️ IMAGE RULES (STRICT)

Each concept MUST include an "image" object with EXACTLY 2 keys:

"image": {
  "image_url": "string placeholder only",
  "image_description": "description"
}

Image requirements:
- image_url → placeholder string only (no real URLs)
- image_description →
  - Identify ONE image frequently tested in
    NEET-PG / USMLE image-based MCQs
  - Must be directly derived from that concept
  - Examples:
    - Barium swallow showing postcricoid web
    - Endoscopic view of esophageal SCC
    - Clinical photograph of koilonychia
    - Radiologic sign classically tested

❌ Do NOT mention multiple images
❌ Do NOT add captions outside this object

🧠 HIGH-YIELD BUZZWORD RULES (VERY STRICT)

"high_yield_buzzwords" MUST contain EXACTLY 3 bullet-style sentences

Each sentence MUST:
- Be ≤ 10 words
- Be First Aid / AMBOSS / UWorld tone
- Use ALL of the following where relevant:
  - Markdown bold and italic
  - Unicode arrows → ↑ ↓
  - Subscripts / superscripts
  - Medical symbols, equations
  - Emojis (sparingly)

⚠️ EXAM TRAPS RULES

Each object inside "exam_traps" MUST contain EXACTLY:

- "trap" → common exam confusion or distractor
- "answer" → correct clarification (minimum 8 words)
- "memory_hook" → short recall aid (emoji allowed)

Each concept MUST contain EXACTLY 3 exam traps

❌ Flat keys like exam_trap_1, answer_1 are FORBIDDEN

📝 MCQ RULES (CRITICAL)

Each concept’s "mcqs" array MUST contain EXACTLY ONE MCQ object.

Each MCQ object MUST contain EXACTLY the following keys IN THIS ORDER:

1. "stem"
   - USMLE-style clinical case vignette
   - Must clearly imply History → Examination → Investigation
   - Paragraph style (not bullets)

2. "options"
   - Object with EXACTLY 4 keys:
     "A", "B", "C", "D"

3. "correct_answer"
   - MUST be exactly "A", "B", "C", or "D"

4. "hyfs"
   - Array of EXACTLY 3 High-Yield Facts
   - Each HYF:
     - ≤ 12 words
     - First Aid / UWorld quality
     - Uses bold, italics, arrows, symbols
     - Represents a key examiner decision point

📌 Each MCQ MUST be strictly derived from that concept only
📌 NO duplicated MCQs across concepts

🚫 FORBIDDEN CONTENT

❌ No keys like "key", "prognosis", "dominant factor"
❌ No "Concept_9_*" naming patterns
❌ No explanations outside JSON
❌ No markdown wrappers
❌ No empty "mcqs" arrays

✅ FINAL VALIDATION CHECK (MANDATORY)

Before outputting, verify internally:

✔ Exactly 10 concepts
✔ Each concept contains:
- concept number
- title
- image object (2 keys only)
- 3 buzzwords
- 3 exam traps
- 1 MCQ

✔ No extra keys
✔ JSON is directly insertable into Supabase JSONB

🔒 OUTPUT THE JSON NOW.

INPUT:
${JSON.stringify(conceptJson, null, 2)}
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
// SAFE JSON PARSER
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
// CLAIM ROWS (all_subjects_raw)
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // 1️⃣ Clear expired locks
  await supabase
    .from("all_subjects_raw")
    .update({ concept_lock: null, concept_lock_at: null })
    .lt("concept_lock_at", cutoff);

  // 2️⃣ Fetch eligible rows
  const { data, error } = await supabase
    .from("all_subjects_raw")
    .select("id, concept_json")
    .not("concept_json", "is", null)
    .is("final_buzzword_class_json", null)
    .is("concept_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  // 3️⃣ Lock rows
  const { data: locked, error: err2 } = await supabase
    .from("all_subjects_raw")
    .update({
      concept_lock: WORKER_ID,
      concept_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("concept_lock", null)
    .select("id, concept_json");

  if (err2) throw err2;
  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  let raw = await callOpenAI(buildPrompt(row.concept_json));
  let parsed;

  try {
    parsed = safeParseJson(raw);
  } catch {
    raw = await callOpenAI(buildPrompt(row.concept_json));
    parsed = safeParseJson(raw);
  }

  await supabase
    .from("all_subjects_raw")
    .update({
      final_buzzword_class_json: parsed,
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
  console.log(`🧠 FINAL BUZZWORD CLASS WORKER STARTED | ${WORKER_ID}`);

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
          if (res.status !== "fulfilled") {
            console.error(`❌ Failed row ${batch[idx].id}`, res.reason);
          }
        });
      }
    } catch (e) {
      console.error("❌ Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
