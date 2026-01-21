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
  `concept-json-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — UNCHANGED)
// ─────────────────────────────────────────────
function buildPrompt(conceptMarkdown) {
  return `
Create this Content Covered as a JSON of 10 Concepts numbered Concept 1 - Concept 10.

Each object must have exactly these keys:
- "concept" : number (1–10)
- "title"
- "core_idea"
- "key_explanation"

Rules:
• Each **core_idea** must be a concise high-yield statement.
• Each **key_explanation** must be a **paragraph-length explanation**, as a teacher explains in a live classroom.
• Ensure **all essential ideas** from the content are covered across the 10 concepts.
• Do NOT skip concepts.
• Do NOT add extra keys.

Formatting rules INSIDE values:
• Explicitly **bold and italicize** all important key words, clinical terms, diseases, signs, investigations, and headings.
• Use arrows (→ ↑ ↓), subscripts/superscripts (₁ ₂ ³ ⁺ ⁻), Greek letters (α β γ), and emojis (💡🫀🫁🧠⚕📘) naturally.

OUTPUT RULES (VERY STRICT):
• Output ONLY valid JSON
• Output ONLY an array of 10 objects
• No Markdown
• No explanations
• No trailing commas

CONTENT:
${conceptMarkdown}
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

  const match = cleaned.match(/\[[\s\S]*\]/);
  if (!match) throw new Error("❌ No JSON array found");

  return JSON.parse(match[0]);
}

// ─────────────────────────────────────────────
// CLAIM ROWS (concept_json IS NULL)
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // 1️⃣ Clear expired locks
  await supabase
    .from("all_subjects_raw")
    .update({ concept_lock: null, concept_lock_at: null })
    .lt("concept_lock_at", cutoff);

  // 2️⃣ Fetch eligible rows
  const { data: rows, error } = await supabase
    .from("all_subjects_raw")
    .select("id, concept")
    .is("concept_json", null)
    .not("concept", "is", null)
    .is("concept_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // 3️⃣ Lock rows
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
// CLEAR LOCKS (on failure)
// ─────────────────────────────────────────────
async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("all_subjects_raw")
    .update({ concept_lock: null, concept_lock_at: null })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS SINGLE ROW
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

  if (!Array.isArray(parsed) || parsed.length !== 10) {
    throw new Error("❌ Output must be an array of exactly 10 concepts");
  }

  await supabase
    .from("all_subjects_raw")
    .update({
      concept_json: parsed,
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
  console.log(`🧠 ALL SUBJECTS CONCEPT JSON WORKER STARTED | ${WORKER_ID}`);

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
            console.log("   ✅ concept_json generated");
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
