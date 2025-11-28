require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.MCQ_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.MCQ_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `mcq-mbbs-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ─────────────────────────────────────────────
// MCQ PROMPT BUILDER
// ─────────────────────────────────────────────
function buildPrompt(topic) {
return `
You are an expert NEET-PG and USMLE question writer with 30 years of experience creating official-standard, high-yield MCQs (NEET-PG / USMLE / NBME level = moderate-hard).
🧩 FORMATTING RULES (apply globally to the entire output):
• Use Markup **bold**, _italic_, Unicode arrows (→ ↑ ↓), subscripts/superscripts (₁₂³⁺⁻), Greek (α β Δ μ), and minimal emojis (✅ ❌ 💡) wherever relevant.
• "stem": Real NEET-PG–style question (clinical case vignette style or Single Liner High Yield fact testing style MCQs depending on the topic).
• "learning_gap": "💡 One-line high-yield takeaway".
• "feedback": "wrong": "❌ Brief rationale" "correct": "✅ Concise reasoning"
• Maintain NEET-PG moderate-to-hard difficulty.
• No “EXCEPT” questions.
• Output **only VALID JSON**.
• Output **exactly 5 MCQs**.
• Each MCQ must follow the **single template below**.
• GPT must output **an array of 5 objects using this template**.
JSON TEMPLATE (for ONE MCQ — produce 5 using this):
{
  "stem": "",
  "options": {
    "A": "",
    "B": "",
    "C": "",
    "D": ""
  },
  "feedback": {
    "wrong": "❌ Brief rationale",
    "correct": "✅ Concise reasoning"
  },
  "learning_gap": "💡 ",
  "correct_answer": ""
}
Your output must be:
[
  { five MCQs here, each object following the template above }
]

TOPIC:
${topic}
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

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages
    });

    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(500 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw e;
  }
}

// ─────────────────────────────────────────────
// JSON REPAIR HELPER
// ─────────────────────────────────────────────
function repairJson(bad) {
  let txt = bad.trim();

  // Remove code fences
  txt = txt.replace(/^```json/i, "").replace(/^```/, "").replace(/```$/, "");

  // Force extract outer JSON array
  const jsonMatch = txt.match(/\[[\s\S]*\]/);
  if (!jsonMatch) return null;
  txt = jsonMatch[0];

  // Common model truncation issues fix
  txt = txt
    .replace(/“/g, '"')
    .replace(/”/g, '"')
    .replace(/‘/g, "'")
    .replace(/’/g, "'");

  // Remove trailing commas:  { ... , }
  txt = txt.replace(/,\s*}/g, "}");
  txt = txt.replace(/,\s*\]/g, "]");

  // Ensure array ends with ]
  if (!txt.trim().endsWith("]")) txt = txt.trim() + "]";

  // Ensure objects end correctly
  const openBraces = (txt.match(/{/g) || []).length;
  const closeBraces = (txt.match(/}/g) || []).length;
  if (openBraces > closeBraces) {
    txt += "}".repeat(openBraces - closeBraces);
  }

  // Ensure array bracket count matches
  const openBrackets = (txt.match(/\[/g) || []).length;
  const closeBrackets = (txt.match(/]/g) || []).length;
  if (openBrackets > closeBrackets) {
    txt += "]".repeat(openBrackets - closeBrackets);
  }

  try {
    return JSON.parse(txt);
  } catch (e) {
    return null;
  }
}

// ─────────────────────────────────────────────
// SAFE PARSE WITH AUTO FIX
// ─────────────────────────────────────────────
function safeParse(raw) {
  const cleaned = raw
    .trim()
    .replace(/^```json/i, "")
    .replace(/^```/, "")
    .replace(/```$/, "");

  const jsonMatch = cleaned.match(/\[[\s\S]*\]/);
  if (!jsonMatch) throw new Error("❌ No valid JSON array found.");

  const jsonOnly = jsonMatch[0];

  try {
    return JSON.parse(jsonOnly);
  } catch (err) {
    console.error("❌ Initial JSON Parse Failed. Attempting repair...");
    
    const repaired = repairJson(cleaned);
    if (repaired) {
      console.log("🔧 JSON Repair Successful");
      return repaired;
    }

    console.error("❌ JSON Repair Failed");
    throw err;
  }
}


// ─────────────────────────────────────────────
// CLAIM ROWS
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  await supabase
    .from("subject_curriculum")
    .update({ concept_lock: null, concept_lock_at: null })
    .lt("concept_lock_at", cutoff);

  const { data: rows, error: err1 } = await supabase
    .from("subject_curriculum")
    .select("id, subject, chapter, topic, chapter_id, topic_id")
    .not("topic", "is", null)
    .not("topic", "eq", "")
    .is("concept_lock", null)
    .order("id", { ascending: true })
    .limit(limit);

  if (err1) throw err1;
  if (!rows || rows.length === 0) return [];

  const ids = rows.map(r => r.id);

  const { data: locked, error: err2 } = await supabase
    .from("subject_curriculum")
    .update({
      concept_lock: WORKER_ID,
      concept_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("concept_lock", null)
    .select("id, subject, chapter, topic, chapter_id, topic_id");

  if (err2) throw err2;

  return locked || [];
}

// ─────────────────────────────────────────────
// CLEAR LOCKS
// ─────────────────────────────────────────────
async function clearLocks(ids) {
  if (!ids.length) return;

  await supabase
    .from("subject_curriculum")
    .update({ concept_lock: null, concept_lock_at: null })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.topic);

  // First attempt
  let raw = await callOpenAI([{ role: "user", content: prompt }]);

  let parsed;
  try {
    parsed = safeParse(raw);
  } catch (err) {
    console.log("🔁 Retrying MCQ generation once for row:", row.id);

    // Retry once
    raw = await callOpenAI([{ role: "user", content: prompt }]);

    try {
      parsed = safeParse(raw);
    } catch (err2) {
      console.error("❌ FINAL FAILURE after retry. Skipping row:", row.id);
      await clearLocks([row.id]);
      throw err2;
    }
  }

  // Save successfully parsed MCQs
  await supabase
    .from("subject_curriculum")
    .update({ practice_mcq: parsed })
    .eq("id", row.id);

  await clearLocks([row.id]);

  return { updated: 1 };
}


// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 MCQ Worker Started | worker=${WORKER_ID} | model=${MODEL}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows`);

      // Run in batches
      for (let i = 0; i < claimed.length; i += BATCH_SIZE) {
        const batch = claimed.slice(i, i + BATCH_SIZE);

        const results = await Promise.allSettled(
          batch.map(r => processRow(r))
        );

        results.forEach((res, idx) => {
          if (res.status === "fulfilled") {
            console.log(`   ✅ Row ${i + idx + 1} processed`);
          } else {
            console.error(`   ❌ Row ${i + idx + 1} failed:`, res.reason);
            clearLocks([batch[idx].id]);
          }
        });
      }

      console.log(`🔁 Loop complete`);

    } catch (e) {
      console.error("Loop error:", e);
      await sleep(1000);
    }
  }
})();
