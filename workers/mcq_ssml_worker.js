require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS (HIGH THROUGHPUT)
// ─────────────────────────────────────────────
const MODEL          = process.env.HY_MODEL || "gpt-5-mini";
const LIMIT          = parseInt(process.env.HY_LIMIT || "150", 10);
const CONCURRENCY    = parseInt(process.env.HY_CONCURRENCY || "10", 10);
const SLEEP_MS       = parseInt(process.env.HY_LOOP_SLEEP_MS || "200", 10);
const LOCK_TTL_MIN   = parseInt(process.env.HY_LOCK_TTL_MIN || "10", 10);
const WORKER_ID      = process.env.WORKER_ID || `mocktest-concept-worker-${process.pid}`;

// ─────────────────────────────────────────────
// SSML PROMPT (UNCHANGED)
// ─────────────────────────────────────────────
function buildPrompt(mcqText) {
return `
You are an expert NEET-PG medical education audio-script writer.

Your task is to CONVERT the given MCQ discussion into a **Google Text-to-Speech compatible SSML script** for a **~3-minute audio conversation** between **4 NEET-PG study friends**, AND then conclude with a **Top 5 High-Yield NEET-PG Facts** rapid revision delivered by the same friends.

====================
🎯 GOAL
====================
Create a **natural, real-life group study discussion** (hostel / library vibe) that:
• Sounds conversational, not scripted
• Covers ALL exam-relevant concepts tested by the MCQ
• Explains why the correct option is correct
• Explains why tempting wrong options are wrong
• Trains NEET-PG exam logic and traps
• Ends with a **Top 5 High-Yield NEET-PG Facts** section

====================
👥 CHARACTERS (FIXED)
====================
Use EXACTLY these 4 speakers:
1. Aarav (male)
2. Meera (female)
3. Rohit (male)
4. Kavya (female)

====================
🕒 DURATION
====================
• Target total audio length: ~3 minutes
• Main discussion: ~2–2.2 minutes
• High-Yield Facts section: ~40–50 seconds
• Natural speaking pace (150–160 words/min)

====================
🎙️ VOICE RULES (CRITICAL)
====================
• Use ONLY Google-TTS compatible SSML
• Wrap everything inside <speak>...</speak>
• Use ONLY <voice> and <break> tags
• Assign voices strictly as follows:
  - Aarav → en-IN-Neural2-A
  - Meera → en-IN-Neural2-B
  - Rohit → en-IN-Neural2-C
  - Kavya → en-IN-Neural2-D
• Use <break time="200ms"/> or <break time="400ms"/> only
• DO NOT use unsupported SSML tags

====================
🚫 STRICT OUTPUT RULES
====================
• OUTPUT ONLY VALID SSML
• NO markdown
• NO explanations
• NO emojis
• NO text outside <speak>...</speak>

====================
📌 INPUT CONTENT
====================
${mcqText}
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
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

// ─────────────────────────────────────────────
// CLAIM ROWS USING mentor_lock
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // Clear expired locks
  await supabase
    .from("concept_phase_final")
    .update({ mentor_lock: null, mentor_lock_at: null })
    .lt("mentor_lock_at", cutoff);

  // Fetch rows
  const { data: rows, error } = await supabase
    .from("concept_phase_final")
    .select("id, phase_json")
    .eq("phase_type", "mcq")
    .is("ssml_script", null)
    .is("mentor_lock", null)
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // Lock rows
  await supabase
    .from("concept_phase_final")
    .update({
      mentor_lock: WORKER_ID,
      mentor_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("mentor_lock", null);

  return rows;
}

// ─────────────────────────────────────────────
// PROCESS SINGLE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(
    typeof row.phase_json === "string"
      ? row.phase_json
      : JSON.stringify(row.phase_json, null, 2)
  );

  const ssml = await callOpenAI(prompt);

  if (!ssml.trim().startsWith("<speak>")) {
    throw new Error("Invalid SSML output");
  }

  await supabase
    .from("concept_phase_final")
    .update({
      ssml_script: ssml,
      mentor_lock: null,
      mentor_lock_at: null
    })
    .eq("id", row.id);

  return true;
}

// ─────────────────────────────────────────────
// MAIN LOOP (TRUE CONCURRENCY)
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🎙️ SSML Worker Started | ${WORKER_ID}`);

  while (true) {
    try {
      const rows = await claimRows(LIMIT);

      if (!rows.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      for (let i = 0; i < rows.length; i += CONCURRENCY) {
        const slice = rows.slice(i, i + CONCURRENCY);

        const results = await Promise.allSettled(
          slice.map(processRow)
        );

        results.forEach((r, idx) => {
          const rowId = slice[idx].id;
          if (r.status === "fulfilled") {
            console.log(`✅ SSML generated for row ${rowId}`);
          } else {
            console.error(`❌ Failed row ${rowId}`, r.reason);
          }
        });
      }

    } catch (e) {
      console.error("Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
