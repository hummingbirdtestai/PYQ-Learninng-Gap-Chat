require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.SSML_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.SSML_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.SSML_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.SSML_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.SSML_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `ssml-mcq-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ─────────────────────────────────────────────
// SSML PROMPT (USED AS-IS)
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
      await sleep(500 * attempt);
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
    .from("concept_phase_final")
    .update({ ssml_lock: null, ssml_lock_at: null })
    .lt("ssml_lock_at", cutoff);

  const { data: rows, error } = await supabase
    .from("concept_phase_final")
    .select("id, phase_json")
    .eq("phase_type", "mcq")
    .is("ssml_script", null)
    .is("ssml_lock", null)
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  await supabase
    .from("concept_phase_final")
    .update({
      ssml_lock: WORKER_ID,
      ssml_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("ssml_lock", null);

  return rows;
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
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
      ssml_lock: null,
      ssml_lock_at: null
    })
    .eq("id", row.id);

  return true;
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🎙️ SSML MCQ Worker Started | ${WORKER_ID}`);

  while (true) {
    try {
      const rows = await claimRows(LIMIT);
      if (!rows.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);
        const results = await Promise.allSettled(batch.map(processRow));

        results.forEach((r, idx) => {
          if (r.status === "fulfilled") {
            console.log(`✅ SSML generated for row ${batch[idx].id}`);
          } else {
            console.error(`❌ Failed row ${batch[idx].id}`, r.reason);
          }
        });
      }

    } catch (e) {
      console.error("Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
