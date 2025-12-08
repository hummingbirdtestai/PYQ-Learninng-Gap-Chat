require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

//──────────────────────────────────────────────
// SETTINGS
//──────────────────────────────────────────────
const MODEL        = process.env.HY_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.HY_LIMIT || "30", 10);
const SLEEP_MS     = parseInt(process.env.HY_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.HY_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `concept-hy-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

//──────────────────────────────────────────────
// PROMPT FOR HIGH-YIELD FACTS + TABLE
//──────────────────────────────────────────────
function buildPrompt(phaseJson) {
  return `
You are 30 Years experienced NEETPG INICET Exam Paper Setter 

Give 10 High Yiedl facts most frequently tested based on which MCQs are asked in NEETPG Exam 
Each High Yiedl fact is less than 8 Words 
At the end of High Yield fact List , give a Table of Rapid Revision Reckoner to remember like in Amboss, Usmle world, First Aid, Marrow, PrepLadder kind of Resources 
Be precisely NEETPG INICET Exam Specific 
Use Unicode for Super Scripts , Subscripts , Symbols , Math , emojis , Bullets and MarkUp to High light Bold and Italic of important Key words in High Yiedl facts List and also Table 
• Highlight key words using ONLY GitHub-Flavored Markdown: 
  - Bold = **text** 
  - Italic = *text* 
  - Bold+Italic = ***text*** 
• Do NOT use any underscore formatting (no _text_, __text__, **_text_**). 
Give the entire content as MarkUp Code Block with # and ## used as Section divider for the High Yiedl facts Section and Tabular Section

INPUT CONTENT:
${JSON.stringify(phaseJson)}
`.trim();
}


//──────────────────────────────────────────────
// HELPERS
//──────────────────────────────────────────────
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

function isRetryable(e) {
  const m = String(e?.message || e);
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(m);
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages,
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (err) {
    if (isRetryable(err) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw err;
  }
}

//──────────────────────────────────────────────
// LOCKING SYSTEM — UPDATED FOR concept_phase_final
//──────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // Release old locks
  await supabase
    .from("concept_phase_final")
    .update({ mentor_lock: null, mentor_lock_at: null })
    .lt("mentor_lock_at", cutoff);

  // Pick rows with phase_type = concept AND phase_json_new IS NULL
  const { data: rows, error } = await supabase
    .from("concept_phase_final")
    .select("id, phase_json")
    .eq("phase_type", "concept")
    .is("phase_json_new", null)
    .is("mentor_lock", null)
    .order("mentor_lock_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map((r) => r.id);

  // Lock them
  const { data: locked, error: lockErr } = await supabase
    .from("concept_phase_final")
    .update({
      mentor_lock: WORKER_ID,
      mentor_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .select("id, phase_json");

  if (lockErr) throw lockErr;

  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;

  await supabase
    .from("concept_phase_final")
    .update({
      mentor_lock: null,
      mentor_lock_at: null,
    })
    .in("id", ids);
}

//──────────────────────────────────────────────
// PROCESS ONE ROW
//──────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.phase_json);

  const raw = await callOpenAI([{ role: "user", content: prompt }]);

  // Direct markdown string storage (no JSON parse)
  await supabase
    .from("concept_phase_final")
    .update({
      phase_json_new: raw,
      mentor_lock: null,
      mentor_lock_at: null,
    })
    .eq("id", row.id);

  return { updated: 1 };
}

//──────────────────────────────────────────────
// MAIN LOOP
//──────────────────────────────────────────────
(async function main() {
  console.log(`🟦 Concept High-Yield Worker Started | model=${MODEL} | worker=${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} concept rows`);

      const results = await Promise.allSettled(
        claimed.map((row) => processRow(row))
      );

      let success = 0;

      results.forEach((res, i) => {
        if (res.status === "fulfilled") {
          console.log(`   ✅ Processed Concept #${i + 1}`);
          success += res.value.updated;
        } else {
          console.error(`   ❌ Error in Concept #${i + 1}:`, res.reason);
          clearLocks([claimed[i].id]);
        }
      });

      console.log(`🔁 Batch finished → saved=${success}/${claimed.length}`);
    } catch (err) {
      console.error("❌ Loop Error:", err);
      await sleep(1000);
    }
  }
})();
