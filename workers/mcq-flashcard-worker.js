require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

//──────────────────────────────────────────────
// SETTINGS
//──────────────────────────────────────────────
const MODEL        = process.env.FLASHCARD_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.FLASHCARD_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.FLASHCARD_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.FLASHCARD_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.FLASHCARD_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `mcq-flash-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

//──────────────────────────────────────────────
// PROMPT GENERATOR
//──────────────────────────────────────────────
function buildPrompt(mcq) {
  return `
Pick Input Column - mcq
Prompt Use As Is -
Convert the MCQ into a NEET-PG flashcard.  
Question: Active-recall, positive phrasing  
Answer: 2–3 words only, crisp and high-yield  

Remove options & MCQ wording.  
For “EXCEPT/NOT” MCQs → convert to a positive fact based on the correct option.  
For “None/All of the above” → ignore those options; extract the high-yield fact.  

Whenever MCQ HAS Clinical case vignette → Create Question as clinical vignette only.  
Whenever clinical vignette is incomplete → make it complete like UWorld/NBME/Amboss style.  

Use Unicode (↑ ↓ → ± ≥ ≤).  
Highlight key terms with **bold** and _italic_.  

Output only JSON: {"Question":"…","Answer":"…"}  

MCQ INPUT:
${mcq}
`.trim();
}

//──────────────────────────────────────────────
// HELPERS
//──────────────────────────────────────────────
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

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
  } catch (err) {
    if (isRetryable(err) && attempt <= 3) {
      await sleep(300 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw err;
  }
}

function safeParseObject(raw) {
  const cleaned = raw
    .trim()
    .replace(/^```json/i, "")
    .replace(/^```/, "")
    .replace(/```$/, "")
    .replace(/,\s*}/g, "}")
    .replace(/,\s*]/g, "]");

  try {
    return JSON.parse(cleaned);
  } catch (err) {
    console.error("❌ JSON Parse Error:", cleaned.slice(0, 150));
    throw err;
  }
}

//──────────────────────────────────────────────
// LOCKING SYSTEM FOR mcq_bank
//──────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // Release stale locks
  await supabase
    .from("mcq_bank")
    .update({ lg_flashcard_lock: null, lg_flashcard_locked_at: null })
    .lt("lg_flashcard_locked_at", cutoff);

  // Get rows needing flashcards
  const { data: rows, error } = await supabase
    .from("mcq_bank")
    .select("id, mcq, correct_answer")
    .is("flash_card_manu", null)
    .is("lg_flashcard_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // Lock them
  const { data: locked, error: err2 } = await supabase
    .from("mcq_bank")
    .update({
      lg_flashcard_lock: WORKER_ID,
      lg_flashcard_locked_at: new Date().toISOString(),
    })
    .in("id", ids)
    .select("id, mcq, correct_answer");

  if (err2) throw err2;

  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;

  await supabase
    .from("mcq_bank")
    .update({ lg_flashcard_lock: null, lg_flashcard_locked_at: null })
    .in("id", ids);
}

//──────────────────────────────────────────────
// PROCESS EACH MCQ ROW
//──────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.mcq);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const parsed = safeParseObject(raw);

  await supabase
    .from("mcq_bank")
    .update({
      flash_card_manu: parsed,
      lg_flashcard_lock: null,
      lg_flashcard_locked_at: null
    })
    .eq("id", row.id);

  return { updated: 1 };
}

//──────────────────────────────────────────────
// MAIN LOOP
//──────────────────────────────────────────────
(async function main() {
  console.log(`🟦 MCQ Flashcard Worker Started | model=${MODEL} | worker=${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} MCQ rows`);

      const results = await Promise.allSettled(
        claimed.map(row => processRow(row))
      );

      let success = 0;

      results.forEach((r, i) => {
        if (r.status === "fulfilled") {
          console.log(`   ✅ Processed MCQ #${i + 1}`);
          success += r.value.updated;
        } else {
          console.error(`   ❌ Error in MCQ #${i + 1}:`, r.reason);
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
