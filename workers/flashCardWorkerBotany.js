// workers/flashCardWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ---------- Settings ----------
const MODEL        = process.env.FLASHCARD_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.FLASHCARD_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.FLASHCARD_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.FLASHCARD_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.FLASHCARD_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `flashcards-botany-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt Builder ----------
function buildPrompt(conceptJson) {
  const compact = JSON.stringify(conceptJson);

  return `
You are a 40-year experienced NEET Botany teacher.  

From the Concept + Explanation below, create **10 unique high-yield NEET flashcards** for **active recall & spaced repetition**.  

Rules:  
- Output = JSON array of 10 objects → { "Question": "", "Answer": "" }  
- Q: concise, NEET-standard (NCERT XI/XII).  
- A: very short (1–2 words/phrases/numbers).  
- Use **Markdown** for emphasis; **KaTeX/LaTeX ($…$)** for formulas, subscripts, superscripts, angles.  
- Each card must test a **different fact** (no repetition).  
- Mix **direct fact** and **assertion-reason style**.  
- No MCQs.  

INPUT Concept JSON:
${compact}
`.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(s);
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages,
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw e;
  }
}

function safeParseArray(raw) {
  const cleaned = raw
    .trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```/i, "")
    .replace(/```$/i, "")
    .replace(/,\s*}/g, "}")
    .replace(/,\s*]/g, "]");

  try {
    const arr = JSON.parse(cleaned);
    return Array.isArray(arr) ? arr : [arr];
  } catch (e) {
    console.error("❌ JSON parse error. Raw snippet:", cleaned.slice(0, 200));
    throw e;
  }
}

// ---------- Locking ----------
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // free stale locks (only Botany)
  await supabase
    .from("concepts_vertical")
    .update({ flash_card_lock: null, flash_card_lock_at: null })
    .is("flash_cards", null)
    .eq("subject_name", "Botany")
    .lt("flash_card_lock_at", cutoff);

  // fetch candidates (only Botany)
  const { data: candidates, error: e1 } = await supabase
    .from("concepts_vertical")
    .select("vertical_id, concept_exp")
    .not("concept_exp", "is", null)
    .is("flash_cards", null)
    .is("flash_card_lock", null)
    .eq("subject_name", "Botany")
    .order("vertical_id", { ascending: true })
    .limit(limit);

  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.vertical_id);

  // lock them (only Botany)
  const { data: locked, error: e2 } = await supabase
    .from("concepts_vertical")
    .update({
      flash_card_lock: WORKER_ID,
      flash_card_lock_at: new Date().toISOString()
    })
    .in("vertical_id", ids)
    .is("flash_cards", null)
    .is("flash_card_lock", null)
    .eq("subject_name", "Botany")
    .select("vertical_id, concept_exp");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("concepts_vertical")
    .update({ flash_card_lock: null, flash_card_lock_at: null })
    .in("vertical_id", ids);
}

// ---------- Process one row ----------
async function processRow(row) {
  const prompt = buildPrompt(row.concept_exp);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const cards = safeParseArray(raw);

  // assign UUIDs
  cards.forEach(card => {
    if (!card.uuid) card.uuid = uuidv4();
  });

  await supabase
    .from("concepts_vertical")
    .update({ flash_cards: cards })
    .eq("vertical_id", row.vertical_id);

  await clearLocks([row.vertical_id]);
  return { updated: 1, total: 1 };
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧵 FlashCard Worker (Botany) ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);

      const results = await Promise.allSettled(claimed.map(row => processRow(row)));

      let updated = 0;
      results.forEach((r, idx) => {
        if (r.status === "fulfilled") {
          console.log(`   row ${idx + 1}: updated ${r.value.updated}/${r.value.total}`);
          updated += r.value.updated;
        } else {
          console.error(`   row ${idx + 1} error:`, r.reason.message || r.reason);
          clearLocks([claimed[idx].vertical_id]);
        }
      });

      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
