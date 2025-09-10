// workers/zoologyUnicodeWorkerFlashCard.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ---------- Settings ----------
const MODEL        = process.env.ZOOLOGY_FLASHCARDS_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.ZOOLOGY_FLASHCARDS_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.ZOOLOGY_FLASHCARDS_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.ZOOLOGY_FLASHCARDS_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.ZOOLOGY_FLASHCARDS_LOCK_TTL_MIN || "15", 10);
const SUBJECT      = "Zoology"; // ✅ hardcoded
const WORKER_ID    = process.env.WORKER_ID || `zoo-flashcards-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(conceptJson) {
  return `
You are a 40-year experienced NEET Zoology teacher.
From the Concept JSON below, create **10 unique high-yield flashcards** for active recall.

Rules:
- Output = JSON array of 10 objects → { "uuid": "UUID", "Question": "", "Answer": "" }
- Q: concise, NEET-standard.
- A: 1–2 words/phrases/numbers only.
- Use **Markdown** for Bold/Italic; Unicode for subscripts/superscripts (H₂O, Na⁺, Fe³⁺, 37°C).
- Mix direct fact + assertion–reason style.
- No MCQs. No extra text.

Concept JSON:
${JSON.stringify(conceptJson)}
`.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function isRetryable(e) {
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(String(e?.message || e));
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "json_object" }
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

function safeParseJson(raw) {
  const cleaned = raw.trim().replace(/^```json\s*/i, "").replace(/```$/i, "");
  return JSON.parse(cleaned);
}

// ---------- Locks ----------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();
  await supabase
    .from("concepts_vertical")
    .update({ flash_cards_lock: null, flash_cards_lock_at: null })
    .is("flash_cards_unicode", null)
    .lt("flash_cards_lock_at", cutoff)
    .eq("subject_name", SUBJECT);
}

async function claimRows(limit) {
  await freeStaleLocks();
  const { data, error } = await supabase
    .from("concepts_vertical")
    .select("vertical_id, concept_json_unicode")
    .is("flash_cards_unicode", null)
    .is("flash_cards_lock", null)
    .eq("subject_name", SUBJECT)
    .limit(limit);
  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.vertical_id);
  const { data: locked, error: e2 } = await supabase
    .from("concepts_vertical")
    .update({ flash_cards_lock: WORKER_ID, flash_cards_lock_at: new Date().toISOString() })
    .in("vertical_id", ids)
    .is("flash_cards_unicode", null)
    .is("flash_cards_lock", null)
    .select("vertical_id, concept_json_unicode");
  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("concepts_vertical")
    .update({ flash_cards_lock: null, flash_cards_lock_at: null })
    .in("vertical_id", ids);
}

// ---------- Process ----------
async function processRow(row) {
  const prompt = buildPrompt(row.concept_json_unicode);
  const raw = await callOpenAI(prompt);
  let jsonOut = safeParseJson(raw);

  // Ensure UUIDs
  jsonOut = jsonOut.map(card => ({
    uuid: card.uuid || uuidv4(),
    Question: card.Question,
    Answer: card.Answer
  }));

  const { error } = await supabase
    .from("concepts_vertical")
    .update({
      flash_cards_unicode: jsonOut,
      flash_cards_lock: null,
      flash_cards_lock_at: null
    })
    .eq("vertical_id", row.vertical_id);

  if (error) throw new Error(`Update failed v_id=${row.vertical_id}: ${error.message}`);
  return { updated: 1 };
}

async function processBatch(rows) {
  const chunks = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE) chunks.push(rows.slice(i, i + BATCH_SIZE));

  let updated = 0;
  for (const chunk of chunks) {
    const results = await Promise.allSettled(chunk.map(processRow));
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (r.status === "fulfilled") updated += r.value.updated;
      else {
        console.error("❌ row error:", r.reason?.message);
        await clearLocks([chunk[i].vertical_id]);
      }
    }
  }
  return updated;
}

// ---------- Main ----------
(async function main() {
  console.log(`🧵 Zoology FlashCards Worker ${WORKER_ID} | model=${MODEL}`);
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }
      console.log(`⚙️ claimed=${claimed.length}`);
      const updated = await processBatch(claimed);
      console.log(`✅ updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
