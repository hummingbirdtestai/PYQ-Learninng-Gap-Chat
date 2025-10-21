require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ---------- Settings ----------
const MODEL        = process.env.CONCEPT_FORMAT_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CONCEPT_FORMAT_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.CONCEPT_FORMAT_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.CONCEPT_FORMAT_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.CONCEPT_FORMAT_LOCK_TTL_MIN || "15", 10);
const SUBJECT      = process.env.CONCEPT_FORMAT_SUBJECT || "NEET-PG";
const WORKER_ID    = process.env.WORKER_ID || `concept-formatter-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt Builder ----------
function buildPrompt(conceptText) {
  return `
You are a senior NEET-PG mentor with 30 years’ experience.
You are given *raw text* of a Concept summary. 

Reformat it with **bold** and *italic* to highlight key words in *natural Markdown* using **Unicode symbols** (no JSON, no code block) — styled for a WhatsApp-like dark chat bubble.

### Formatting Rules
- Use Markdown headings: #, ##, ### where naturally meaningful.
- Use **bold** and _italic_ text for emphasis.
- Use lists or numbering for clarity.
- Use Unicode arrows (→, ↑, ↓), subscripts/superscripts (₁, ₂, ³, ⁺, ⁻).
- Use emojis sparingly (💡 🧠 ⚕ 📘).
- Keep it exam-oriented, concise, and readable.

Give output as **formatted Markdown wrapped in a single SQL-safe string literal**, ready to insert into a Supabase text column.

Raw Concept:
${conceptText}
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
      response_format: { type: "text" },
    });
    return resp.choices?.[0]?.message?.content?.trim() || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

// ---------- Locks ----------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();
  await supabase
    .from("flashcard_raw")
    .update({ phase_json_lock: null, phase_json_lock_at: null })
    .is("concept_final", null)
    .lt("phase_json_lock_at", cutoff);
}

async function claimRows(limit) {
  await freeStaleLocks();
  const { data, error } = await supabase
    .from("flashcard_raw")
    .select("id, concept")
    .is("concept_final", null)
    .is("phase_json_lock", null)
    .limit(limit);
  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);
  const { data: locked, error: e2 } = await supabase
    .from("flashcard_raw")
    .update({
      phase_json_lock: WORKER_ID,
      phase_json_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("concept_final", null)
    .is("phase_json_lock", null)
    .select("id, concept");
  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("flashcard_raw")
    .update({ phase_json_lock: null, phase_json_lock_at: null })
    .in("id", ids);
}

// ---------- Processing ----------
async function processRow(row) {
  const prompt = buildPrompt(row.concept);
  const formatted = await callOpenAI(prompt);

  if (!formatted || typeof formatted !== "string") {
    throw new Error(`❌ No valid formatted text returned for id=${row.id}`);
  }

  const { error } = await supabase
    .from("flashcard_raw")
    .update({
      concept_final: formatted,
      phase_json_lock: null,
      phase_json_lock_at: null,
      updated_at: new Date().toISOString(),
    })
    .eq("id", row.id);

  if (error) throw new Error(`Update failed id=${row.id}: ${error.message}`);
  return { updated: 1 };
}

async function processBatch(rows) {
  const chunks = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE)
    chunks.push(rows.slice(i, i + BATCH_SIZE));

  let updated = 0;
  for (const chunk of chunks) {
    const results = await Promise.allSettled(chunk.map(processRow));
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (r.status === "fulfilled") updated += r.value.updated;
      else {
        console.error(r.reason?.message || r.reason);
        await clearLocks([chunk[i].id]);
      }
    }
  }
  return updated;
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧠 Concept Formatter Worker ${WORKER_ID} | model=${MODEL}`);
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
