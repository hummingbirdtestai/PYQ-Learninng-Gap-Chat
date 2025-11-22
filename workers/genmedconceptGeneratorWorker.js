// workers/genmedconceptGeneratorWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------------- SETTINGS ----------------
const MODEL        = process.env.CONCEPT_GEN_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CONCEPT_GEN_LIMIT || "200", 10);
const BATCH_SIZE   = parseInt(process.env.CONCEPT_GEN_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.CONCEPT_GEN_LOOP_SLEEP_MS || "800", 10);
const LOCK_TTL_MIN = parseInt(process.env.CONCEPT_GEN_LOCK_TTL_MIN || "15", 10);

// MULTI-SUBJECT LIST
const SUBJECT_FILTERS = [
  "General Medicine",
  "Respiratory Medicine",
  "Physical Medicine",
  "Radiotherapy"
];

const WORKER_ID = process.env.WORKER_ID ||
  `concept-worker-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// -------------- PROMPT ---------------------
function buildPrompt(topic) {
  return (
`
You are an 30 Years experienced Undergraduate MBBS **General Medicine** Teacher expert in NMC PRESCRIBED Competency Based Curriculum. 
Explain the topic:*${topic}* using the following 6 sections. Keep language simple, Final-year MBBS friendly, accurate, and high-yield. Follow this exact structure:

1) Central Concept  
2) Core General Medicine  
3) 10 High-Yield Facts  
4) Clinical Case Vignettes  
5) Viva Voce Questions  
6) Summary Table / Mnemonics

Explain using the following rules exactly:

1) **Central Concept**  
   – Give a short, crisp, foundational explanation of the topic.  
   – Use analogies if helpful.

2) **Core General Medicine**  
   – Explain **pathophysiology, clinical features, risk factors, red-flag symptoms, diagnostic approach, bedside examination clues, investigations (CBC, LFT, RFT, ECG, CXR, CT/MRI, echo), important lab interpretations, systemic involvement patterns (CVS, RS, CNS, endocrine, renal), complications, differential diagnosis, management principles (acute + chronic)**, and follow-up essentials.  
   – Present in concise bullet points.

3) **10 High-Yield Facts (USMLE + NEET-PG + FMGE)**  
   – Single-line pearls  
   – Emphasize exam-friendly and memory-friendly points.

4) **5 Clinical Case Vignettes (Medicine-oriented)**  
   – Each 3–4 lines maximum  
   – Reasoning should connect **symptom → system involved → key investigation → most likely diagnosis**.

5) **Top 5 Viva Voce Questions (with answers)**  
   – Simple, direct, easily recallable.

6) **Provide a summary table, differential diagnosis chart, investigation interpretation table, severity scoring system, red-flag signs list, or mnemonic for revision.**

Output must strictly follow Sections 1–6.  
Give the output **strictly in Markdown code blocks** with Unicode symbols.  
In the output, explicitly **bold and italicize** all important key words, clinical terms, diseases, signs, investigations, and headings for emphasis using proper Markdown (e.g., *bold, italic*).  
Use headings, **bold**, *italic*, arrows (→, ↑, ↓), subscripts/superscripts (₁, ₂, ³, ⁺, ⁻), Greek letters, and emojis (💡🫀🫁🧠⚕📘) naturally throughout for visual clarity.  
Do **NOT** output as JSON but output as **Markdown code blocks**.  
Do **NOT** add any titles or headers beyond the 6 sections I specify.  
Output ONLY those 6 sections exactly as numbered.
`
  ).trim();
}

// ---------------- HELPERS ------------------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(String(e));
}

// ---------------- OPENAI CALL ---------------
async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }]
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      console.warn(`⏳ Retrying OpenAI call (attempt ${attempt}) due to:`, e.message);
      await sleep(300 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

// ---------------- LOCKING -------------------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  const { error } = await supabase
    .from("subject_curriculum")
    .update({ concept_lock: null, concept_lock_at: null })
    .in("subject", SUBJECT_FILTERS)
    .lt("concept_lock_at", cutoff)
    .is("concept", null);

  if (error) console.error("freeStaleLocks error:", error);
}

async function claimRows(limit) {
  await freeStaleLocks();

  let { data: rows, error } = await supabase
    .from("subject_curriculum")
    .select("id, topic")
    .in("subject", SUBJECT_FILTERS)
    .is("concept", null)
    .is("concept_lock", null)
    .order("id", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  const { data: locked, error: lockErr } = await supabase
    .from("subject_curriculum")
    .update({
      concept_lock: WORKER_ID,
      concept_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("concept", null)
    .is("concept_lock", null)
    .select("id, topic");

  if (lockErr) throw lockErr;

  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("subject_curriculum")
    .update({ concept_lock: null, concept_lock_at: null })
    .in("id", ids);
}

// ---------------- PROCESS SINGLE ------------
async function processRow(row) {
  const prompt = buildPrompt(row.topic);
  const output = await callOpenAI(prompt);

  if (!output || output.length < 100) {
    throw new Error(`Empty/invalid output for id=${row.id}`);
  }

  const { error: upErr } = await supabase
    .from("subject_curriculum")
    .update({
      concept: output,
      concept_lock: null,
      concept_lock_at: null,
    })
    .eq("id", row.id);

  if (upErr) {
    throw new Error(`Update failed for id=${row.id}: ${upErr.message}`);
  }

  return { updated: 1 };
}

// ---------------- BATCH ---------------------
async function processBatch(rows) {
  const chunks = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    chunks.push(rows.slice(i, i + BATCH_SIZE));
  }

  let updated = 0;

  for (const chunk of chunks) {
    const results = await Promise.allSettled(
      chunk.map(r => processRow(r))
    );

    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (r.status === "fulfilled") {
        updated += r.value.updated;
      } else {
        console.error(`❌ Row ${chunk[i].id} error:`, r.reason?.message || r.reason);
        await clearLocks([chunk[i].id]);
      }
    }
  }

  return updated;
}

// ---------------- MAIN LOOP -----------------
(async function main() {
  console.log(`🧠 Concept Generator Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows`);

      const updated = await processBatch(claimed);
      console.log(`✅ Completed batch: updated=${updated}/${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message);
      await sleep(1000);
    }
  }
})();
