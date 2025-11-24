// workers/orthoconceptGeneratorWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------------- SETTINGS ----------------
const MODEL        = process.env.CONCEPT_GEN_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CONCEPT_GEN_LIMIT || "200", 10);
const BATCH_SIZE   = parseInt(process.env.CONCEPT_GEN_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.CONCEPT_GEN_LOOP_SLEEP_MS || "800", 10);
const LOCK_TTL_MIN = parseInt(process.env.CONCEPT_GEN_LOCK_TTL_MIN || "15", 10);

const SUBJECT_FILTER = "Community Medicine";

const WORKER_ID = process.env.WORKER_ID ||
  `concept-worker-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// -------------- PROMPT ---------------------
function buildPrompt(topic) {
  return (
`
You are a 30-years experienced Undergraduate MBBS **Community Medicine** Teacher, expert in the NMC-prescribed Competency Based Curriculum. 
Explain the topic:*${topic}* using the following 6 sections. Keep language simple, Final-year MBBS friendly, accurate, and high-yield. Follow this exact structure:

1) Central Concept  
2) Core Community Medicine  
3) 10 High-Yield Facts  
4) Clinical Case Vignettes  
5) Viva Voce Questions  
6) Summary Table / Mnemonics

Follow these rules EXACTLY:

1) **Central Concept**  
   – Give a short, crisp, foundational explanation of the topic  
   – Use analogies if helpful  
   – Keep it ≤ 6 lines but concept-strong  

2) **Core Community Medicine**  
   – Give deep but MBBS-friendly coverage of  
     • Epidemiology  
     • Natural history of disease  
     • Levels of prevention  
     • R₀, incubation period, serial interval (if relevant)  
     • Screening principles (Wilson-Jungner, Snell)  
     • Biostatistics basics  
     • National programmes & strategies  
   – ALWAYS explain with simple examples  

3) **10 High-Yield Facts**  
   – Exactly 10 points  
   – Each point one line, exam-sharp, memory-friendly  
   – Include formulas, cut-offs, epidemiological measures, programme targets, etc.  

4) **Clinical Case Vignettes**  
   – 4–6 lines each  
   – Present like a real community/field/epidemiology scenario  
   – Always end with a 1-line final answer  
   – Include differential hints, investigation choices, and public-health actions  

5) **Viva Voce Questions**  
   – 10 questions + crisp one-line answers  
   – Must match typical MBBS finals viva depth  

6) **Summary Table / Mnemonics**  
   – Give a compact table  
   – Add 1–2 smart mnemonics  
   – Visual, simple, high-yield  

General Rules:  
– Maintain clean Markdown headings  
– Use simple language but high conceptual clarity  
– Do NOT write anything outside the 6 sections  
– No additional introductions or conclusions  
– Keep entire output exam-oriented and Community-Medicine specific
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
    .eq("subject", SUBJECT_FILTER)
    .lt("concept_lock_at", cutoff)
    .is("concept", null);

  if (error) console.error("freeStaleLocks error:", error);
}

async function claimRows(limit) {
  await freeStaleLocks();

  // Pick unlocked rows
  let { data: rows, error } = await supabase
    .from("subject_curriculum")
    .select("id, topic")
    .eq("subject", SUBJECT_FILTER)
    .is("concept", null)
    .is("concept_lock", null)
    .order("id", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // Apply lock
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
      concept: output,         // Markdown stored directly
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
