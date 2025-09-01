require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

/* -------- Settings -------- */
const GEN_MODEL        = process.env.GEN_MODEL || "gpt-5-mini";
const GEN_LIMIT        = parseInt(process.env.GEN_LIMIT || "120", 10);
const GEN_CONCURRENCY  = parseInt(process.env.GEN_CONCURRENCY || "5", 10);
const GEN_LOCK_TTL_MIN = parseInt(process.env.GEN_LOCK_TTL_MIN || "45", 10);
const SLEEP_EMPTY_MS   = parseInt(process.env.GEN_LOOP_SLEEP_MS || "800", 10);
const WORKER_ID        = process.env.WORKER_ID || `lg-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

/* -------- Prompt Template -------- */
const PROMPT_TEMPLATE = `
You are a NEETPG & USMLE expert medical teacher with 20 years of experience.

### Task
From the RAW MCQ text provided as input, create exactly 20 Questions and Answers for NEETPG/USMLE preparation.

### Rules
1. Output must be a **valid JSON object only**.  
2. At the top level, include a key "topic" = the most specific, high-yield subject of the primary MCQ (e.g., "Pulmonary Circulation", "Beta Blockers", "Renal Physiology").  
3. Also include a key "questions" = array of exactly 20 objects.  
4. Each object in "questions" must have exactly these keys:  
   - "question": Rewrite the MCQ into a **3–4 sentence clinical vignette style active recall question**.  
   - "answer": Provide the **precise, high-yield answer**.  
5. Do **NOT format as MCQs**. Each must be in **Question–Answer format**.  
6. Use **Markdown bold** to highlight important exam facts.  
7. Do not include UUIDs. Supabase will auto-generate them.  
8. Do not include any text outside the JSON.  
9. Questions must **not be repeated** — instead, create **recursive remediation questions**.  
10. If a vignette is not possible, frame a **direct NEETPG-style high-yield Q&A** instead.  
11. Ensure all questions and answers are **unique and exam-style**.  
12. Start with the **primary fact tested in the raw MCQ**, then branch into **18–19 related high-yield concepts**.  
13. Maintain the same JSON structure strictly for every output.  

### Output JSON Format Example

{
  "topic": "Pulmonary Circulation",
  "questions": [
    { "question": "", "answer": "" }
  ]
}
`;

/* -------- Helpers -------- */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function cleanAndParseJSON(raw) {
  let t = (raw || "").trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/```$/, "")
    .trim();
  return JSON.parse(t);
}

async function asyncPool(limit, items, iter) {
  const out = [];
  const exec = [];
  for (const it of items) {
    const p = Promise.resolve().then(() => iter(it));
    out.push(p);
    const e = p.then(() => exec.splice(exec.indexOf(e), 1));
    exec.push(e);
    if (exec.length >= limit) await Promise.race(exec);
  }
  return Promise.allSettled(out);
}

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|rate limit|temporar|unavailable|ECONNRESET/i.test(s);
}

async function callOpenAI(mcqText, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: GEN_MODEL,
      messages: [
        { role: "system", content: PROMPT_TEMPLATE },
        { role: "user", content: mcqText }
      ]
      // ❌ removed temperature (not supported in gpt-5-mini)
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(mcqText, attempt + 1);
    }
    throw e;
  }
}

/* -------- Locking & Claiming -------- */
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - GEN_LOCK_TTL_MIN * 60 * 1000).toISOString();

  await supabase
    .from("learning_gap_vertical")
    .update({ topic_lock: null, topic_locked_at: null })
    .lt("topic_locked_at", cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from("learning_gap_vertical")
    .select("id, mcq_json")
    .is("topic", null)
    .is("topic_lock", null)
    .not("mcq_json", "is", null)
    .order("id", { ascending: true })
    .limit(limit * 3);

  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  const { data: locked, error: e2 } = await supabase
    .from("learning_gap_vertical")
    .update({
      topic_lock: WORKER_ID,
      topic_locked_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("topic", null)
    .is("topic_lock", null)
    .select("id, mcq_json");

  if (e2) throw e2;

  return (locked || []).slice(0, limit);
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("learning_gap_vertical")
    .update({ topic_lock: null, topic_locked_at: null })
    .in("id", ids);
}

/* -------- Per-Row Processor -------- */
async function processRow(row) {
  try {
    const raw = await callOpenAI(
      typeof row.mcq_json === "string" ? row.mcq_json : JSON.stringify(row.mcq_json)
    );

    console.log("🔎 Raw GPT output for row", row.id, ":", (raw || "").slice(0, 400));

    let parsed;
    try {
      parsed = cleanAndParseJSON(raw);
    } catch (e) {
      console.error("❌ JSON parse error for row", row.id, "output:", raw);
      throw e;
    }

    const topicGuess = parsed.topic || "General";

    const { error: upErr } = await supabase
      .from("learning_gap_vertical")
      .update({
        topic: topicGuess,
        topic_lock: null,
        topic_locked_at: null
      })
      .eq("id", row.id);

    if (upErr) throw upErr;

    return { ok: true, id: row.id };
  } catch (e) {
    console.error("❌ processRow failed", row.id, e.message);
    await clearLocks([row.id]);
    return { ok: false, id: row.id, error: e.message || String(e) };
  }
}

/* -------- Main Loop -------- */
(async function main() {
  console.log(`🧵 Topic Worker ${WORKER_ID} | model=${GEN_MODEL} | limit=${GEN_LIMIT} | conc=${GEN_CONCURRENCY} | ttl=${GEN_LOCK_TTL_MIN}m`);
  while (true) {
    try {
      const claimed = await claimRows(GEN_LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_EMPTY_MS);
        continue;
      }
      console.log(`⚙️ claimed=${claimed.length}`);
      const results = await asyncPool(GEN_CONCURRENCY, claimed, r => processRow(r));
      const ok = results.filter(r => r.status === "fulfilled" && r.value?.ok).length;
      const fail = results.length - ok;
      console.log(`✅ ok=${ok} ❌ fail=${fail}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
