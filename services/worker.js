require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

/* -------- Settings (env-overridable) -------- */
const GEN_MODEL        = process.env.GEN_MODEL || "gpt-5-mini";
const GEN_LIMIT        = parseInt(process.env.GEN_LIMIT || "120", 10);
const GEN_CONCURRENCY  = parseInt(process.env.GEN_CONCURRENCY || "5", 10);
const GEN_LOCK_TTL_MIN = parseInt(process.env.GEN_LOCK_TTL_MIN || "45", 10);
const SLEEP_EMPTY_MS   = parseInt(process.env.GEN_LOOP_SLEEP_MS || "800", 10);
const WORKER_ID        = process.env.WORKER_ID || `lg-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

/* -------- Prompt Template (verbatim from you) -------- */
const PROMPT_TEMPLATE = `
You are a NEETPG & USMLE expert medical teacher with 20 years of experience.

### Task
From the RAW MCQ text provided as input, create exactly 20 Questions and Answers for NEETPG/USMLE preparation.

### Rules
1. Output must be a **valid JSON array only**.  
2. Each Question and Answer must be a JSON object with exactly these keys:  
   - "question": Rewrite the MCQ into a **3–4 sentence clinical vignette style active recall question** (standard of NBME, Amboss, UWorld, USMLERx). Must be **exam-oriented, very specific, and high-yield**.  
   - "answer": Provide the **precise, high-yield answer** (not generic).  
3. Do **NOT format as MCQs**. Each must be in **Question–Answer format**.  
4. Use **Markdown bold** to highlight important words, numbers, anatomical structures, diseases, drugs, and exam-relevant facts in both the **question** and the **answer**.  
5. Do not include UUIDs. Supabase will auto-generate them.  
6. Do not include any text outside the JSON (no commentary, no explanations, no headings).  
7. Questions must **not be repeated** — instead, create **recursive remediation questions** that expand into the most relevant connected high-yield concepts that logically follow from the primary MCQ, as appropriate for the subject.  
8. If a **clinical vignette is not possible** (low-yield recall fact), frame a **direct NEETPG-style high-yield Q&A** instead.  
9. Ensure all questions and answers are **unique, non-repetitive, and exam-style**.  
10. Start with the **primary fact tested in the raw MCQ**, and then branch into **18–19 progressively related high-yield concepts** for active recall and spaced repetition.  
11. Maintain the same JSON structure strictly for every output.  

### Output JSON Format Example

[
  {
    "question": "",
    "answer": ""
  }
]
`;

/* -------- Helpers -------- */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function cleanAndParseJSON(raw) {
  let t = (raw || "").trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/```$/, "")
    .trim();
  if (!t.startsWith("[") || !t.endsWith("]")) throw new Error("No JSON array found");
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
      ],
      temperature: 0
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

  // release stale locks
  await supabase
    .from("mcq_bank")
    .update({ lg_lock: null, lg_locked_at: null })
    .lt("lg_locked_at", cutoff);

  // find candidates
  const { data: candidates, error: e1 } = await supabase
    .from("mcq_bank")
    .select("id, mcq")
    .is("lg_flashcard", null)
    .or(`lg_lock.is.null,lg_locked_at.lt.${cutoff}`)
    .order("id", { ascending: true })
    .limit(limit * 3);
  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  // claim
  const { data: locked, error: e2 } = await supabase
    .from("mcq_bank")
    .update({
      lg_lock: WORKER_ID,
      lg_locked_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("lg_flashcard", null)
    .is("lg_lock", null)
    .select("id, mcq");
  if (e2) throw e2;

  return (locked || []).slice(0, limit);
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("mcq_bank")
    .update({ lg_lock: null, lg_locked_at: null })
    .in("id", ids);
}

/* -------- Per-Row Processor -------- */
async function processRow(row) {
  try {
    const raw = await callOpenAI(row.mcq);
    const parsed = cleanAndParseJSON(raw);

    const { error: upErr } = await supabase
      .from("mcq_bank")
      .update({
        lg_flashcard: parsed,
        lg_lock: null,
        lg_locked_at: null
      })
      .eq("id", row.id);
    if (upErr) throw upErr;

    return { ok: true, id: row.id };
  } catch (e) {
    await clearLocks([row.id]);
    return { ok: false, id: row.id, error: e.message || String(e) };
  }
}

/* -------- Main Loop -------- */
(async function main() {
  console.log(`🧵 Flashcard Worker ${WORKER_ID} | model=${GEN_MODEL} | limit=${GEN_LIMIT} | conc=${GEN_CONCURRENCY} | ttl=${GEN_LOCK_TTL_MIN}m`);
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
