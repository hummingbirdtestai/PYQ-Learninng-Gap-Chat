// workers/learningGapTutorWorker.js
require('dotenv').config();
const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');
const { v4: uuidv4 } = require('uuid');

const MODEL             = process.env.LG_MODEL || 'gpt-5-mini';
const LIMIT             = parseInt(process.env.LG_LIMIT || '50', 10);
const CONCURRENCY       = parseInt(process.env.LG_CONCURRENCY || '6', 10);
const LOCK_TTL_MIN      = parseInt(process.env.LG_LOCK_TTL_MIN || '15', 10);
const SLEEP_EMPTY_MS    = parseInt(process.env.LG_LOOP_SLEEP_MS || '1000', 10);
const WORKER_ID         = process.env.WORKER_ID || `lg-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

/* -------- Prompt (verbatim from your spec) -------- */
const PROMPT_TEMPLATE = `🚨 OUTPUT RULES: 
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

You are an expert NEETPG/USMLE/FMGE medical tutor.

Pre-Cleaning Rule
If the given MCQ has numbering/poor wording, rebuild it into exam style with:
- Clear stem/vignette (3 sentences, USMLE-style).
- 4 balanced options (A–D).
- Correct answer mapped.
This cleaned MCQ = Level 1 in Recursive Gaps + mcq_1 in Tutoring Tree.

Phase 1: Recursive Learning Gaps
Always generate exactly 6 levels of learning gaps.
Level 1 = exact confusion of the primary MCQ.
Levels 2–6 = progressively deeper, fundamental gaps, each logically explaining the previous one.
Do not hardcode; derive dynamically from the given MCQ.
In both "confusion" and "gap", mark high-yield terms in **bold**.
Each gap must follow:
{ "level": n, "confusion": "...", "gap": "..." }

Phase 2: Adaptive Tutoring Tree (MCQs)
Create exactly 6 MCQs (mcq_1–mcq_6), each directly addressing one gap.
mcq_1 = given PYQ.
mcq_2–mcq_6 = progressively deeper remediation.

🚨 Uncompromising MCQ Rules (must follow verbatim):
stem: 3 sentences, USMLE-style vignette, high-yield, with Markdown (bold buzzwords, italics).
options: 4 choices (A–D).
correct_answer: single uppercase letter.
feedback.correct: ✅ acknowledgement, praise, high-yield reinforcement, mnemonic/tip; 3–5 empathetic, live sentences.
feedback.wrong: ❌ acknowledgement, why it seems logical, correction, mnemonic/hook; 3–5 empathetic, live sentences.
learning_gap: one concise sentence explaining the misconception.

Phase 3: Enrichment (strict)
At root include:
- final_summary → Markdown cheat-sheet table containing strictly 15 High Yield facts with **bold italic highlighted words** + mnemonic.
- high_yield_images → exactly 10 items, strictly from the first PYQ topic only.
Each item = { "search_query": "...", "description": "...", "keywords": ["...", "...", "..."] }
- recommended_videos → exactly 5 items, strictly exam-prep, first PYQ topic only.
Each item = { "search_query": "...", "description": "...", "keywords": ["...", "...", "..."] }
- buzzwords → 5–10 high-yield phrases from the PYQ + remediation chain.
- learning_gap_tags → 3–6 structured tags for common confusions.

Sample JSON
{ "recursive_learning_gaps":[...], "mcqs":{...}, "final_summary":"...", "high_yield_images":[...], "recommended_videos":[...], "buzzwords":[...], "learning_gap_tags":[...] }
`;

/* -------- Helpers -------- */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function cleanAndParseJSON(raw) {
  let t = String(raw || '').trim()
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/```$/,'')
    .trim();
  const first = t.indexOf('{'); const last = t.lastIndexOf('}');
  if (first === -1 || last === -1 || last < first) throw new Error('No JSON found');
  t = t.slice(first, last + 1);
  return JSON.parse(t);
}

/* --- UUID injection into JSON --- */
function addUUIDs(parsed) {
  if (parsed?.recursive_learning_gaps) {
    parsed.recursive_learning_gaps = parsed.recursive_learning_gaps.map(g => ({
      id: uuidv4(),
      ...g
    }));
  }
  if (parsed?.mcqs) {
    for (const key of Object.keys(parsed.mcqs)) {
      parsed.mcqs[key] = {
        id: uuidv4(),
        ...parsed.mcqs[key]
      };
    }
  }
  return parsed;
}

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|rate limit|temporar|unavailable|ECONNRESET/i.test(s);
}

async function asyncPool(limit, items, iter) {
  const out = []; const exec = [];
  for (const it of items) {
    const p = Promise.resolve().then(() => iter(it));
    out.push(p);
    const e = p.then(() => exec.splice(exec.indexOf(e), 1));
    exec.push(e);
    if (exec.length >= limit) await Promise.race(exec);
  }
  return Promise.allSettled(out);
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages
    });
    return resp.choices?.[0]?.message?.content || '';
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw e;
  }
}

/* -------- Locking & claiming -------- */
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  await supabase
    .from('mcq_bank')
    .update({ lg_lock: null, lg_locked_at: null })
    .is('learning_gap', null)
    .lt('lg_locked_at', cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from('mcq_bank')
    .select('id, mcq')
    .is('learning_gap', null)
    .not('mcq', 'is', null)
    .or(`lg_lock.is.null,lg_locked_at.lt.${cutoff}`)
    .order('id', { ascending: true })
    .limit(limit * 3);

  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  const { data: locked, error: e2 } = await supabase
    .from('mcq_bank')
    .update({ lg_lock: WORKER_ID, lg_locked_at: new Date().toISOString() })
    .in('id', ids)
    .is('learning_gap', null)
    .is('lg_lock', null)
    .select('id, mcq');

  if (e2) throw e2;
  return (locked || []).slice(0, limit);
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from('mcq_bank')
    .update({ lg_lock: null, lg_locked_at: null })
    .in('id', ids);
}

/* -------- Per-row processor -------- */
async function processRow(row) {
  try {
    const prompt = `${PROMPT_TEMPLATE}\n\nMCQ:\n${row.mcq}`;
    let parsed;

    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const raw = await callOpenAI([
          { role: 'system', content: 'You are a medical educator generating adaptive tutoring JSON in strict JSON.' },
          { role: 'user', content: prompt }
        ]);
        parsed = cleanAndParseJSON(raw);
        parsed = addUUIDs(parsed); // 🔑 inject UUIDs here
        break;
      } catch (err) {
        if (attempt < 3 && isRetryable(err)) {
          await sleep(400 * attempt);
          continue;
        }
        throw err;
      }
    }

    const { error: upErr } = await supabase
      .from('mcq_bank')
      .update({
        learning_gap: parsed,
        lg_lock: null,
        lg_locked_at: null
      })
      .eq('id', row.id);

    if (upErr) throw upErr;
    return { ok: true, id: row.id };
  } catch (e) {
    await supabase
      .from('mcq_bank')
      .update({ lg_lock: null, lg_locked_at: null })
      .eq('id', row.id);
    return { ok: false, id: row.id, error: e.message || String(e) };
  }
}

/* -------- Main loop -------- */
(async function main() {
  console.log(`🧵 LearningGap Worker ${WORKER_ID} | model=${MODEL} | limit=${LIMIT} | conc=${CONCURRENCY} | ttl=${LOCK_TTL_MIN}m`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_EMPTY_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);
      const results = await asyncPool(CONCURRENCY, claimed, r => processRow(r));

      const ok = results.filter(r => r.status === 'fulfilled' && r.value?.ok).length;
      const fail = results.length - ok;
      console.log(`✅ ok=${ok}  ❌ fail=${fail}`);

      const stillLocked = claimed
        .filter((_, i) => results[i].status !== 'fulfilled' || !results[i].value?.ok)
        .map(r => r.id);
      if (stillLocked.length) await clearLocks(stillLocked);
    } catch (e) {
      console.error('Loop error:', e.message || e);
      await sleep(1000);
    }
  }
})();
