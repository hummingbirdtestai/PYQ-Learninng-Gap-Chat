require('dotenv').config();
const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

/* ===== Settings (separate envs so they don't clash with Primary) ===== */
const L1_MODEL        = process.env.L1_MODEL || 'gpt-5-mini';
const L1_LIMIT        = parseInt(process.env.L1_LIMIT || '40', 10);      // rows per loop
const L1_CONCURRENCY  = parseInt(process.env.L1_CONCURRENCY || '4', 10); // parallel OpenAI calls
const L1_LOCK_TTL_MIN = parseInt(process.env.L1_LOCK_TTL_MIN || '15', 10);
const L1_SLEEP_EMPTY  = parseInt(process.env.L1_LOOP_SLEEP_MS || '750', 10);
const WORKER_ID       = process.env.WORKER_ID || `l1-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

/* ===== Prompt (your template, unchanged except fenced) ===== */
const LEVEL_1_PROMPT = `🚨 OUTPUT RULES:
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

🔬 You are an expert medical educator and exam learning strategist.

🎯 Your role is to act as a **Learning Gap Diagnostician** for MBBS/MD aspirants preparing for FMGE, NEETPG, INICET, or USMLE.

🧠 OBJECTIVE:
You will be given a MCQ in the following JSON format:

{
  "buzzwords": [...],
  "primary_mcq": {
    "stem": "...",
    "options": { "A": "...", "B": "...", "C": "...", "D": "...", "E": "..." },
    "correct_answer": "..."
  },
  "learning_gap": "..."
}

Your task is to:

1. Do NOT create an MCQ testing the same learning_gap verbatim.
2. Generate a new **Level 1 MCQ** on the **prior conceptual gap**.
3. Write a 5-sentence USMLE-style clinical vignette with bolded keywords.
4. Include 5 options (A–E), mark the correct answer.
5. Provide a new learning gap with 2+ <strong> keywords.
6. Include 10 high-yield buzzwords with emoji prefix and bold terms.
7. Return in this format:

{
  "level_1": {
    "mcq": {
      "stem": "...",
      "options": { "A": "...", "B": "...", "C": "...", "D": "...", "E": "..." },
      "correct_answer": "..."
    },
    "buzzwords": [...],
    "learning_gap": "..."
  }
}
`;

/* ===== helpers ===== */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function cleanAndParseJSON(raw) {
  let t = (raw || '').trim()
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/```$/,'')
    .trim();
  const first = t.indexOf('{'); const last = t.lastIndexOf('}');
  if (first === -1 || last === -1 || last < first) throw new Error('No JSON object found');
  t = t.slice(first, last + 1);
  return JSON.parse(t);
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

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|rate limit|temporar|unavailable|ECONNRESET/i.test(s);
}

async function callOpenAI(messages, attempt = 1) {
  try {
    // gpt-5-mini chat.completions: avoid temperature/max_tokens
    const resp = await openai.chat.completions.create({
      model: L1_MODEL,
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

/* ===== locking & claiming ===== */
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - L1_LOCK_TTL_MIN * 60 * 1000).toISOString();

  // proactively clear stale locks
  await supabase
    .from('mcq_bank')
    .update({ level1_lock: null, level1_locked_at: null })
    .is('level_1', null)
    .lt('level1_locked_at', cutoff);

  // find candidates (must have primary_mcq, missing level_1) and be unlocked or stale
  const { data: candidates, error: e1 } = await supabase
    .from('mcq_bank')
    .select('id')
    .is('level_1', null)
    .not('primary_mcq', 'is', null)
    .or(`level1_lock.is.null,level1_locked_at.lt.${cutoff}`)
    .order('id', { ascending: true })
    .limit(limit * 3);
  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  // lock & fetch
  const { data: locked, error: e2 } = await supabase
    .from('mcq_bank')
    .update({ level1_lock: WORKER_ID, level1_locked_at: new Date().toISOString() })
    .in('id', ids)
    .is('level_1', null)
    .not('primary_mcq', 'is', null)
    .or(`level1_lock.is.null,level1_locked_at.lt.${cutoff}`)
    .select('id, primary_mcq');
  if (e2) throw e2;

  return (locked || []).slice(0, limit);
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from('mcq_bank')
    .update({ level1_lock: null, level1_locked_at: null })
    .in('id', ids);
}

/* ===== per-row process ===== */
function isValidL1(obj) {
  const l1 = obj?.level_1;
  return !!(
    l1 &&
    l1.mcq &&
    typeof l1.mcq.stem === 'string' &&
    l1.mcq.options &&
    typeof l1.mcq.options.A === 'string' &&
    typeof l1.mcq.options.B === 'string' &&
    typeof l1.mcq.options.C === 'string' &&
    typeof l1.mcq.options.D === 'string' &&
    typeof l1.mcq.options.E === 'string' &&
    typeof l1.mcq.correct_answer === 'string' &&
    Array.isArray(l1.buzzwords) &&
    typeof l1.learning_gap === 'string'
  );
}

async function processRow(row) {
  try {
    // compact JSON to save tokens
    const compactPrimary = JSON.stringify(row.primary_mcq);
    const prompt = `${LEVEL_1_PROMPT}\n\nPrimary MCQ:\n${compactPrimary}`;

    const raw = await callOpenAI([{ role: 'user', content: prompt }]);

    const parsed = cleanAndParseJSON(raw);
    if (!isValidL1(parsed)) throw new Error('Invalid Level 1 schema');

    const { error: upErr } = await supabase
      .from('mcq_bank')
      .update({
        level_1: parsed.level_1,
        level1_lock: null,
        level1_locked_at: null
      })
      .eq('id', row.id);
    if (upErr) throw upErr;

    return { ok: true, id: row.id };
  } catch (e) {
    // on failure, clear lock so another attempt can try
    await supabase
      .from('mcq_bank')
      .update({ level1_lock: null, level1_locked_at: null })
      .eq('id', row.id);
    return { ok: false, id: row.id, error: e.message || String(e) };
  }
}

/* ===== main loop ===== */
(async function main() {
  console.log(`🧵 Level1 Worker ${WORKER_ID} | model=${L1_MODEL} | limit=${L1_LIMIT} | conc=${L1_CONCURRENCY} | ttl=${L1_LOCK_TTL_MIN}m`);

  while (true) {
    try {
      const claimed = await claimRows(L1_LIMIT);
      if (!claimed.length) {
        await sleep(L1_SLEEP_EMPTY);
        continue;
      }

      console.log(`⚙️  claimed=${claimed.length}`);
      const results = await asyncPool(L1_CONCURRENCY, claimed, r => processRow(r));

      const ok = results.filter(r => r.status === 'fulfilled' && r.value?.ok).length;
      const fail = results.length - ok;
      console.log(`✅ ok=${ok}  ❌ fail=${fail}`);

      // clear any that are still locked (should be none)
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
