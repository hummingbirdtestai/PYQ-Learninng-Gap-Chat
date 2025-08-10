// services/level1.worker.js
require('dotenv').config();
const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

// -------- Settings (env-overridable) --------
const L1_MODEL            = process.env.L1_MODEL || 'gpt-5-mini';
const L1_LIMIT            = parseInt(process.env.L1_LIMIT || '80', 10);        // rows per loop per instance
const L1_CONCURRENCY      = parseInt(process.env.L1_CONCURRENCY || '8', 10);   // parallel OpenAI calls
const L1_LOCK_TTL_MIN     = parseInt(process.env.L1_LOCK_TTL_MIN || '15', 10); // lock expiry
const L1_SLEEP_EMPTY_MS   = parseInt(process.env.L1_LOOP_SLEEP_MS || '750', 10);
const WORKER_ID           = process.env.WORKER_ID || `l1-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// -------- Prompt (verbatim from your ref) --------
const LEVEL_1_PROMPT_TEMPLATE = `🚨 OUTPUT RULES:
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
2. Generate a new **Level 1 MCQ** on the **learning_gap** of previous **primary_mcq** for Recursive Learning Gap detection for adaptive Learning.
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

// -------- Helpers --------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function l1CleanAndParseJSON(raw) {
  let t = String(raw || '').trim()
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/```$/,'')
    .trim();
  const first = t.indexOf('{'); const last = t.lastIndexOf('}');
  if (first === -1 || last === -1 || last < first) throw new Error('No JSON object found');
  t = t.slice(first, last + 1);
  return JSON.parse(t);
}

function l1NormalizePrimaryForPrompt(primaryObj) {
  const pm = primaryObj?.primary_mcq || primaryObj || {};
  return {
    buzzwords: Array.isArray(primaryObj?.buzzwords) ? primaryObj.buzzwords : [],
    learning_gap: typeof primaryObj?.learning_gap === 'string' ? primaryObj.learning_gap : '',
    primary_mcq: {
      stem: pm.stem ?? (primaryObj?.stem ?? ''),
      options: pm.options ?? (primaryObj?.options ?? {}),
      correct_answer: pm.correct_answer || pm.correct_option || primaryObj?.correct_answer || primaryObj?.correct_option || ''
    }
  };
}

function l1IsValidOutput(parsed) {
  const l1 = parsed?.level_1;
  if (!l1) return false;
  const mcq = l1.mcq;
  if (!mcq || typeof mcq.stem !== 'string') return false;
  const opts = mcq.options || {};
  const hasABCDE = ['A','B','C','D','E'].every(k => typeof opts[k] === 'string' && opts[k].length > 0);
  if (!hasABCDE) return false;
  if (typeof mcq.correct_answer !== 'string' || !mcq.correct_answer) return false;
  if (!Array.isArray(l1.buzzwords)) return false;
  if (typeof l1.learning_gap !== 'string') return false;
  return true;
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

// -------- OpenAI wrapper --------
async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: L1_MODEL,
      messages
      // keep temperature/max_tokens omitted for compatibility with gpt-5-mini chat.completions
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

// -------- Locking & claiming (Level-1 specific) --------
// Table mcq_bank must have: level_1 (jsonb|null), level1_lock (text|null), level1_locked_at (timestamptz|null)
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - L1_LOCK_TTL_MIN * 60 * 1000).toISOString();

  // 1) find candidates (unlocked or expired) that have primary_mcq but missing level_1
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

  // 2) lock a batch
  const { data: locked, error: e2 } = await supabase
    .from('mcq_bank')
    .update({ level1_lock: WORKER_ID, level1_locked_at: new Date().toISOString() })
    .in('id', ids)
    .is('level_1', null)
    .is('level1_lock', null)
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

// -------- Per-row processor --------
async function processRow(row) {
  try {
    const normalized = l1NormalizePrimaryForPrompt(row.primary_mcq);
    const prompt = `${LEVEL_1_PROMPT_TEMPLATE}\n\nPrimary MCQ:\n${JSON.stringify(normalized)}`;

    let parsed;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const raw = await callOpenAI([
          { role: 'system', content: 'You are a medical educator generating Level 1 MCQs in strict JSON.' },
          { role: 'user', content: prompt }
        ]);
        parsed = l1CleanAndParseJSON(raw);
        if (!l1IsValidOutput(parsed)) throw new Error('Invalid Level 1 schema');
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
        level_1: parsed.level_1,
        level1_lock: null,
        level1_locked_at: null
      })
      .eq('id', row.id);
    if (upErr) throw upErr;

    return { ok: true, id: row.id };
  } catch (e) {
    // Clear lock on failure so other instances can retry later
    await supabase
      .from('mcq_bank')
      .update({ level1_lock: null, level1_locked_at: null })
      .eq('id', row.id);
    return { ok: false, id: row.id, error: e.message || String(e) };
  }
}

// -------- Main loop --------
(async function main() {
  console.log(`🧵 Level-1 Worker ${WORKER_ID} | model=${L1_MODEL} | limit=${L1_LIMIT} | conc=${L1_CONCURRENCY} | ttl=${L1_LOCK_TTL_MIN}m`);

  while (true) {
    try {
      const claimed = await claimRows(L1_LIMIT);
      if (!claimed.length) {
        await sleep(L1_SLEEP_EMPTY_MS);
        continue;
      }

      console.log(`⚙️  claimed=${claimed.length}`);
      const results = await asyncPool(L1_CONCURRENCY, claimed, r => processRow(r));

      const ok = results.filter(r => r.status === 'fulfilled' && r.value?.ok).length;
      const fail = results.length - ok;
      console.log(`✅ ok=${ok}  ❌ fail=${fail}`);

      // Safety: clear any still-locked failures (should already be cleared in processRow)
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
