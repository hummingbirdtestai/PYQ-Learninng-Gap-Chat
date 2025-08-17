require('dotenv').config();
const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');
const { v4: uuidv4 } = require('uuid');

/* -------- Settings -------- */
const LG_MODEL          = process.env.LG_MODEL || 'gpt-5-mini';
const LG_LIMIT          = parseInt(process.env.LG_LIMIT || '80', 10);
const LG_CONCURRENCY    = parseInt(process.env.LG_CONCURRENCY || '8', 10);
const LG_LOCK_TTL_MIN   = parseInt(process.env.LG_LOCK_TTL_MIN || '15', 10);
const LG_SLEEP_EMPTY_MS = parseInt(process.env.LG_LOOP_SLEEP_MS || '750', 10);
const WORKER_ID         = process.env.WORKER_ID || `lg-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

/* -------- Prompt (verbatim) -------- */
const LEARNING_GAP_PROMPT = `You are an expert NEETPG/USMLE/FMGE medical tutor. You are given a MCQ Create Recursive Learning Gaps Always generate exactly 6 levels of learning gaps. Level 1 = exact confusion of the primary MCQ. Levels 2–6 = progressively deeper, fundamental gaps, each logically explaining the previous one. Do not hardcode; derive dynamically from the given MCQ. In both "confusion" and "gap", mark high-yield terms in bold. Each gap must follow: { "level": n, "confusion": "...", "gap": "..." } { "recursive_learning_gaps": [ { "level": 1, "confusion": " ...", "gap": " ..." }, { "level": 2, "confusion": " ...", "gap": " ..." }, { "level": 3, "confusion": " ...", "gap": " ..." }, { "level": 4, "confusion": " ...", "gap": " ..." }, { "level": 5, "confusion": " ...", "gap": " ..." }, { "level": 6, "confusion": " ...", "gap": "..." } ] }`;

/* -------- Helpers -------- */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function cleanAndParseJSON(raw) {
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

function isValidOutput(parsed) {
  const arr = parsed?.recursive_learning_gaps;
  if (!Array.isArray(arr) || arr.length !== 6) return false;
  return arr.every(
    g => typeof g.level === 'number' &&
         typeof g.confusion === 'string' &&
         typeof g.gap === 'string'
  );
}

function addUUIDs(parsed) {
  if (!parsed?.recursive_learning_gaps) return parsed;
  parsed.recursive_learning_gaps = parsed.recursive_learning_gaps.map(g => ({
    id: uuidv4(),
    ...g
  }));
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

/* -------- OpenAI wrapper -------- */
async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: LG_MODEL,
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
// Requires: learning_gap (jsonb|null), lg_lock (text|null), lg_locked_at (timestamptz|null)
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LG_LOCK_TTL_MIN * 60 * 1000).toISOString();

  await supabase
    .from('mcq_bank')
    .update({ lg_lock: null, lg_locked_at: null })
    .is('learning_gap', null)
    .lt('lg_locked_at', cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from('mcq_bank')
    .select('id')
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
    const prompt = `${LEARNING_GAP_PROMPT}\n\nMCQ:\n${JSON.stringify(row.mcq)}`;

    let parsed;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const raw = await callOpenAI([
          { role: 'system', content: 'You are a medical educator generating recursive learning gaps in strict JSON.' },
          { role: 'user', content: prompt }
        ]);
        parsed = cleanAndParseJSON(raw);
        if (!isValidOutput(parsed)) throw new Error('Invalid learning_gap schema');
        parsed = addUUIDs(parsed);
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
  console.log(`🧵 Learning Gap Worker ${WORKER_ID} | model=${LG_MODEL} | limit=${LG_LIMIT} | conc=${LG_CONCURRENCY} | ttl=${LG_LOCK_TTL_MIN}m`);

  while (true) {
    try {
      const claimed = await claimRows(LG_LIMIT);
      if (!claimed.length) {
        await sleep(LG_SLEEP_EMPTY_MS);
        continue;
      }

      console.log(`⚙️  claimed=${claimed.length}`);
      const results = await asyncPool(LG_CONCURRENCY, claimed, r => processRow(r));

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
