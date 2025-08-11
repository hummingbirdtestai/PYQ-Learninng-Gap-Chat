require('dotenv').config();
const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

/* -------- Settings (env-overridable) -------- */
const L2_MODEL          = process.env.L2_MODEL || 'gpt-5-mini';
const L2_LIMIT          = parseInt(process.env.L2_LIMIT || '80', 10);        // rows per loop per instance
const L2_CONCURRENCY    = parseInt(process.env.L2_CONCURRENCY || '8', 10);   // parallel OpenAI calls
const L2_LOCK_TTL_MIN   = parseInt(process.env.L2_LOCK_TTL_MIN || '15', 10); // lock expiry
const L2_SLEEP_EMPTY_MS = parseInt(process.env.L2_LOOP_SLEEP_MS || '750', 10);
const WORKER_ID         = process.env.WORKER_ID || `l2-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

/* -------- Prompt -------- */
const LEVEL_2_PROMPT_TEMPLATE = `🚨 OUTPUT RULES: 
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

🔬 You are an expert medical educator and exam learning strategist.

🎯 Your role is to act as a **Learning Gap Diagnostician** for MBBS/MD aspirants preparing for FMGE, NEETPG, INICET, or USMLE.

🧠 OBJECTIVE:
You will be given a Level 1 MCQ in the following JSON format:

{
  "mcq": {
    "stem": "...",
    "options": { "A": "...", "B": "...", "C": "...", "D": "...", "E": "..." },
    "correct_answer": "..."
  },
  "buzzwords": [...],
  "learning_gap": "..."
}

Your task is to:

1. Do NOT repeat the same learning gap or content.
2. Generate a new **Level 2 MCQ** on the **learning_gap** of previous **level_1** for Recursive Learning Gap detection for adaptive Learning.
3. Include 5 options (A–E), mark the correct answer.
4. Provide 10 *high-quality, laser-sharp, buzzword-style facts* related to the concept of the current MCQ:
   - Each fact must be *8 to 12 words long*, maximum of one sentence.
   - Start with a relevant *emoji*.
   - Bold key terms using <strong>...</strong>.
   - Format as flat strings in a "buzzwords": [] array.
   - Style should match Amboss/NBME/USMLE exam revision quality — *concise, specific, exam-sure*.
5. Identify the *key learning gap* if the MCQ was answered wrong.
   - The learning gap statement must be *one sentence*, and include <strong>bolded keywords</strong> for the missed concept.
6. Return in this format:

{
  "level_2": {
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

/* -------- Helpers -------- */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function l2CleanAndParseJSON(raw) {
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

// Accept both shapes: { mcq, buzzwords, learning_gap } OR { level_1: { ... } }
function l2NormalizeLevel1ForPrompt(l1ObjRaw) {
  const l1 = l1ObjRaw?.level_1 || l1ObjRaw || {};
  const mcq = l1.mcq || {};
  return {
    mcq: {
      stem: mcq.stem ?? '',
      options: mcq.options ?? {}, // A–D required; E optional (we validate later)
      correct_answer: mcq.correct_answer || mcq.correct_option || ''
    },
    buzzwords: Array.isArray(l1.buzzwords) ? l1.buzzwords : [],
    learning_gap: typeof l1.learning_gap === 'string' ? l1.learning_gap : ''
  };
}

function l2IsValidOutput(parsed) {
  const l2 = parsed?.level_2;
  if (!l2) return false;
  const mcq = l2.mcq;
  if (!mcq || typeof mcq.stem !== 'string') return false;
  const opts = mcq.options || {};
  const hasABCDE = ['A','B','C','D','E'].every(k => typeof opts[k] === 'string' && opts[k].length > 0);
  if (!hasABCDE) return false; // strict A–E for L2 as per prompt
  if (typeof mcq.correct_answer !== 'string' || !mcq.correct_answer) return false;
  if (!Array.isArray(l2.buzzwords)) return false;
  if (typeof l2.learning_gap !== 'string') return false;
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

/* -------- OpenAI wrapper -------- */
async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: L2_MODEL,
      messages
      // omit temperature/max_tokens for gpt-5-mini chat.completions
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

/* -------- Locking & claiming (Level-2 specific) -------- */
// Requires: level_2 (jsonb|null), level2_lock (text|null), level2_locked_at (timestamptz|null)
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - L2_LOCK_TTL_MIN * 60 * 1000).toISOString();

  // Clear stale locks so they can be reclaimed
  await supabase
    .from('mcq_bank')
    .update({ level2_lock: null, level2_locked_at: null })
    .is('level_2', null)
    .lt('level2_locked_at', cutoff);

  // 1) find candidates (unlocked or expired) that have level_1 but missing level_2
  const { data: candidates, error: e1 } = await supabase
    .from('mcq_bank')
    .select('id')
    .is('level_2', null)
    .not('level_1', 'is', null)
    .or(`level2_lock.is.null,level2_locked_at.lt.${cutoff}`)
    .order('id', { ascending: true })
    .limit(limit * 3);

  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  // 2) lock a batch & fetch payload
  const { data: locked, error: e2 } = await supabase
    .from('mcq_bank')
    .update({ level2_lock: WORKER_ID, level2_locked_at: new Date().toISOString() })
    .in('id', ids)
    .is('level_2', null)
    .is('level2_lock', null)
    .select('id, level_1');

  if (e2) throw e2;
  return (locked || []).slice(0, limit);
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from('mcq_bank')
    .update({ level2_lock: null, level2_locked_at: null })
    .in('id', ids);
}

/* -------- Per-row processor -------- */
async function processRow(row) {
  try {
    const normalized = l2NormalizeLevel1ForPrompt(row.level_1);
    const prompt = `${LEVEL_2_PROMPT_TEMPLATE}\n\nLevel 1 MCQ:\n${JSON.stringify(normalized)}`;

    let parsed;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const raw = await callOpenAI([
          { role: 'system', content: 'You are a medical educator generating Level 2 MCQs in strict JSON.' },
          { role: 'user', content: prompt }
        ]);
        parsed = l2CleanAndParseJSON(raw);
        if (!l2IsValidOutput(parsed)) throw new Error('Invalid Level 2 schema');
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
        level_2: parsed.level_2,
        level2_lock: null,
        level2_locked_at: null
      })
      .eq('id', row.id);
    if (upErr) throw upErr;

    return { ok: true, id: row.id };
  } catch (e) {
    // Clear lock on failure so other instances can retry later
    await supabase
      .from('mcq_bank')
      .update({ level2_lock: null, level2_locked_at: null })
      .eq('id', row.id);
    return { ok: false, id: row.id, error: e.message || String(e) };
  }
}

/* -------- Main loop -------- */
(async function main() {
  console.log(`🧵 Level-2 Worker ${WORKER_ID} | model=${L2_MODEL} | limit=${L2_LIMIT} | conc=${L2_CONCURRENCY} | ttl=${L2_LOCK_TTL_MIN}m`);

  while (true) {
    try {
      const claimed = await claimRows(L2_LIMIT);
      if (!claimed.length) {
        await sleep(L2_SLEEP_EMPTY_MS);
        continue;
      }

      console.log(`⚙️  claimed=${claimed.length}`);
      const results = await asyncPool(L2_CONCURRENCY, claimed, r => processRow(r));

      const ok = results.filter(r => r.status === 'fulfilled' && r.value?.ok).length;
      const fail = results.length - ok;
      console.log(`✅ ok=${ok}  ❌ fail=${fail}`);

      // Safety: clear any still-locked failures (already cleared in processRow)
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
