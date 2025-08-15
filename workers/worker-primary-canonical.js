// worker-primary-canonical.js
require('dotenv').config();
const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

/* -------- Settings (env-overridable) -------- */
const GEN_MODEL          = process.env.GEN_MODEL || 'gpt-5-mini';
const GEN_LIMIT          = parseInt(process.env.GEN_LIMIT || '150', 10);  // rows per claim
const GEN_CONCURRENCY    = parseInt(process.env.GEN_CONCURRENCY || '5', 10); // parallel API calls
const GEN_LOCK_TTL_MIN   = parseInt(process.env.GEN_LOCK_TTL_MIN || '45', 10);
const SLEEP_EMPTY_MS     = parseInt(process.env.GEN_LOOP_SLEEP_MS || '400', 10);
const WORKER_ID          = process.env.WORKER_ID || `w-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

/* -------- Prompt Template -------- */
const PROMPT_TEMPLATE = `
🚨 OUTPUT RULES: Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations or comments.
- Output must start with { and end with } and be valid JSON.

You are given a medical MCQ in any format.
Convert it into a clean JSON object with exactly this shape:

{
  "buzzwords": ["array of 5–12 concise, high-yield points from the question"],
  "stem": "Full question stem text with HTML tags preserved where needed for formatting.",
  "options": {
    "A": "Option text",
    "B": "Option text",
    "C": "Option text",
    "D": "Option text",
    "E": "Option text if present, else omit"
  },
  "correct_answer": "Letter of correct option (A–E)",
  "learning_gap": "One-sentence explanation of what the student missed or needs to learn if wrong."
}

Rules:
- All keys must be present (empty arrays/objects if no data).
- Do not put options in arrays; must be an object with keys A, B, C, D (and E if present).
- correct_answer must be a single uppercase letter A–E.
- Buzzwords must be concise, exam-relevant, and use <strong>...</strong> for bold.
- Preserve all original medical details.
- Remove "(Correct)" from option text and set correct_answer accordingly.
`;

/* -------- Helpers -------- */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function truncate(s, n = 2000) {
  s = String(s || '');
  return s.length > n ? s.slice(0, n) + ' …' : s;
}

function buildPrompt(row) {
  const mcqText = typeof row.primary_mcq === 'string'
    ? row.primary_mcq
    : JSON.stringify(row.primary_mcq || {});
  return `${PROMPT_TEMPLATE}\n\nMCQ: ${truncate(mcqText)}`;
}

function cleanAndParseJSON(raw) {
  let t = (raw || '').trim()
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/```$/,'')
    .trim();
  const first = t.indexOf('{');
  const last = t.lastIndexOf('}');
  if (first === -1 || last === -1 || last < first) throw new Error('No JSON object found');
  t = t.slice(first, last + 1);
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

/* -------- OpenAI Call -------- */
async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: GEN_MODEL,
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

/* -------- Locking & Claiming -------- */
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - GEN_LOCK_TTL_MIN * 60 * 1000).toISOString();

  // Release stale locks
  await supabase
    .from('mcq_bank')
    .update({ primary_canonical_lock: null, primary_canonical_locked_at: null })
    .lt('primary_canonical_locked_at', cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from('mcq_bank')
    .select('id')
    .not('primary_mcq', 'is', null)
    .is('primary_canonical', null)
    .or(`primary_canonical_lock.is.null,primary_canonical_locked_at.lt.${cutoff}`)
    .order('id', { ascending: true })
    .limit(limit * 3);
  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  const { data: locked, error: e2 } = await supabase
    .from('mcq_bank')
    .update({
      primary_canonical_lock: WORKER_ID,
      primary_canonical_locked_at: new Date().toISOString()
    })
    .in('id', ids)
    .is('primary_canonical', null)
    .is('primary_canonical_lock', null)
    .select('id, primary_mcq');
  if (e2) throw e2;

  return (locked || []).slice(0, limit);
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from('mcq_bank')
    .update({ primary_canonical_lock: null, primary_canonical_locked_at: null })
    .in('id', ids);
}

/* -------- Per-Row Processor -------- */
async function processRow(row) {
  try {
    const prompt = buildPrompt(row);
    const raw = await callOpenAI([{ role: 'user', content: prompt }]);
    const parsed = cleanAndParseJSON(raw);

    const { error: upErr } = await supabase
      .from('mcq_bank')
      .update({
        primary_canonical: parsed,
        primary_canonical_lock: null,
        primary_canonical_locked_at: null
      })
      .eq('id', row.id);
    if (upErr) throw upErr;

    return { ok: true, id: row.id };
  } catch (e) {
    await clearLocks([row.id]);
    return { ok: false, id: row.id, error: e.message || String(e) };
  }
}

/* -------- Main Loop -------- */
(async function main() {
  console.log(`🧵 Canonical Worker ${WORKER_ID} | model=${GEN_MODEL} | limit=${GEN_LIMIT} | conc=${GEN_CONCURRENCY} | ttl=${GEN_LOCK_TTL_MIN}m`);
  while (true) {
    try {
      const claimed = await claimRows(GEN_LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_EMPTY_MS);
        continue;
      }
      console.log(`⚙️ claimed=${claimed.length}`);
      const results = await asyncPool(GEN_CONCURRENCY, claimed, r => processRow(r));
      const ok = results.filter(r => r.status === 'fulfilled' && r.value?.ok).length;
      const fail = results.length - ok;
      console.log(`✅ ok=${ok} ❌ fail=${fail}`);
    } catch (e) {
      console.error('Loop error:', e.message || e);
      await sleep(1000);
    }
  }
})();
