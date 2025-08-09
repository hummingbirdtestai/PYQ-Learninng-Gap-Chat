require('dotenv').config();
const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

// ---- Settings (env overrideable) ----
const GEN_MODEL          = process.env.GEN_MODEL || 'gpt-5-mini';
const GEN_LIMIT          = parseInt(process.env.GEN_LIMIT || '40', 10);      // rows per loop
const GEN_CONCURRENCY    = parseInt(process.env.GEN_CONCURRENCY || '4', 10); // parallel OpenAI calls
const GEN_LOCK_TTL_MIN   = parseInt(process.env.GEN_LOCK_TTL_MIN || '15', 10);
const SLEEP_EMPTY_MS     = parseInt(process.env.GEN_LOOP_SLEEP_MS || '750', 10);
const WORKER_ID          = process.env.WORKER_ID || `w-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---- Your existing prompt template (verbatim) ----
const PROMPT_TEMPLATE = `🚨 OUTPUT RULES: Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

🔬 You are an expert medical educator and exam learning strategist.
🎯 Your role is to act as a *Learning Gap Diagnostician* for MBBS/MD aspirants preparing for FMGE, NEETPG, INICET, or USMLE.
🧠 OBJECTIVE:
You will be given a *Previous Year Question (PYQ)* MCQ that a student got wrong. Your task is to:

1. Reframe the MCQ as a clinical vignette with *exactly 5 full sentences*, USMLE-style.  
   - The MCQ stem must resemble Amboss/NBME/USMLE-level difficulty.  
   - Bold all *high-yield keywords* using <strong>...</strong>.  
   - If an image is mentioned or implied but not provided, imagine a *relevant clinical/anatomical image* and incorporate its findings logically into the stem.

2. Provide 4 answer options (A–D), with one correct answer clearly marked.

3. Identify the *key learning gap* if the MCQ was answered wrong.
   - The learning gap statement must be *one sentence*, and include <strong>bolded keywords</strong> for the missed concept.

4. Provide 10 *high-quality, laser-sharp, buzzword-style facts* related to the concept of the current MCQ:
   - Each fact must be *8 to 12 words long*, maximum of one sentence.
   - Start with a relevant *emoji*.
   - Bold key terms using <strong>...</strong>.
   - Format as flat strings in a "buzzwords": [] array.
   - Style should match Amboss/NBME/USMLE exam revision quality — *concise, specific, exam-sure*.

5. Output a single JSON object:
   - "primary_mcq" → for the initial MCQ
   - "learning_gap" → for the missed concept
   - "buzzwords" → for revision

💡 Notes:
All "stem" and "learning_gap" values must contain 2 or more <strong>...</strong> terms.
If the original MCQ implies an image (e.g., anatomy, CT scan, fundus, histo slide), describe it logically in sentence 5 of the MCQ stem.
All "buzzwords" must be 10 high-yield, bolded HTML-formatted one-liners, each starting with an emoji.
`;

// ---- Helpers ----
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function truncate(s, n = 1000) { s = String(s || ''); return s.length > n ? s.slice(0, n) + ' …' : s; }

function buildPrompt(row) {
  const mcqText = typeof row.mcq === 'string'
    ? row.mcq
    : (row.mcq?.stem || row.mcq?.question || row.mcq?.text || JSON.stringify(row.mcq));
  return `${PROMPT_TEMPLATE}\n\nMCQ: ${truncate(mcqText)}\nCorrect Answer: ${row.correct_answer || ''}`;
}

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
  const ret = []; const exec = [];
  for (const it of items) {
    const p = Promise.resolve().then(() => iter(it));
    ret.push(p);
    const e = p.then(() => exec.splice(exec.indexOf(e), 1));
    exec.push(e);
    if (exec.length >= limit) await Promise.race(exec);
  }
  return Promise.allSettled(ret);
}

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|rate limit|temporar|unavailable|ECONNRESET/i.test(s);
}

// ---- OpenAI call (no temperature / no max_tokens for compatibility) ----
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

// ---- Locking & claiming ----
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - GEN_LOCK_TTL_MIN * 60 * 1000).toISOString();

  // 1) candidates
  const { data: candidates, error: e1 } = await supabase
    .from('mcq_bank')
    .select('id')
    .is('primary_mcq', null)
    .or(`primary_lock.is.null,primary_locked_at.lt.${cutoff}`)
    .order('id', { ascending: true })
    .limit(limit * 3);
  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  // 2) lock & fetch
  const { data: locked, error: e2 } = await supabase
    .from('mcq_bank')
    .update({ primary_lock: WORKER_ID, primary_locked_at: new Date().toISOString() })
    .in('id', ids)
    .is('primary_mcq', null)
    .is('primary_lock', null)
    .select('id, mcq, correct_answer');
  if (e2) throw e2;

  return (locked || []).slice(0, limit);
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from('mcq_bank')
    .update({ primary_lock: null, primary_locked_at: null })
    .in('id', ids);
}

// ---- Per-row processor ----
async function processRow(row) {
  try {
    const prompt = buildPrompt(row);
    const raw = await callOpenAI([{ role: 'user', content: prompt }]);
    const parsed = cleanAndParseJSON(raw);

    if (!parsed.primary_mcq || !parsed.learning_gap || !Array.isArray(parsed.buzzwords)) {
      throw new Error('Missing required fields in JSON');
    }

    const { error: upErr } = await supabase
      .from('mcq_bank')
      .update({
        primary_mcq: parsed,
        primary_lock: null,             // success → clear lock
        primary_locked_at: null
      })
      .eq('id', row.id);
    if (upErr) throw upErr;

    return { ok: true, id: row.id };
  } catch (e) {
    // failure → also clear lock so another attempt can retry later
    await supabase
      .from('mcq_bank')
      .update({ primary_lock: null, primary_locked_at: null })
      .eq('id', row.id);
    return { ok: false, id: row.id, error: e.message || String(e) };
  }
}

// ---- Main loop ----
(async function main() {
  console.log(`🧵 Primary Worker ${WORKER_ID} | model=${GEN_MODEL} | limit=${GEN_LIMIT} | conc=${GEN_CONCURRENCY} | ttl=${GEN_LOCK_TTL_MIN}m`);

  while (true) {
    try {
      const claimed = await claimRows(GEN_LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_EMPTY_MS);
        continue;
      }

      console.log(`⚙️  claimed=${claimed.length}`);
      const results = await asyncPool(GEN_CONCURRENCY, claimed, r => processRow(r));

      const ok = results.filter(r => r.status === 'fulfilled' && r.value?.ok).length;
      const fail = results.length - ok;
      console.log(`✅ ok=${ok}  ❌ fail=${fail}`);

      // Just in case: clear any still-locked failures
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
