// workers/finalChapterWorker.js
require('dotenv').config();
const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

// ---------- Settings ----------
const MODEL              = process.env.CLASSIFY_MODEL || "gpt-5-mini";
const LIMIT              = parseInt(process.env.FINAL_LIMIT || "180", 10);
const BLOCK_SIZE         = parseInt(process.env.FINAL_BLOCK_SIZE || "60", 10);
const SLEEP_MS           = parseInt(process.env.FINAL_SLEEP_MS || "800", 10);
const LOCK_TTL_MIN       = parseInt(process.env.FINAL_LOCK_TTL_MIN || "15", 10);
const WORKER_ID          = process.env.WORKER_ID || `final-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt Builder ----------
function buildPrompt(items) {
  const header = `
You are a medical textbook expert and data normalizer.

You will be given two text fields: "chapter" and "topic".
Decide the correct text for the "final_chapter" column using these rules:

1. If "chapter" and "topic" are exactly the same, output the chapter text.
2. If they are not the same, but the topic clearly falls under that chapter in standard medical textbooks, output the chapter text.
3. If they are not the same, and the topic does not fall under that chapter, output the topic text.

⚠️ Very important:
- Output only the exact chosen text, nothing else.
- Do not rephrase, wrap, or explain.
- Do not output JSON or labels.
- The output must be a single string, exactly as it should be stored in the "final_chapter" column.
`.trim();

  const body = items
    .map((it, i) => `${i + 1}) chapter: "${it.chapter}" | topic: "${it.topic}"`)
    .join("\n\n");

  return `${header}\n\nInputs:\n\n${body}\n\nRemember: output exactly ${items.length} lines, one per input.`;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|rate limit|temporar|unavailable|ECONNRESET/i.test(s);
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

// ---------- Locking & Claim ----------
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // free stale locks
  await supabase
    .from('learning_gap_vertical')
    .update({ final_chapter_lock: null, final_chapter_lock_at: null })
    .is('final_chapter', null)
    .lt('final_chapter_lock_at', cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from('learning_gap_vertical')
    .select('id, chapter, topic')
    .is('final_chapter', null)
    .not('chapter', 'is', null)
    .not('topic', 'is', null)
    .order('id', { ascending: true })
    .limit(limit);
  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  const { data: locked, error: e2 } = await supabase
    .from('learning_gap_vertical')
    .update({ final_chapter_lock: WORKER_ID, final_chapter_lock_at: new Date().toISOString() })
    .in('id', ids)
    .is('final_chapter', null)
    .is('final_chapter_lock', null)
    .select('id, chapter, topic');
  if (e2) throw e2;

  console.log(`🔎 candidates=${candidates.length}, locked=${locked?.length}`);
  return (locked || []).slice(0, limit);
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from('learning_gap_vertical')
    .update({ final_chapter_lock: null, final_chapter_lock_at: null })
    .in('id', ids);
}

// ---------- Process one block ----------
async function processBlock(block) {
  const prompt = buildPrompt(block);
  const raw = await callOpenAI([{ role: 'user', content: prompt }]);

  const lines = (raw || '')
    .trim()
    .replace(/^```.*?\n|\n```$/g, '')
    .split(/\r?\n/)
    .map(l => l.replace(/^\d+[\).\s-]+/, '').trim())
    .filter(Boolean);

  const updates = [];
  for (let i = 0; i < block.length && i < lines.length; i++) {
    if (lines[i]) {
      updates.push({ id: block[i].id, final_chapter: lines[i] });
    }
  }

  if (updates.length) {
    for (const u of updates) {
      const { error: upErr } = await supabase
        .from('learning_gap_vertical')
        .update({ final_chapter: u.final_chapter })
        .eq('id', u.id);
      if (upErr) throw upErr;
    }
  }

  await clearLocks(block.map(r => r.id));

  return { updated: updates.length, total: block.length };
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧵 Final Chapter Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | block=${BLOCK_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);
      let updated = 0;
      for (let i = 0; i < claimed.length; i += BLOCK_SIZE) {
        const block = claimed.slice(i, i + BLOCK_SIZE);
        try {
          const r = await processBlock(block);
          updated += r.updated;
          console.log(`   block ${i / BLOCK_SIZE + 1}: updated ${r.updated}/${r.total}`);
        } catch (e) {
          console.error('   block error:', e.message || e);
          await clearLocks(block.map(r => r.id));
        }
      }

      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error('Loop error:', e.message || e);
      await sleep(1000);
    }
  }
})();
