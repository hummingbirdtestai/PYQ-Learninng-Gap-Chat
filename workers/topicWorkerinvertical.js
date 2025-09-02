// workers/topicWorkerinvertical.js
require('dotenv').config();
const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

// ---------- Settings ----------
const TOPIC_MODEL       = process.env.TOPIC_MODEL || "gpt-5-mini";
const TOPIC_LIMIT       = parseInt(process.env.TOPIC_LIMIT || "180", 10);
const TOPIC_BLOCK_SIZE  = parseInt(process.env.TOPIC_BLOCK_SIZE || "60", 10);
const TOPIC_SLEEP_MS    = parseInt(process.env.TOPIC_LOOP_SLEEP_MS || "800", 10);
const TOPIC_LOCK_TTL_MIN = parseInt(process.env.TOPIC_LOCK_TTL_MIN || "15", 10);
const WORKER_ID         = process.env.WORKER_ID || `lg-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

const TOPICS = [
  "Nephron","Hypoxia","Blood Flow Regulation","Spirometry","Oxygen Dissociation Curve","BP",
  "Exercise Physiology","Long Tracts","Nerve Conduction","Action Potential","Cardiac Conduction",
  "Altitude Physiology","EEG","Ovulation","Temperature Regulation","Calcitonin","Insulin",
  "Peristalsis","Renin Angiotensin System","Pancreas","Acidosis","Coagulation","Erythropoiesis",
  "Second Messenger","Surfactant","Autonomic Nervous System","Cardiac Cycle","ADH",
  "Nerve Fibre Classification","Parathormone","Basal Ganglia","Bilirubin Metabolism","Cerebellum",
  "Electrolytes","Pituitary","Sensory Receptors","Skeltal Muscle Physiology","Sodium Potassium Pump",
  "Spermatogenesis","V/Q Ratio","A-a O2","ECF","Epinephrine","Hypothalamus","Respiratory Cycle",
  "Shock","Small Intestine Absorption","Smooth Muscle Contraction","Thyroid","Visual Pathway",
  "Respiration Regulation","Cardiac Muscle Contraction","Cell Membrane","Cholecystokinin",
  "CO2 Transport","CSF","Diffusion","Edema","Hemoglobin","Histamine"
];

// ---------- Helpers ----------
function extractStem(mcqJson) {
  if (!mcqJson) return '';
  if (typeof mcqJson === 'string') return mcqJson;
  if (typeof mcqJson === 'object') return mcqJson.stem || mcqJson.question || mcqJson.text || JSON.stringify(mcqJson);
  return String(mcqJson);
}

const truncate = (s, n = 600) =>
  (String(s || '').length > n ? String(s).slice(0, n) + ' …' : String(s || ''));

function buildPrompt(items) {
  const header = `
You are an expert physiology teacher. Classify each MCQ into EXACTLY one topic.

Use ONLY these exact topics (no synonyms, no new topics):
${TOPICS.map(t => `- ${t}`).join('\n')}

Return format:
- Output EXACTLY ${items.length} LINES.
- Each line = one topic (from the list above), in the same order as the MCQs.
- No numbering, no extra words.
`.trim();

  const body = items.map((it, i) =>
    `${i + 1}) ${truncate(extractStem(it.mcq_json))}`).join('\n\n');

  return `${header}\n\nMCQs:\n\n${body}\n\nRemember: output exactly ${items.length} lines, one topic per line.`;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|rate limit|temporar|unavailable|ECONNRESET/i.test(s);
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: TOPIC_MODEL,
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
  const cutoff = new Date(Date.now() - TOPIC_LOCK_TTL_MIN * 60 * 1000).toISOString();

  await supabase
    .from('learning_gap_vertical')
    .update({ chapter_lock: null, chapter_lock_at: null })
    .eq('subject_name', 'Physiology')
    .is('chapter', null)
    .lt('chapter_lock_at', cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from('learning_gap_vertical')
    .select('id')
    .eq('subject_name', 'Physiology')
    .is('chapter', null)
    .order('id', { ascending: true })
    .limit(limit * 3);
  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  const { data: locked, error: e2 } = await supabase
    .from('learning_gap_vertical')
    .update({ chapter_lock: WORKER_ID, chapter_lock_at: new Date().toISOString() })
    .in('id', ids)
    .is('chapter', null)
    .is('chapter_lock', null)
    .select('id, mcq_json');
  if (e2) throw e2;
  return (locked || []).slice(0, limit);
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from('learning_gap_vertical')
    .update({ chapter_lock: null, chapter_lock_at: null })
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
    const topic = TOPICS.find(t => t.toLowerCase() === lines[i].toLowerCase());
    if (topic) updates.push({ id: block[i].id, chapter: topic });
  }

  if (updates.length) {
    for (const u of updates) {
      const { error: upErr } = await supabase
        .from('learning_gap_vertical')
        .update({ chapter: u.chapter })
        .eq('id', u.id);
      if (upErr) throw upErr;
    }
  }

  await clearLocks(block.map(r => r.id));

  return { updated: updates.length, total: block.length };
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧵 Topic Worker ${WORKER_ID} | model=${TOPIC_MODEL} | claim=${TOPIC_LIMIT} | block=${TOPIC_BLOCK_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(TOPIC_LIMIT);
      if (!claimed.length) {
        await sleep(TOPIC_SLEEP_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);
      let updated = 0;
      for (let i = 0; i < claimed.length; i += TOPIC_BLOCK_SIZE) {
        const block = claimed.slice(i, i + TOPIC_BLOCK_SIZE);
        try {
          const r = await processBlock(block);
          updated += r.updated;
          console.log(`   block ${i / TOPIC_BLOCK_SIZE + 1}: updated ${r.updated}/${r.total}`);
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
