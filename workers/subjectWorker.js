require('dotenv').config();
const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

// ---------- Settings ----------
const SUBJECT_MODEL        = process.env.CLASSIFY_MODEL || 'gpt-5-mini';
const SUBJECT_LIMIT        = parseInt(process.env.SUBJECT_LIMIT || '180', 10);   // rows to claim per loop
const SUBJECT_BLOCK_SIZE   = parseInt(process.env.SUBJECT_BLOCK_SIZE || '60', 10); // rows per LLM call
const SUBJECT_LOCK_TTL_MIN = parseInt(process.env.SUBJECT_LOCK_TTL_MIN || '15', 10);
const SUBJECT_SLEEP_MS     = parseInt(process.env.SUBJECT_LOOP_SLEEP_MS || '800', 10);
const WORKER_ID            = process.env.WORKER_ID || `subj-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

const SUBJECTS = [
  "Anatomy","Physiology","Biochemistry","Pathology","Pharmacology","Microbiology",
  "Forensic Medicine","Community Medicine","ENT","Ophthalmology",
  "General Medicine","Pediatrics","Dermatology","Psychiatry",
  "General Surgery","Orthopedics","Anesthesia","Radiology","Obstetrics and Gynaecology"
];
const SUBJECT_SYNONYMS = {
  "immunology":"Microbiology","genetics":"Biochemistry","public health":"Community Medicine","psm":"Community Medicine","spm":"Community Medicine",
  "radiodiagnosis":"Radiology","radiodiagnostics":"Radiology","internal medicine":"General Medicine","medicine":"General Medicine",
  "surgery":"General Surgery","obg":"Obstetrics and Gynaecology","ob&g":"Obstetrics and Gynaecology","obstetrics & gynaecology":"Obstetrics and Gynaecology",
  "obstetrics and gynecology":"Obstetrics and Gynaecology","gynecology":"Obstetrics and Gynaecology","ophthal":"Ophthalmology","ophthalmic":"Ophthalmology",
  "eye":"Ophthalmology","ent":"ENT","psych":"Psychiatry","derm":"Dermatology","peds":"Pediatrics","ortho":"Orthopedics","anesthesiology":"Anesthesia",
  "micro":"Microbiology","biochem":"Biochemistry","pharma":"Pharmacology"
};

function normalizeSubject(s) {
  if (!s) return null;
  s = String(s).trim();
  if (SUBJECTS.includes(s)) return s;
  const key = s.toLowerCase();
  if (SUBJECT_SYNONYMS[key]) return SUBJECT_SYNONYMS[key];
  const simplified = key.replace(/[&.]/g, '').replace(/\s+/g, ' ').trim();
  for (const sub of SUBJECTS) if (sub.toLowerCase() === simplified) return sub;
  return null;
}

function extractPrimaryStem(primary) {
  if (!primary) return '';
  const pm = primary?.primary_mcq || primary || {};
  return pm.stem || primary?.stem || '';
}
function extractMCQText(row) {
  const fromPrimary = extractPrimaryStem(row.primary_mcq);
  if (fromPrimary) return fromPrimary;
  const mcq = row.mcq;
  if (!mcq) return '';
  if (typeof mcq === 'string') return mcq;
  if (typeof mcq === 'object') return mcq.stem || mcq.question || mcq.text || JSON.stringify(mcq);
  return String(mcq);
}
const truncate = (s, n = 600) => (String(s || '').length > n ? String(s).slice(0, n) + ' …' : String(s || ''));

function buildPrompt(items) {
  const header = `
You classify MBBS MCQs into subjects.

Use ONLY these exact subjects:
${SUBJECTS.map(s => `- ${s}`).join('\n')}

Return format:
- Output EXACTLY ${items.length} LINES.
- Each line is ONE subject from the list, matching the order of the MCQs given.
- No numbering, no IDs, no extra text before/after. Just the subject names, one per line.

Map related terms as:
- "Immunology" → Microbiology
- "Genetics" → Biochemistry
- "Public Health/PSM/SPM" → Community Medicine
- "Radiodiagnosis/…diagnostics" → Radiology
- "Internal Medicine/Medicine" → General Medicine
- "Surgery" (broad) → General Surgery
- "OBG/OB & G/Obstetrics & Gynecology" → Obstetrics and Gynaecology
- "Ophthal/Ophthalmic/Eye" → Ophthalmology
- "Psych" → Psychiatry, "Derm" → Dermatology, "Peds" → Pediatrics, "Ortho" → Orthopedics,
  "Anesthesiology" → Anesthesia, "Micro" → Microbiology, "Biochem" → Biochemistry, "Pharma" → Pharmacology.
`.trim();

  const body = items.map((it, i) => `${i + 1}) ${truncate(extractMCQText(it))}`).join('\n\n');
  return `${header}\n\nMCQs:\n\n${body}\n\nRemember: output exactly ${items.length} lines, one subject per line.`;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|rate limit|temporar|unavailable|ECONNRESET/i.test(s);
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: SUBJECT_MODEL,
      temperature: 0,
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
// Requires: subject_lock text, subject_locked_at timestamptz (see SQL below)
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - SUBJECT_LOCK_TTL_MIN * 60 * 1000).toISOString();

  // free stale locks (only for still-unclassified)
  await supabase
    .from('mcq_bank')
    .update({ subject_lock: null, subject_locked_at: null })
    .is('subject', null)
    .lt('subject_locked_at', cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from('mcq_bank')
    .select('id')
    .is('subject', null)
    .or(`subject_lock.is.null,subject_locked_at.lt.${cutoff}`)
    .order('id', { ascending: true })
    .limit(limit * 3);
  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  const { data: locked, error: e2 } = await supabase
    .from('mcq_bank')
    .update({ subject_lock: WORKER_ID, subject_locked_at: new Date().toISOString() })
    .in('id', ids)
    .is('subject', null)
    .is('subject_lock', null)
    .select('id, mcq, primary_mcq');

  if (e2) throw e2;
  return (locked || []).slice(0, limit);
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from('mcq_bank')
    .update({ subject_lock: null, subject_locked_at: null })
    .in('id', ids);
}

// ---- Process a block with one LLM call ----
async function processBlock(block) {
  const prompt = buildPrompt(block);
  const raw = await callOpenAI([{ role: 'user', content: prompt }]);

  const lines = (raw || '')
    .trim()
    .replace(/^```.*?\n|\n```$/g, '')
    .split(/\r?\n/)
    .map(l => l.replace(/^\d+[\).\s-]+/, '').trim())
    .filter(Boolean);

  // Map by position
  const updates = [];
  for (let i = 0; i < block.length && i < lines.length; i++) {
    const sub = normalizeSubject(lines[i]);
    if (sub) updates.push({ id: block[i].id, subject: sub });
  }

  // Persist subjects + clear locks
  if (updates.length) {
    const { error: upErr } = await supabase
      .from('mcq_bank')
      .upsert(updates, { onConflict: 'id', ignoreDuplicates: false });
    if (upErr) throw upErr;
  }

  // Clear locks for everything in this block (even if no valid subject -> retry later)
  await clearLocks(block.map(r => r.id));

  return { updated: updates.length, total: block.length };
}

// ---- Main loop ----
(async function main() {
  console.log(`🧵 Subject Worker ${WORKER_ID} | model=${SUBJECT_MODEL} | claim=${SUBJECT_LIMIT} | block=${SUBJECT_BLOCK_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(SUBJECT_LIMIT);
      if (!claimed.length) {
        await sleep(SUBJECT_SLEEP_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);

      // slice into blocks to keep prompts safe
      let updated = 0;
      for (let i = 0; i < claimed.length; i += SUBJECT_BLOCK_SIZE) {
        const block = claimed.slice(i, i + SUBJECT_BLOCK_SIZE);
        try {
          const r = await processBlock(block);
          updated += r.updated;
          console.log(`   block ${i / SUBJECT_BLOCK_SIZE + 1}: updated ${r.updated}/${r.total}`);
        } catch (e) {
          console.error('   block error:', e.message || e);
          // best effort unlock if any issue
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
