const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

// ✅ Subject list (exact spellings) and UUID map
const SUBJECTS = [
  "Anatomy","Physiology","Biochemistry","Pathology","Pharmacology","Microbiology",
  "Forensic Medicine","Community Medicine","ENT","Ophthalmology",
  "General Medicine","Pediatrics","Dermatology","Psychiatry",
  "General Surgery","Orthopedics","Anesthesia","Radiology","Obstetrics and Gynaecology"
];

const SUBJECT_NAME_TO_ID = {
  "Anatomy": "2884ede5-ebdc-4dd7-be0d-cce07fd54c05",
  "Physiology": "cde00973-d19a-4ad2-9683-08f79f219603",
  "Biochemistry": "4df2c8b9-d9c0-480c-aecc-61563fa616ce",
  "Pathology": "aed84226-be65-43a3-bae9-06ab57ab48bf",
  "Pharmacology": "59665c8f-6277-4e17-9c92-18257d2fc1f2",
  "Microbiology": "5d3c2d0a-8718-4964-baaa-7ffc664f072c",
  "Forensic Medicine": "6b9abcfa-9ac0-4930-a42f-f390c6be04c4",
  "Community Medicine": "78f794bb-90b5-4bb5-b5d6-d57381175e52",
  "ENT": "4e5a1bd4-18de-4975-bed3-28d428cda51c",
  "Ophthalmology": "be03e32f-c62a-431b-97d8-88be27a24175",
  "General Medicine": "3cd6242a-51be-4e93-98f2-b42268a8175a",
  "Pediatrics": "6268d53e-9ed5-45ae-833b-59dd88b3af74",
  "Dermatology": "832bb0b0-30ef-453d-914b-1484ad455959",
  "Psychiatry": "2f599bf5-e471-4705-b183-55e44bd93c92",
  "General Surgery": "aebf4b8b-446d-4a67-a325-ee8d1c7f05ca",
  "Orthopedics": "fbbafd10-ba0f-4113-ad24-8192f40aa60d",
  "Anesthesia": "ebc4ef0f-46dd-4a4e-acca-1e53c7d6f127",
  "Radiology": "fb0c84ba-9eb1-4274-b4ef-2af5ce56cff5",
  "Obstetrics and Gynaecology": "8c9c6b8c-bd2f-404b-8e58-d5a7a722650b"
};

// Tune safely first; then scale up.
const BATCH_SIZE = parseInt(process.env.CLASSIFY_BATCH_SIZE || '50', 10);     // MCQs per LLM call
const CONCURRENCY = parseInt(process.env.CLASSIFY_CONCURRENCY || '5', 10);    // parallel LLM calls
const PAGE_SIZE   = parseInt(process.env.CLASSIFY_PAGE_SIZE || '2000', 10);   // DB page size
const MODEL       = process.env.CLASSIFY_MODEL || 'gpt-4o-mini';              // pick whatever you have quota for

const CLASSIFICATION_PROMPT = `
You are a meticulous classifier for MBBS exam MCQs.

Pick exactly ONE subject for each MCQ from this closed list (case-sensitive, exact match):
${SUBJECTS.map(s => `- ${s}`).join('\n')}

OUTPUT RULES:
- Return ONLY a single JSON array, parsable by JSON.parse.
- Each item must be: {"id": <number>, "subject": "<one of the above exactly>"}.
- Do not add text before or after the JSON.
- If ambiguous, choose the most probable.
- Never invent subjects outside the list.
`.trim();

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function asyncPool(limit, array, iteratorFn) {
  const ret = [];
  const executing = [];
  for (const item of array) {
    const p = Promise.resolve().then(() => iteratorFn(item));
    ret.push(p);
    if (limit <= array.length) {
      const e = p.then(() => executing.splice(executing.indexOf(e), 1));
      executing.push(e);
      if (executing.length >= limit) {
        await Promise.race(executing);
      }
    }
  }
  return Promise.allSettled(ret);
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function classifyChunk(mcqs, attempt = 1) {
  const payload = {
    mcqs: mcqs.map(r => ({
      id: r.id,
      text:
        r.mcq?.stem ||
        r.mcq?.question ||
        r.mcq?.text ||
        (typeof r.mcq === 'string' ? r.mcq : '') ||
        '',
      options: r.mcq?.options || null,
      correct_answer: r.mcq?.correct_answer || r.mcq?.answer || null
    }))
  };

  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      temperature: 0,
      messages: [
        { role: 'system', content: CLASSIFICATION_PROMPT },
        { role: 'user', content: JSON.stringify(payload) }
      ]
    });

    const raw = resp.choices?.[0]?.message?.content?.trim() || '[]';
    let parsed;
    try {
      parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) throw new Error('Model did not return an array');
    } catch (e) {
      if (attempt < 3) { await sleep(300 * attempt); return classifyChunk(mcqs, attempt + 1); }
      throw new Error(`Invalid JSON from model: ${raw.slice(0, 240)}`);
    }

    // Validate & map to UUID
    const valid = parsed
      .filter(it => typeof it?.id === 'number' && typeof it?.subject === 'string' && SUBJECTS.includes(it.subject))
      .map(it => ({
        id: it.id,
        subject: it.subject,
        subject_id: SUBJECT_NAME_TO_ID[it.subject] || null
      }))
      .filter(it => it.subject_id); // ensure we actually have a UUID

    if (!valid.length) {
      if (attempt < 3) { await sleep(300 * attempt); return classifyChunk(mcqs, attempt + 1); }
      throw new Error('No valid classifications returned from model');
    }

    // Bulk upsert id + subject + subject_id
    const { error: upErr } = await supabase
      .from('mcq_bank')
      .upsert(valid, { onConflict: 'id', ignoreDuplicates: false });

    if (upErr) throw upErr;

    return { ok: true, count: valid.length, invalid: parsed.length - valid.length };
  } catch (err) {
    return { ok: false, error: err.message || String(err) };
  }
}

/**
 * POST /classify/subjects/run
 * Body (optional): { batchSize, concurrency, pageSize }
 * Runs batched, parallel classification for rows where subject_id IS NULL.
 */
exports.classifySubjectsRun = async (req, res) => {
  const batchSize = Math.min(parseInt(req.body?.batchSize || BATCH_SIZE, 10), 200);
  const concurrency = Math.min(parseInt(req.body?.concurrency || CONCURRENCY, 10), 12);
  const pageSize = Math.min(parseInt(req.body?.pageSize || PAGE_SIZE, 10), 5000);

  let totalUpdated = 0, totalInvalid = 0, pages = 0;

  try {
    while (true) {
      // Pull a page of unclassified rows (prefer subject_id null check)
      const { data: rows, error: fetchErr } = await supabase
        .from('mcq_bank')
        .select('id, mcq')
        .is('subject_id', null)
        .order('id', { ascending: true })
        .limit(pageSize);

      if (fetchErr) throw fetchErr;
      if (!rows || rows.length === 0) break;

      pages += 1;
      const chunks = chunk(rows, batchSize);

      const results = await asyncPool(concurrency, chunks, c => classifyChunk(c));

      for (const r of results) {
        if (r.status === 'fulfilled' && r.value?.ok) {
          totalUpdated += r.value.count || 0;
          totalInvalid += r.value.invalid || 0;
        } else if (r.status === 'fulfilled') {
          console.error('🔁 Chunk failed:', r.value?.error);
        } else {
          console.error('💥 Chunk promise rejected:', r.reason);
        }
      }

      // brief breather
      await sleep(150);
    }

    return res.json({
      message: '✅ Classification complete',
      pages,
      totalUpdated,
      totalInvalidReturned: totalInvalid,
      batchSize,
      concurrency,
      pageSize,
      model: MODEL
    });
  } catch (err) {
    console.error('❌ classifySubjectsRun error:', err);
    return res.status(500).json({ error: err.message || 'Internal error' });
  }
};
