// 🎯 Final Prompt Template
const { v4: uuidv4 } = require('uuid');
const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

/**const PROMPT_TEMPLATE = `🚨 OUTPUT RULES:
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

🔬 You are an expert medical educator and exam learning strategist.
🎯 Your role is to act as a **Learning Gap Diagnostician** for MBBS/MD aspirants preparing for FMGE, NEETPG, INICET, or USMLE.
🧠 OBJECTIVE:
You will be given a **Previous Year Question (PYQ)** MCQ that a student got wrong. Your task is to:

1. Reframe the MCQ as a clinical vignette with **exactly 5 full sentences**, USMLE-style.  
2. Provide 5 answer options (A–E), with one correct answer clearly marked.
3. Identify the **key learning gap** in one sentence with <strong>bolded keywords</strong>.
4. Give 10 concise **buzzword-style facts** with <strong>bolded terms</strong> and emoji prefixes.
5. Then generate a chain of **3 recursive MCQs** (Level 1 → Level 3), each based on the previous level’s learning gap.

📤 Final Output Format:
{
  "primary_mcq": { ... },
  "recursive_levels": [ { ... }, { ... }, { ... } ]
}`;
**/



const validatePrimaryMCQ = (mcq) => {
  if (!mcq?.stem || typeof mcq.stem !== 'string') throw new Error("Primary MCQ: Missing or invalid 'stem'");
  if (!mcq?.correct_answer || typeof mcq.correct_answer !== 'string') throw new Error("Primary MCQ: Missing or invalid 'correct_answer'");
  const optionKeys = Object.keys(mcq.options || {});
  if (optionKeys.length < 4 || optionKeys.length > 5) {
    throw new Error(`Primary MCQ: Must have 4 or 5 options, found ${optionKeys.length}`);
  }
  return true;
};

const validateRecursiveMCQ = (mcq) => {
  const requiredKeys = ['A', 'B', 'C', 'D', 'E'];
  if (!mcq?.stem || typeof mcq.stem !== 'string') throw new Error("Recursive MCQ: Missing or invalid 'stem'");
  if (!mcq?.correct_answer || typeof mcq.correct_answer !== 'string') throw new Error("Recursive MCQ: Missing or invalid 'correct_answer'");
  if (!mcq?.options || typeof mcq.options !== 'object') throw new Error("Recursive MCQ: Missing or invalid 'options'");
  for (const key of requiredKeys) {
    const val = mcq.options[key];
    if (!val || typeof val !== 'string' || val.trim().length === 0) {
      throw new Error(`Recursive MCQ: Option '${key}' is missing or empty`);
    }
  }
  return true;
};

const insertMCQ = async (mcq, level, validateFn, subject_id) => {
  if (!validateFn(mcq)) throw new Error(`MCQ at level ${level} failed validation`);
  const id = uuidv4();
  const { error } = await supabase.from('mcqs').insert({
    id,
    subject_id,
    stem: mcq.stem,
    option_a: mcq.options?.A || null,
    option_b: mcq.options?.B || null,
    option_c: mcq.options?.C || null,
    option_d: mcq.options?.D || null,
    option_e: mcq.options?.E || null,
    correct_answer: mcq.correct_answer,
    explanation: mcq.explanation || '',
    learning_gap: mcq.learning_gap || '',
    level,
    mcq_json: mcq
  });
  if (error) throw error;
  return id;
};

exports.generateMCQGraphFromInput = async (req, res) => {
  const { raw_mcq_text, subject_id } = req.body;

  if (!raw_mcq_text || !subject_id) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  const fullPrompt = `${PROMPT_TEMPLATE}

Here is the MCQ:
${raw_mcq_text}

- The above contains the full MCQ as entered by a teacher.
- You must identify the question, extract options A–E, and detect the correct answer if present.
- Then follow all previous instructions to reframe it into the required JSON output.
- ⚠️ DO NOT return the object as a string.
- ❌ NO markdown, no headings, no \`\`\`json, no HTML wrapping.`;

  const maxAttempts = 3;
  let parsed = null;
  let lastRawOutput = '';

  const sanitizeJSON = (text) => {
    return text.trim()
      .replace(/^```(json)?/i, '')
      .replace(/```$/, '')
      .replace(/^"({[\s\S]*})"$/, '$1')
      .replace(/\\"/g, '"')
      .replace(/,\s*}/g, '}')
      .replace(/,\s*]/g, ']')
      .replace(/\\n/g, '')
      .replace(/\n/g, '')
      .replace(/\\t/g, '')
      .replace(/\\r/g, '');
  };

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const gptResponse = await openai.chat.completions.create({
        model: 'gpt-5', // updated to GPT-5
        messages: [{ role: 'user', content: fullPrompt }],
        temperature: 0.7
      });

      lastRawOutput = gptResponse.choices?.[0]?.message?.content || '';
      const cleanedOutput = sanitizeJSON(lastRawOutput);

      try {
        parsed = JSON.parse(cleanedOutput);
        if (typeof parsed === 'string') parsed = JSON.parse(parsed);
        break;
      } catch (err) {
        if (attempt === maxAttempts) {
          await supabase.from('mcq_generation_errors').insert({
            raw_input: raw_mcq_text,
            raw_output: lastRawOutput,
            reason: err.message,
            subject_id
          });
          return res.status(500).json({
            error: '❌ GPT output invalid',
            details: err.message,
            raw_output: lastRawOutput
          });
        }
      }
    } catch (gptErr) {
      if (attempt === maxAttempts) {
        return res.status(500).json({
          error: 'GPT API failed after 3 attempts',
          details: gptErr.message
        });
      }
    }
  }

  try {
    const { primary_mcq, recursive_levels } = parsed;
    if (!primary_mcq) throw new Error("Missing 'primary_mcq' in GPT response");

    const primaryId = await insertMCQ(primary_mcq, 0, validatePrimaryMCQ, subject_id);
    const recursiveIds = [];

    if (Array.isArray(recursive_levels)) {
      for (let i = 0; i < Math.min(3, recursive_levels.length); i++) {
        const level = i + 1;
        const mcq = recursive_levels[i];
        try {
          const id = await insertMCQ(mcq, level, validateRecursiveMCQ, subject_id);
          recursiveIds.push(id);
        } catch (err) {
          console.warn(`⚠️ Skipping recursive level ${level}: ${err.message}`);
        }
      }
    }

    await supabase.from('mcq_graphs').insert({
      subject_id,
      graph: {
        primary_mcq: primaryId,
        recursive_levels: recursiveIds
      },
      generated: true
    });

    return res.status(200).json({
      message: '✅ MCQ Graph generated',
      graph: {
        primary_mcq: primaryId,
        recursive_levels: recursiveIds
      }
    });
  } catch (err) {
    console.error('❌ Error saving MCQs:', err.message);
    return res.status(500).json({
      error: 'Failed to save MCQ graph',
      details: err.message
    });
  }
};

// 🛠️ Manual insertion endpoint
exports.insertMCQGraphFromJson = async (req, res) => {
  const { graph_json, exam_id, subject_id } = req.body;

  if (!graph_json || !exam_id || !subject_id) {
    return res.status(400).json({ error: 'Missing graph_json or exam/subject ID' });
  }

  try {
    const primaryId = await insertMCQ(graph_json.primary_mcq, 0, validatePrimaryMCQ, exam_id, subject_id);
    const recursiveIds = [];

    if (!Array.isArray(graph_json.recursive_levels)) {
      return res.status(400).json({ error: 'recursive_levels must be an array' });
    }

    for (let i = 0; i < graph_json.recursive_levels.length; i++) {
      const id = await insertMCQ(graph_json.recursive_levels[i], i + 1, validateRecursiveMCQ, exam_id, subject_id);
      recursiveIds.push(id);
    }

    const graph = {
      primary_mcq: primaryId,
      recursive_levels: recursiveIds
    };

    await supabase.from('mcq_graphs').insert({
      raw_mcq_id: null,
      exam_id,
      subject_id,
      graph,
      generated: false
    });

    return res.status(200).json({ message: '✅ MCQ graph inserted successfully', graph });
  } catch (err) {
    console.error('❌ Insertion Error:', err.message);
    return res.status(500).json({ error: 'Failed to insert MCQ graph', details: err.message });
  }
};

// ✅ New API Dad 1
exports.generateAndSaveGraphDraft = async (req, res) => {
  const { raw_text, subject_id } = req.body;

  if (!raw_text || !subject_id) {
    return res.status(400).json({ error: 'Missing raw_text or subject_id' });
  }

 /** const prompt = `
🚨 OUTPUT RULES:
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

🔬 You are an expert medical educator and exam learning strategist.
🎯 Your role is to act as a **Learning Gap Diagnostician** for MBBS/MD aspirants preparing for FMGE, NEETPG, INICET, or USMLE.
🧠 OBJECTIVE:
You will be given a **Previous Year Question (PYQ)** MCQ that a student got wrong. Your task is to:

1. Reframe the MCQ as a clinical vignette with **exactly 5 full sentences**, USMLE-style.  
   - The MCQ stem must resemble Amboss/NBME/USMLE-level difficulty.  
   - Bold all **high-yield keywords** using <strong>...</strong>.  
   - If an image is mentioned or implied but not provided, imagine a **relevant clinical/anatomical image** and incorporate its findings logically into the stem.

2. Provide 5 answer options (A–E), with one correct answer clearly marked.

3. Identify the **key learning gap** if the MCQ was answered wrong.
   - The learning gap statement must be **one sentence**, and include <strong>bolded keywords</strong> for the missed concept.

4. Provide 10 **high-quality, laser-sharp, buzzword-style facts** related to the concept of the current MCQ:
   - Each fact must be **8 to 12 words long**, maximum of one sentence.
   - Start with a relevant **emoji**.
   - Bold key terms using <strong>...</strong>.
   - Format as flat strings in a "buzzwords": [] array.
   - Style should match Amboss/NBME/USMLE exam revision quality — **concise, specific, exam-sure**.

5. Based on the identified learning gap, generate a new MCQ that tests **only that gap**.
   - Use the same format: 5 full sentences, A–E options, correct answer, learning gap, and 10 buzzword facts.
   - Each new level (Level 1 → Level 10) must recursively target the previous level’s learning gap.
   - Each MCQ must be meaningfully distinct and clinically rich, but directly tied to the chain of gaps.

6. Output a single JSON object:
   - "primary_mcq" → for the initial MCQ
   - "recursive_levels" → an array of 10 objects, Level 1 to Level 10

💡 Notes:
All "stem" and "learning_gap" values must contain 2 or more <strong>...</strong> terms.
If the original MCQ implies an image (e.g., anatomy, CT scan, fundus, histo slide), describe it logically in sentence 5 of the MCQ stem.
All "buzzwords" must be 10 high-yield, bolded HTML-formatted one-liners, each starting with an emoji.

Here is the MCQ:
"""${raw_text}"""
`;
**/

  try {
    const completion = await openai.chat.completions.create({
      model: 'gpt-5', // updated to GPT-5
      messages: [{ role: 'user', content: prompt }],
      temperature: 0.7,
    });

    const rawOutput = completion.choices[0]?.message?.content;

    const parsed = JSON.parse(rawOutput);

    const { error } = await supabase.from('mcq_graphs').insert({
      id: uuidv4(),
      subject_id,
      graph: parsed,
      generated: false,
    });

    if (error) throw error;

    return res.json({ message: '✅ Draft graph saved successfully.' });
  } catch (err) {
    console.error('❌ GPT or DB error:', err.message || err);
    return res.status(500).json({ error: 'Failed to generate or store MCQ graph.' });
  }
};

// ✅ New API Dad 2
exports.processGraphById = async (req, res) => {
  const { graphId } = req.params;

  try {
    // Step 1: Fetch the graph
    const { data: graphRow, error: fetchError } = await supabase
      .from('mcq_graphs')
      .select('id, subject_id, graph, generated')
      .eq('id', graphId)
      .single();

    if (fetchError || !graphRow) {
      return res.status(404).json({ error: 'Graph not found' });
    }

    if (graphRow.generated) {
      return res.status(400).json({ error: 'Graph already processed' });
    }

    const { subject_id, graph } = graphRow;

    // Step 2: Get exam_id from subject_id
    const { data: subjectRow, error: subjectError } = await supabase
      .from('subjects')
      .select('exam_id')
      .eq('id', subject_id)
      .single();

    if (subjectError || !subjectRow?.exam_id) {
      return res.status(400).json({ error: 'Invalid subject_id or exam_id missing' });
    }

    const exam_id = subjectRow.exam_id;

    const mcqsToInsert = [];

    // Step 3: Parse and prepare all MCQs
    const { primary_mcq, recursive_levels } = graph;

    const allMcqs = [{ ...primary_mcq, level: 0 }, ...(recursive_levels || []).map((mcq, idx) => ({ ...mcq, level: idx + 1 }))];

    for (const mcq of allMcqs) {
      const { stem, options, correct_answer, explanation, learning_gap, buzzwords } = mcq;

      if (!stem || !options || !correct_answer || !explanation) {
        console.warn('Skipping incomplete MCQ');
        continue;
      }

      mcqsToInsert.push({
        subject_id,
        exam_id,
        level: mcq.level,
        stem,
        option_a: options.A || '',
        option_b: options.B || '',
        option_c: options.C || '',
        option_d: options.D || '',
        option_e: options.E || null,
        correct_answer,
        explanation,
        learning_gap,
        mcq_json: { ...mcq, buzzwords }, // optional: store full GPT content
        mcq_graph_id: graphId,
      });
    }

    // Step 4: Bulk insert
    const { error: insertError } = await supabase.from('mcqs').insert(mcqsToInsert);

    if (insertError) {
      console.error('Error inserting MCQs:', insertError);
      return res.status(500).json({ error: 'Failed to insert MCQs' });
    }

    // Step 5: Mark graph as processed
    await supabase
      .from('mcq_graphs')
      .update({ generated: true })
      .eq('id', graphId);

    res.json({
      message: `✅ Processed ${mcqsToInsert.length} MCQs from graph.`,
      mcqs_inserted: mcqsToInsert.length,
    });
  } catch (err) {
    console.error('❌ Server error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
};

// CLASSIFY SUBJECTS
const MODEL = process.env.CLASSIFY_MODEL || 'gpt-4o-mini';
const DEFAULT_LIMIT = 80;      // start 60–80; adjust once stable
const MAX_LIMIT = 150;         // keep modest to avoid truncation

const SUBJECTS = [
  "Anatomy","Physiology","Biochemistry","Pathology","Pharmacology","Microbiology",
  "Forensic Medicine","Community Medicine","ENT","Ophthalmology",
  "General Medicine","Pediatrics","Dermatology","Psychiatry",
  "General Surgery","Orthopedics","Anesthesia","Radiology","Obstetrics and Gynaecology"
];

// common variants → canonical
const SUBJECT_SYNONYMS = {
  "immunology": "Microbiology",
  "genetics": "Biochemistry",
  "public health": "Community Medicine",
  "psm": "Community Medicine",
  "spm": "Community Medicine",
  "radiodiagnosis": "Radiology",
  "radiodiagnostics": "Radiology",
  "internal medicine": "General Medicine",
  "medicine": "General Medicine",
  "surgery": "General Surgery",
  "obg": "Obstetrics and Gynaecology",
  "ob&g": "Obstetrics and Gynaecology",
  "obstetrics & gynaecology": "Obstetrics and Gynaecology",
  "obstetrics and gynecology": "Obstetrics and Gynaecology",
  "gynecology": "Obstetrics and Gynaecology",
  "ophthal": "Ophthalmology",
  "ophthalmic": "Ophthalmology",
  "eye": "Ophthalmology",
  "ent": "ENT",
  "psych": "Psychiatry",
  "derm": "Dermatology",
  "peds": "Pediatrics",
  "ortho": "Orthopedics",
  "anesthesiology": "Anesthesia",
  "micro": "Microbiology",
  "biochem": "Biochemistry",
  "pharma": "Pharmacology"
};

function normalizeSubject(s) {
  if (!s) return null;
  s = String(s).trim();
  if (SUBJECTS.includes(s)) return s;
  const key = s.toLowerCase();
  if (SUBJECT_SYNONYMS[key]) return SUBJECT_SYNONYMS[key];
  // gentle cleanup for minor punctuation/spacing
  const simplified = key.replace(/[&.]/g, '').replace(/\s+/g, ' ').trim();
  for (const sub of SUBJECTS) {
    if (sub.toLowerCase() === simplified) return sub;
  }
  return null;
}

function extractText(mcq) {
  if (!mcq) return '';
  if (typeof mcq === 'string') return mcq;
  if (typeof mcq === 'object') return mcq.stem || mcq.question || mcq.text || JSON.stringify(mcq);
  return String(mcq);
}
const truncate = (s, n = 600) => (s.length > n ? s.slice(0, n) + ' …' : s);

// Build a super-stable line-output prompt (no JSON, no IDs)
function buildPrompt(items) {
  const header = `
You classify MBBS MCQs into subjects.

Use ONLY these exact subjects:
${SUBJECTS.map(s => `- ${s}`).join('\n')}

Return format:
- Output EXACTLY ${items.length} LINES.
- Each line is ONE subject from the list, matching the order of the MCQs given.
- No numbering, no IDs, no extra text before/after. Just the subject names, one per line.

If you think of:
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

  const body = items
    .map((it, i) => `${i + 1}) ${truncate(extractText(it.mcq))}`)
    .join('\n\n');

  // We keep the MCQs in order; model must return exactly N lines in same order
  return `${header}\n\nMCQs:\n\n${body}\n\nRemember: output exactly ${items.length} lines, one subject per line.`;
}

exports.classifySubjects = async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit || '', 10) || DEFAULT_LIMIT, MAX_LIMIT);

    // 1) Fetch unclassified
    const { data: rows, error: fetchError } = await supabase
      .from('mcq_bank')
      .select('id, mcq')
      .is('subject', null)
      .order('id', { ascending: true })
      .limit(limit);

    if (fetchError) throw fetchError;
    if (!rows?.length) {
      return res.json({ message: '✅ No unclassified MCQs found.', fetched: 0, updated: 0 });
    }

    // 2) Build prompt & call model
    const prompt = buildPrompt(rows);
    const completion = await openai.chat.completions.create({
      model: MODEL,
      temperature: 0,
      // bump if you still see truncation
      // max_tokens: 2200,
      messages: [{ role: 'user', content: prompt }]
    });

    const raw = completion.choices?.[0]?.message?.content || '';
    // 3) Parse lines, map by index
    const lines = raw
      .trim()
      .replace(/^```.*?\n|\n```$/g, '') // strip fences if any
      .split(/\r?\n/)
      .map(l => l.replace(/^\d+[\).\s-]+/, '').trim()) // strip accidental numbering
      .filter(l => l.length > 0)
      .slice(0, rows.length);

    // 4) Normalize subjects
    const updates = [];
    for (let i = 0; i < rows.length && i < lines.length; i++) {
      const sub = normalizeSubject(lines[i]);
      if (sub) updates.push({ id: rows[i].id, subject: sub });
    }

    if (!updates.length) {
      return res.status(422).json({ error: 'No valid classifications returned' });
    }

    // 5) Bulk upsert subject only
    const { error: upErr, data: upd } = await supabase
      .from('mcq_bank')
      .upsert(updates, { onConflict: 'id', ignoreDuplicates: false })
      .select('id');

    if (upErr) throw upErr;

    return res.json({
      message: `✅ Classified ${upd?.length || updates.length} / ${rows.length} MCQs`,
      fetched: rows.length,
      updated: upd?.length || updates.length,
      droppedForWhitelist: rows.length - (upd?.length || updates.length),
      model: MODEL
    });
  } catch (err) {
    console.error('❌ classifySubjects error:', err);
    return res.status(500).json({ error: 'Internal Server Error', details: err.message });
  }
};


// === Tunables (GEN_* so they don't clash with other controllers) ===
const GEN_MODEL = process.env.GEN_MODEL || 'gpt-5-mini';
const GEN_DEFAULT_LIMIT = parseInt(process.env.GEN_LIMIT || '40', 10);
const GEN_MAX_LIMIT = 100;
const GEN_CONCURRENCY = parseInt(process.env.GEN_CONCURRENCY || '3', 10);
const GEN_LOCK_TTL_MIN = parseInt(process.env.GEN_LOCK_TTL_MIN || '20', 10);
const GEN_MAX_TOKENS = parseInt(process.env.GEN_MAX_TOKENS || '1800', 10);

// === Your prompt (renamed to avoid collisions) ===
const PRIMARY_PROMPT_TEMPLATE = `🚨 OUTPUT RULES: Your entire output must be a single valid JSON object.
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

// ---- helpers (all prefixed to avoid clashes) ----
const genSleep = (ms) => new Promise(r => setTimeout(r, ms));

async function genAsyncPool(limit, items, iter) {
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

function genCleanAndParseJSON(raw) {
  let t = (raw || '').trim()
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/```$/, '')
    .trim();
  const first = t.indexOf('{'); const last = t.lastIndexOf('}');
  if (first === -1 || last === -1 || last < first) throw new Error('No JSON object found');
  t = t.slice(first, last + 1);
  return JSON.parse(t);
}

function genBuildPrompt(row) {
  const mcqText = typeof row.mcq === 'string'
    ? row.mcq
    : (row.mcq?.stem || row.mcq?.question || row.mcq?.text || JSON.stringify(row.mcq));
  return `${PRIMARY_PROMPT_TEMPLATE}\n\nMCQ: ${mcqText}\nCorrect Answer: ${row.correct_answer || ''}`;
}

function genIsRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|rate limit|temporar|unavailable|ECONNRESET/i.test(s);
}

async function genCallOpenAIWithRetry(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: GEN_MODEL,
      temperature: 0.6,
      max_tokens: GEN_MAX_TOKENS,
      messages
    });
    return resp.choices?.[0]?.message?.content || '';
  } catch (e) {
    if (genIsRetryable(e) && attempt <= 3) {
      await genSleep(400 * attempt);
      return genCallOpenAIWithRetry(messages, attempt + 1);
    }
    throw e;
  }
}

// ---- locking ----
async function genClaimRows(limit, workerId) {
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

  // 2) lock & fetch rows
  const { data: locked, error: e2 } = await supabase
    .from('mcq_bank')
    .update({ primary_lock: workerId, primary_locked_at: new Date().toISOString() })
    .in('id', ids)
    .is('primary_mcq', null)
    .is('primary_lock', null)
    .select('id, mcq, correct_answer');
  if (e2) throw e2;

  return (locked || []).slice(0, limit);
}

async function genClearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from('mcq_bank')
    .update({ primary_lock: null, primary_locked_at: null })
    .in('id', ids);
}

// ---- per-row processor ----
async function genProcessRow(row) {
  const prompt = genBuildPrompt(row);
  const raw = await genCallOpenAIWithRetry([{ role: 'user', content: prompt }]);
  const parsed = genCleanAndParseJSON(raw);

  if (!parsed.primary_mcq || !parsed.learning_gap || !Array.isArray(parsed.buzzwords)) {
    throw new Error('Missing required fields in JSON');
  }

  const { error: upErr } = await supabase
    .from('mcq_bank')
    .update({ primary_mcq: parsed, primary_lock: null, primary_locked_at: null })
    .eq('id', row.id);
  if (upErr) throw upErr;

  return { id: row.id, ok: true };
}

// ---- main API ----
exports.generatePrimaryMCQs = async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || GEN_DEFAULT_LIMIT, 10), GEN_MAX_LIMIT);
  const concurrency = Math.max(1, parseInt(req.query.concurrency || GEN_CONCURRENCY, 10));
  const workerId = req.headers['x-worker-id'] || `w-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;

  try {
    const claimed = await genClaimRows(limit, workerId);
    if (!claimed.length) {
      return res.status(200).json({ message: 'No pending rows', claimed: 0, updated: 0, failed: 0, model: GEN_MODEL });
    }

    const results = await genAsyncPool(concurrency, claimed, r => genProcessRow(r));
    const updated = results.filter(r => r.status === 'fulfilled').length;
    const failed = results.length - updated;

    // clear locks for failed ones
    const stillLockedIds = claimed
      .filter((_, i) => results[i].status !== 'fulfilled')
      .map(r => r.id);
    await genClearLocks(stillLockedIds);

    return res.status(200).json({
      message: 'OK',
      model: GEN_MODEL,
      claimed: claimed.length,
      updated,
      failed,
      concurrency,
    });
  } catch (err) {
    console.error('❌ generatePrimaryMCQs error:', err.message || err);
    return res.status(500).json({ error: 'Internal Server Error', details: err.message });
  }
};


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

exports.generateLevel1ForMCQBank = async (req, res) => {
  try {
    const { data: rows, error: fetchError } = await supabase
      .from('mcq_bank')
      .select('id, primary_mcq')
      .is('level_1', null)
      .not('primary_mcq', 'is', null)
      .limit(20);

    if (fetchError) throw fetchError;
    if (!rows || rows.length === 0) {
      return res.json({ message: 'No eligible MCQs found without Level 1.' });
    }

    const results = [];

    for (const row of rows) {
      const prompt = `${LEVEL_1_PROMPT_TEMPLATE}\n\nPrimary MCQ:\n${JSON.stringify(row.primary_mcq)}`;

      let parsed = null;
      let attempt = 0;

      while (attempt < 3) {
        attempt++;
        try {
          const completion = await openai.chat.completions.create({
            model: 'gpt-5', // updated to GPT-5
            messages: [
              { role: 'system', content: 'You are a medical educator generating MCQs in JSON.' },
              { role: 'user', content: prompt }
            ],
            temperature: 0.5
          });

          const outputText = completion.choices[0].message.content.trim();

          // Attempt to parse output
          parsed = JSON.parse(outputText);

          // Validate structure
          if (
            !parsed.level_1 ||
            !parsed.level_1.mcq ||
            !parsed.level_1.mcq.stem ||
            !parsed.level_1.mcq.options ||
            !parsed.level_1.mcq.correct_answer ||
            !Array.isArray(parsed.level_1.buzzwords) ||
            !parsed.level_1.learning_gap
          ) {
            throw new Error('Invalid schema in GPT response');
          }

          // Schema validated
          break;
        } catch (err) {
          console.warn(`⚠️ Attempt ${attempt} failed to generate valid JSON:`, err.message);
          parsed = null;
        }
      }

      if (!parsed) {
        results.push({ id: row.id, status: '❌ GPT response invalid after 3 attempts' });
        continue;
      }

      // Save to Supabase
      const { error: updateError } = await supabase
        .from('mcq_bank')
        .update({ level_1: parsed.level_1 })
        .eq('id', row.id);

      if (updateError) {
        console.error('❌ Supabase update error:', updateError.message);
        results.push({ id: row.id, status: '❌ Supabase error' });
        continue;
      }

      results.push({ id: row.id, status: '✅ Level 1 saved' });
    }

    return res.json({
      message: `${results.length} Level 1 MCQs processed.`,
      updated: results
    });
  } catch (err) {
    console.error('❌ Fatal error:', err.message);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
};

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
2. Generate a new **Level 2 MCQ** targeting the **previous conceptual prerequisite**.
3. Use 5-sentence USMLE-style clinical vignette with bolded keywords.
4. Provide 5 options (A–E) with one correct answer.
5. Add a new learning gap (with at least 2 bolded keywords).
6. Include 10 exam-relevant buzzwords (emoji prefixed, bold terms).
7. Format output as:

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

exports.generateLevel2ForMCQBank = async (req, res) => {
  try {
    const { data: rows, error: fetchError } = await supabase
      .from('mcq_bank')
      .select('id, level_1')
      .is('level_2', null)
      .not('level_1', 'is', null)
      .limit(10);

    if (fetchError) throw fetchError;
    if (!rows || rows.length === 0) {
      return res.json({ message: 'No eligible MCQs found without Level 2.' });
    }

    const results = [];

    for (const row of rows) {
      const level1 = row.level_1;

      // ✅ Validate level_1 content before GPT prompt
      if (
        !level1?.mcq?.stem ||
        !level1?.mcq?.options ||
        !level1?.mcq?.correct_answer ||
        !level1?.learning_gap
      ) {
        results.push({ id: row.id, status: '❌ Invalid or incomplete level_1 MCQ' });
        continue;
      }

      const prompt = `${LEVEL_2_PROMPT_TEMPLATE}\n\nLevel 1 MCQ:\n${JSON.stringify(level1)}`;
      let parsed = null;
      let attempt = 0;

      while (attempt < 3) {
        attempt++;
        try {
          const completion = await openai.chat.completions.create({
            model: 'gpt-5', // updated to GPT-5
            messages: [
              { role: 'system', content: 'You are a medical educator generating MCQs in JSON.' },
              { role: 'user', content: prompt }
            ],
            temperature: 0.5
          });

          const outputText = completion.choices[0].message.content.trim();

          // Attempt to parse output
          parsed = JSON.parse(outputText);

          // Validate schema
          if (
            !parsed.level_2 ||
            !parsed.level_2.mcq ||
            !parsed.level_2.mcq.stem ||
            !parsed.level_2.mcq.options ||
            !parsed.level_2.mcq.correct_answer ||
            !Array.isArray(parsed.level_2.buzzwords) ||
            !parsed.level_2.learning_gap
          ) {
            throw new Error('Invalid schema in GPT response');
          }

          // Schema validated
          break;
        } catch (err) {
          console.warn(`⚠️ Attempt ${attempt} failed for ID ${row.id}:`, err.message);
          parsed = null;
        }
      }

      if (!parsed) {
        results.push({ id: row.id, status: '❌ GPT response invalid after 3 attempts' });
        continue;
      }

      // ✅ Save result to Supabase
      const { error: updateError } = await supabase
        .from('mcq_bank')
        .update({ level_2: parsed.level_2 })
        .eq('id', row.id);

      if (updateError) {
        console.error(`❌ Supabase update error for ID ${row.id}:`, updateError.message);
        results.push({ id: row.id, status: '❌ Supabase error' });
        continue;
      }

      results.push({ id: row.id, status: '✅ Level 2 saved' });
    }

    return res.json({
      message: `${results.length} Level 2 MCQs processed.`,
      updated: results
    });
  } catch (err) {
    console.error('❌ Fatal error:', err.message);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
};

const LEVEL_3_PROMPT_TEMPLATE = `🚨 OUTPUT RULES: 
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

🔬 You are an expert medical educator and exam learning strategist.

🎯 Your role is to act as a **Learning Gap Diagnostician** for MBBS/MD aspirants preparing for FMGE, NEETPG, INICET, or USMLE.

🧠 OBJECTIVE:
You will be given a Level 2 MCQ in the following JSON format:

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
2. Generate a new **Level 3 MCQ** targeting the **previous conceptual prerequisite** of Level 2.
3. Use 5-sentence USMLE-style clinical vignette with bolded keywords.
4. Provide 5 options (A–E) with one correct answer.
5. Add a new learning gap (with at least 2 bolded keywords).
6. Include 10 exam-relevant buzzwords (emoji prefixed, bold terms).
7. Format output as:

{
  "level_3": {
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

exports.generateLevel3ForMCQBank = async (req, res) => {
  try {
    const { data: rows, error: fetchError } = await supabase
      .from('mcq_bank')
      .select('id, level_2')
      .is('level_3', null)
      .not('level_2', 'is', null)
      .limit(10);

    if (fetchError) throw fetchError;
    if (!rows || rows.length === 0) {
      return res.json({ message: 'No eligible MCQs found without Level 3.' });
    }

    const results = [];

    for (const row of rows) {
      const level2 = row.level_2;

      // ✅ Validate level_2 content before GPT prompt
      if (
        !level2?.mcq?.stem ||
        !level2?.mcq?.options ||
        !level2?.mcq?.correct_answer ||
        !level2?.learning_gap
      ) {
        results.push({ id: row.id, status: '❌ Invalid or incomplete level_2 MCQ' });
        continue;
      }

      const prompt = `${LEVEL_3_PROMPT_TEMPLATE}\n\nLevel 2 MCQ:\n${JSON.stringify(level2)}`;
      let parsed = null;
      let attempt = 0;

      while (attempt < 3) {
        attempt++;
        try {
          const completion = await openai.chat.completions.create({
            model: 'gpt-5', // updated to GPT-5
            messages: [
              { role: 'system', content: 'You are a medical educator generating MCQs in JSON.' },
              { role: 'user', content: prompt }
            ],
            temperature: 0.5
          });

          const outputText = completion.choices[0].message.content.trim();
          parsed = JSON.parse(outputText);

          if (
            !parsed.level_3 ||
            !parsed.level_3.mcq?.stem ||
            !parsed.level_3.mcq?.options ||
            !parsed.level_3.mcq?.correct_answer ||
            !Array.isArray(parsed.level_3.buzzwords) ||
            !parsed.level_3.learning_gap
          ) {
            throw new Error('Invalid schema in GPT response');
          }

          break;
        } catch (err) {
          console.warn(`⚠️ Attempt ${attempt} failed for ID ${row.id}:`, err.message);
          parsed = null;
        }
      }

      if (!parsed) {
        results.push({ id: row.id, status: '❌ GPT response invalid after 3 attempts' });
        continue;
      }

      const { error: updateError } = await supabase
        .from('mcq_bank')
        .update({ level_3: parsed.level_3 })
        .eq('id', row.id);

      if (updateError) {
        console.error(`❌ Supabase update error for ID ${row.id}:`, updateError.message);
        results.push({ id: row.id, status: '❌ Supabase error' });
        continue;
      }

      results.push({ id: row.id, status: '✅ Level 3 saved' });
    }

    return res.json({
      message: `${results.length} Level 3 MCQs processed.`,
      updated: results
    });
  } catch (err) {
    console.error('❌ Fatal error:', err.message);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
};

const LEVEL_4_PROMPT_TEMPLATE = `🚨 OUTPUT RULES:
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

🎓 You are an expert medical educator and learning gap diagnostician.

🎯 GOAL:
Given a Level 3 MCQ, generate a deeper Level 4 MCQ targeting the conceptual root of the learning gap.

📦 INPUT FORMAT:
{
  "mcq": {
    "stem": "...",
    "options": {
      "A": "...", "B": "...", "C": "...", "D": "...", "E": "..."
    },
    "correct_answer": "..."
  },
  "learning_gap": "..."
}

📤 OUTPUT FORMAT:
{
  "level_4": {
    "mcq": {
      "stem": "...",
      "options": {...},
      "correct_answer": "..."
    },
    "explanation": "...",
    "buzzwords": [...],
    "learning_gap": "..."
  }
}`;

exports.generateLevel4ForMCQBank = async (req, res) => {
  try {
    const { data: rows, error: fetchError } = await supabase
      .from('mcq_bank')
      .select('id, level_3')
      .not('level_3', 'is', null)
      .is('level_4', null)
      .limit(5);

    if (fetchError) throw fetchError;
    if (!rows || rows.length === 0) {
      return res.json({ message: 'No eligible MCQs found for Level 4.' });
    }

    const results = [];

    for (const row of rows) {
      const prompt = `${LEVEL_4_PROMPT_TEMPLATE}\n\nLevel 3 MCQ:\n${JSON.stringify(row.level_3)}`;
      let parsed = null;
      let attempt = 0;

      while (attempt < 3) {
        attempt++;
        try {
          const completion = await openai.chat.completions.create({
            model: 'gpt-5', // updated to GPT-5
            messages: [
              { role: 'system', content: 'You are a medical educator generating MCQs in JSON.' },
              { role: 'user', content: prompt }
            ],
            temperature: 0.5
          });

          const outputText = completion.choices[0].message.content.trim();
          parsed = JSON.parse(outputText);

          if (
            !parsed.level_4 ||
            !parsed.level_4.stem ||
            !parsed.level_4.options ||
            !parsed.level_4.correct_answer ||
            !parsed.level_4.explanation ||
            !parsed.level_4.learning_gap ||
            !Array.isArray(parsed.level_4.buzzwords)
          ) {
            throw new Error('Invalid schema in GPT response');
          }

          break;
        } catch (err) {
          console.warn(`⚠️ Attempt ${attempt} failed for ID ${row.id}:`, err.message);
        }
      }

      if (!parsed) {
        results.push({ id: row.id, status: '❌ GPT response invalid after 3 attempts' });
        continue;
      }

      const { error: updateError } = await supabase
        .from('mcq_bank')
        .update({ level_4: parsed.level_4 })
        .eq('id', row.id);

      if (updateError) {
        console.error('❌ Supabase update error:', updateError.message);
        results.push({ id: row.id, status: '❌ Supabase update failed' });
        continue;
      }

      results.push({ id: row.id, status: '✅ Level 4 saved' });
    }

    return res.json({
      message: `${results.length} Level 4 MCQs processed.`,
      updated: results
    });
  } catch (err) {
    console.error('❌ Fatal error in Level 4 generation:', err.message);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
};

const LEVEL_5_PROMPT_TEMPLATE = `🚨 OUTPUT RULES:
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

🎓 You are an expert medical educator and learning gap diagnostician.

🎯 GOAL:
Given a Level 4 MCQ, generate a deeper Level 5 MCQ targeting the core foundational misunderstanding behind the Level 4 learning gap.

📦 INPUT FORMAT:
{
  "mcq": {
    "stem": "...",
    "options": {
      "A": "...", "B": "...", "C": "...", "D": "...", "E": "..."
    },
    "correct_answer": "..."
  },
  "learning_gap": "..."
}

📤 OUTPUT FORMAT:
{
  "level_5": {
    "mcq": {
      "stem": "...",
      "options": { ... },
      "correct_answer": "..."
    },
    "explanation": "...",
    "buzzwords": [ "...", "...", ... ],
    "learning_gap": "..."
  }
}`;
exports.generateLevel5ForMCQBank = async (req, res) => {
  try {
    const { data: rows, error: fetchError } = await supabase
      .from('mcq_bank')
      .select('id, level_4')
      .not('level_4', 'is', null)
      .is('level_5', null)
      .limit(5);

    if (fetchError) throw fetchError;
    if (!rows || rows.length === 0) {
      return res.json({ message: 'No eligible MCQs found for Level 5.' });
    }

    const results = [];

    for (const row of rows) {
      const prompt = `${LEVEL_5_PROMPT_TEMPLATE}\n\nLevel 4 MCQ:\n${JSON.stringify(row.level_4)}`;
      let parsed = null;
      let attempt = 0;

      while (attempt < 3) {
        attempt++;
        try {
          const completion = await openai.chat.completions.create({
            model: 'gpt-5', // updated to GPT-5
            messages: [
              { role: 'system', content: 'You are a medical educator generating MCQs in JSON.' },
              { role: 'user', content: prompt }
            ],
            temperature: 0.5
          });

          const outputText = completion.choices[0].message.content.trim();
          parsed = JSON.parse(outputText);

          if (
            !parsed.level_5 ||
            !parsed.level_5.mcq?.stem ||
            !parsed.level_5.mcq?.options ||
            !parsed.level_5.mcq?.correct_answer ||
            !parsed.level_5.explanation ||
            !parsed.level_5.learning_gap ||
            !Array.isArray(parsed.level_5.buzzwords)
          ) {
            throw new Error('Invalid schema in GPT response');
          }

          break;
        } catch (err) {
          console.warn(`⚠️ Attempt ${attempt} failed for ID ${row.id}:`, err.message);
        }
      }

      if (!parsed) {
        results.push({ id: row.id, status: '❌ GPT response invalid after 3 attempts' });
        continue;
      }

      const { error: updateError } = await supabase
        .from('mcq_bank')
        .update({ level_5: parsed.level_5 })
        .eq('id', row.id);

      if (updateError) {
        console.error('❌ Supabase update error:', updateError.message);
        results.push({ id: row.id, status: '❌ Supabase update failed' });
        continue;
      }

      results.push({ id: row.id, status: '✅ Level 5 saved' });
    }

    return res.json({
      message: `${results.length} Level 5 MCQs processed.`,
      updated: results
    });
  } catch (err) {
    console.error('❌ Fatal error in Level 5 generation:', err.message);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
};

const LEVEL_6_PROMPT_TEMPLATE = `🚨 OUTPUT RULES:
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

🎓 You are an expert medical educator and learning gap diagnostician.

🎯 GOAL:
Given a Level 5 MCQ, generate a deeper Level 6 MCQ targeting the **foundational conceptual root** behind the previous learning gap.

📦 INPUT FORMAT:
{
  "mcq": {
    "stem": "...",
    "options": {
      "A": "...", "B": "...", "C": "...", "D": "...", "E": "..."
    },
    "correct_answer": "..."
  },
  "learning_gap": "..."
}

📤 OUTPUT FORMAT:
{
  "level_6": {
    "mcq": {
      "stem": "...",
      "options": { ... },
      "correct_answer": "..."
    },
    "explanation": "...",
    "buzzwords": [ "...", "...", ... ],
    "learning_gap": "..."
  }
}`;
exports.generateLevel6ForMCQBank = async (req, res) => {
  try {
    const { data: rows, error: fetchError } = await supabase
      .from('mcq_bank')
      .select('id, level_5')
      .not('level_5', 'is', null)
      .is('level_6', null)
      .limit(5);

    if (fetchError) throw fetchError;
    if (!rows || rows.length === 0) {
      return res.json({ message: 'No eligible MCQs found for Level 6.' });
    }

    const results = [];

    for (const row of rows) {
      const prompt = `${LEVEL_6_PROMPT_TEMPLATE}\n\nLevel 5 MCQ:\n${JSON.stringify(row.level_5)}`;
      let parsed = null;
      let attempt = 0;

      while (attempt < 3) {
        attempt++;
        try {
          const completion = await openai.chat.completions.create({
            model: 'gpt-5', // updated to GPT-5
            messages: [
              { role: 'system', content: 'You are a medical educator generating MCQs in JSON.' },
              { role: 'user', content: prompt }
            ],
            temperature: 0.5
          });

          const outputText = completion.choices[0].message.content.trim();
          parsed = JSON.parse(outputText);

          if (
            !parsed.level_6 ||
            !parsed.level_6.mcq?.stem ||
            !parsed.level_6.mcq?.options ||
            !parsed.level_6.mcq?.correct_answer ||
            !parsed.level_6.explanation ||
            !parsed.level_6.learning_gap ||
            !Array.isArray(parsed.level_6.buzzwords)
          ) {
            throw new Error('Invalid schema in GPT response');
          }

          break;
        } catch (err) {
          console.warn(`⚠️ Attempt ${attempt} failed for ID ${row.id}:`, err.message);
        }
      }

      if (!parsed) {
        results.push({ id: row.id, status: '❌ GPT response invalid after 3 attempts' });
        continue;
      }

      const { error: updateError } = await supabase
        .from('mcq_bank')
        .update({ level_6: parsed.level_6 })
        .eq('id', row.id);

      if (updateError) {
        console.error('❌ Supabase update error:', updateError.message);
        results.push({ id: row.id, status: '❌ Supabase update failed' });
        continue;
      }

      results.push({ id: row.id, status: '✅ Level 6 saved' });
    }

    return res.json({
      message: `${results.length} Level 6 MCQs processed.`,
      updated: results
    });
  } catch (err) {
    console.error('❌ Fatal error in Level 6 generation:', err.message);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
};

const LEVEL_7_PROMPT_TEMPLATE = `🚨 OUTPUT RULES:
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

🎓 You are an expert medical educator and learning gap diagnostician.

🎯 GOAL:
Given a Level 6 MCQ, generate a deeper Level 7 MCQ targeting the **most granular foundational error** behind the previous learning gap.

📦 INPUT FORMAT:
{
  "mcq": {
    "stem": "...",
    "options": {
      "A": "...", "B": "...", "C": "...", "D": "...", "E": "..."
    },
    "correct_answer": "..."
  },
  "learning_gap": "..."
}

📤 OUTPUT FORMAT:
{
  "level_7": {
    "mcq": {
      "stem": "...",
      "options": { ... },
      "correct_answer": "..."
    },
    "explanation": "...",
    "buzzwords": [ "...", "...", ... ],
    "learning_gap": "..."
  }
}`;
exports.generateLevel7ForMCQBank = async (req, res) => {
  try {
    const { data: rows, error: fetchError } = await supabase
      .from('mcq_bank')
      .select('id, level_6')
      .not('level_6', 'is', null)
      .is('level_7', null)
      .limit(5);

    if (fetchError) throw fetchError;
    if (!rows || rows.length === 0) {
      return res.json({ message: 'No eligible MCQs found for Level 7.' });
    }

    const results = [];

    for (const row of rows) {
      const prompt = `${LEVEL_7_PROMPT_TEMPLATE}\n\nLevel 6 MCQ:\n${JSON.stringify(row.level_6)}`;
      let parsed = null;
      let attempt = 0;

      while (attempt < 3) {
        attempt++;
        try {
          const completion = await openai.chat.completions.create({
            model: 'gpt-5', // updated to GPT-5
            messages: [
              { role: 'system', content: 'You are a medical educator generating MCQs in JSON.' },
              { role: 'user', content: prompt }
            ],
            temperature: 0.5
          });

          const outputText = completion.choices[0].message.content.trim();
          parsed = JSON.parse(outputText);

          if (
            !parsed.level_7 ||
            !parsed.level_7.mcq?.stem ||
            !parsed.level_7.mcq?.options ||
            !parsed.level_7.mcq?.correct_answer ||
            !parsed.level_7.explanation ||
            !parsed.level_7.learning_gap ||
            !Array.isArray(parsed.level_7.buzzwords)
          ) {
            throw new Error('Invalid schema in GPT response');
          }

          break;
        } catch (err) {
          console.warn(`⚠️ Attempt ${attempt} failed for ID ${row.id}:`, err.message);
        }
      }

      if (!parsed) {
        results.push({ id: row.id, status: '❌ GPT response invalid after 3 attempts' });
        continue;
      }

      const { error: updateError } = await supabase
        .from('mcq_bank')
        .update({ level_7: parsed.level_7 })
        .eq('id', row.id);

      if (updateError) {
        console.error('❌ Supabase update error:', updateError.message);
        results.push({ id: row.id, status: '❌ Supabase update failed' });
        continue;
      }

      results.push({ id: row.id, status: '✅ Level 7 saved' });
    }

    return res.json({
      message: `${results.length} Level 7 MCQs processed.`,
      updated: results
    });
  } catch (err) {
    console.error('❌ Fatal error in Level 7 generation:', err.message);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
};

const LEVEL_8_PROMPT_TEMPLATE = `🚨 OUTPUT RULES:
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

🎓 You are an expert medical educator and learning gap diagnostician.

🎯 GOAL:
Given a Level 7 MCQ, generate an even deeper Level 8 MCQ that targets the **most microscopic conceptual error** remaining in the student's understanding from the previous level.

📦 INPUT FORMAT:
{
  "mcq": {
    "stem": "...",
    "options": {
      "A": "...", "B": "...", "C": "...", "D": "...", "E": "..."
    },
    "correct_answer": "..."
  },
  "learning_gap": "..."
}

📤 OUTPUT FORMAT:
{
  "level_8": {
    "mcq": {
      "stem": "...",
      "options": { ... },
      "correct_answer": "..."
    },
    "explanation": "...",
    "buzzwords": [ "...", "...", ... ],
    "learning_gap": "..."
  }
}`;
exports.generateLevel8ForMCQBank = async (req, res) => {
  try {
    const { data: rows, error: fetchError } = await supabase
      .from('mcq_bank')
      .select('id, level_7')
      .not('level_7', 'is', null)
      .is('level_8', null)
      .limit(5);

    if (fetchError) throw fetchError;
    if (!rows || rows.length === 0) {
      return res.json({ message: 'No eligible MCQs found for Level 8.' });
    }

    const results = [];

    for (const row of rows) {
      const prompt = `${LEVEL_8_PROMPT_TEMPLATE}\n\nLevel 7 MCQ:\n${JSON.stringify(row.level_7)}`;
      let parsed = null;
      let attempt = 0;

      while (attempt < 3) {
        attempt++;
        try {
          const completion = await openai.chat.completions.create({
            model: 'gpt-5', // updated to GPT-5
            messages: [
              { role: 'system', content: 'You are a medical educator generating MCQs in JSON.' },
              { role: 'user', content: prompt }
            ],
            temperature: 0.5
          });

          const outputText = completion.choices[0].message.content.trim();
          parsed = JSON.parse(outputText);

          if (
            !parsed.level_8 ||
            !parsed.level_8.mcq?.stem ||
            !parsed.level_8.mcq?.options ||
            !parsed.level_8.mcq?.correct_answer ||
            !parsed.level_8.explanation ||
            !parsed.level_8.learning_gap ||
            !Array.isArray(parsed.level_8.buzzwords)
          ) {
            throw new Error('Invalid schema in GPT response');
          }

          break;
        } catch (err) {
          console.warn(`⚠️ Attempt ${attempt} failed for ID ${row.id}:`, err.message);
        }
      }

      if (!parsed) {
        results.push({ id: row.id, status: '❌ GPT response invalid after 3 attempts' });
        continue;
      }

      const { error: updateError } = await supabase
        .from('mcq_bank')
        .update({ level_8: parsed.level_8 })
        .eq('id', row.id);

      if (updateError) {
        console.error('❌ Supabase update error:', updateError.message);
        results.push({ id: row.id, status: '❌ Supabase update failed' });
        continue;
      }

      results.push({ id: row.id, status: '✅ Level 8 saved' });
    }

    return res.json({
      message: `${results.length} Level 8 MCQs processed.`,
      updated: results
    });
  } catch (err) {
    console.error('❌ Fatal error in Level 8 generation:', err.message);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
};

const LEVEL_9_PROMPT_TEMPLATE = `🚨 OUTPUT RULES:
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

🎓 You are an expert medical educator and learning gap diagnostician.

🎯 GOAL:
Given a Level 8 MCQ, generate an even deeper Level 9 MCQ that reveals the **most elemental misunderstanding** behind the previous learning gap.

📦 INPUT FORMAT:
{
  "mcq": {
    "stem": "...",
    "options": {
      "A": "...", "B": "...", "C": "...", "D": "...", "E": "..."
    },
    "correct_answer": "..."
  },
  "learning_gap": "..."
}

📤 OUTPUT FORMAT:
{
  "level_9": {
    "mcq": {
      "stem": "...",
      "options": { ... },
      "correct_answer": "..."
    },
    "explanation": "...",
    "buzzwords": [ "...", "...", ... ],
    "learning_gap": "..."
  }
}`;
exports.generateLevel9ForMCQBank = async (req, res) => {
  try {
    const { data: rows, error: fetchError } = await supabase
      .from('mcq_bank')
      .select('id, level_8')
      .not('level_8', 'is', null)
      .is('level_9', null)
      .limit(5);

    if (fetchError) throw fetchError;
    if (!rows || rows.length === 0) {
      return res.json({ message: 'No eligible MCQs found for Level 9.' });
    }

    const results = [];

    for (const row of rows) {
      const prompt = `${LEVEL_9_PROMPT_TEMPLATE}\n\nLevel 8 MCQ:\n${JSON.stringify(row.level_8)}`;
      let parsed = null;
      let attempt = 0;

      while (attempt < 3) {
        attempt++;
        try {
          const completion = await openai.chat.completions.create({
            model: 'gpt-5', // updated to GPT-5
            messages: [
              { role: 'system', content: 'You are a medical educator generating MCQs in JSON.' },
              { role: 'user', content: prompt }
            ],
            temperature: 0.5
          });

          const outputText = completion.choices[0].message.content.trim();
          parsed = JSON.parse(outputText);

          if (
            !parsed.level_9 ||
            !parsed.level_9.mcq?.stem ||
            !parsed.level_9.mcq?.options ||
            !parsed.level_9.mcq?.correct_answer ||
            !parsed.level_9.explanation ||
            !parsed.level_9.learning_gap ||
            !Array.isArray(parsed.level_9.buzzwords)
          ) {
            throw new Error('Invalid schema in GPT response');
          }

          break;
        } catch (err) {
          console.warn(`⚠️ Attempt ${attempt} failed for ID ${row.id}:`, err.message);
        }
      }

      if (!parsed) {
        results.push({ id: row.id, status: '❌ GPT response invalid after 3 attempts' });
        continue;
      }

      const { error: updateError } = await supabase
        .from('mcq_bank')
        .update({ level_9: parsed.level_9 })
        .eq('id', row.id);

      if (updateError) {
        console.error('❌ Supabase update error:', updateError.message);
        results.push({ id: row.id, status: '❌ Supabase update failed' });
        continue;
      }

      results.push({ id: row.id, status: '✅ Level 9 saved' });
    }

    return res.json({
      message: `${results.length} Level 9 MCQs processed.`,
      updated: results
    });
  } catch (err) {
    console.error('❌ Fatal error in Level 9 generation:', err.message);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
};

const LEVEL_10_PROMPT_TEMPLATE = `🚨 OUTPUT RULES:
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

🎓 You are an expert medical educator and learning gap diagnostician.

🎯 GOAL:
Given a Level 9 MCQ, generate the **final Level 10 MCQ** that targets the **most irreducible, atomic-level conceptual misunderstanding** in the student’s mental model.

📦 INPUT FORMAT:
{
  "mcq": {
    "stem": "...",
    "options": {
      "A": "...", "B": "...", "C": "...", "D": "...", "E": "..."
    },
    "correct_answer": "..."
  },
  "learning_gap": "..."
}

📤 OUTPUT FORMAT:
{
  "level_10": {
    "mcq": {
      "stem": "...",
      "options": { ... },
      "correct_answer": "..."
    },
    "explanation": "...",
    "buzzwords": [ "...", "...", ... ],
    "learning_gap": "..."
  }
}`;
exports.generateLevel10ForMCQBank = async (req, res) => {
  try {
    const { data: rows, error: fetchError } = await supabase
      .from('mcq_bank')
      .select('id, level_9')
      .not('level_9', 'is', null)
      .is('level_10', null)
      .limit(5);

    if (fetchError) throw fetchError;
    if (!rows || rows.length === 0) {
      return res.json({ message: 'No eligible MCQs found for Level 10.' });
    }

    const results = [];

    for (const row of rows) {
      const prompt = `${LEVEL_10_PROMPT_TEMPLATE}\n\nLevel 9 MCQ:\n${JSON.stringify(row.level_9)}`;
      let parsed = null;
      let attempt = 0;

      while (attempt < 3) {
        attempt++;
        try {
          const completion = await openai.chat.completions.create({
            model: 'gpt-5', // updated to GPT-5
            messages: [
              { role: 'system', content: 'You are a medical educator generating MCQs in JSON.' },
              { role: 'user', content: prompt }
            ],
            temperature: 0.5
          });

          const outputText = completion.choices[0].message.content.trim();
          parsed = JSON.parse(outputText);

          if (
            !parsed.level_10 ||
            !parsed.level_10.mcq?.stem ||
            !parsed.level_10.mcq?.options ||
            !parsed.level_10.mcq?.correct_answer ||
            !parsed.level_10.explanation ||
            !parsed.level_10.learning_gap ||
            !Array.isArray(parsed.level_10.buzzwords)
          ) {
            throw new Error('Invalid schema in GPT response');
          }

          break;
        } catch (err) {
          console.warn(`⚠️ Attempt ${attempt} failed for ID ${row.id}:`, err.message);
        }
      }

      if (!parsed) {
        results.push({ id: row.id, status: '❌ GPT response invalid after 3 attempts' });
        continue;
      }

      const { error: updateError } = await supabase
        .from('mcq_bank')
        .update({ level_10: parsed.level_10 })
        .eq('id', row.id);

      if (updateError) {
        console.error('❌ Supabase update error:', updateError.message);
        results.push({ id: row.id, status: '❌ Supabase update failed' });
        continue;
      }

      results.push({ id: row.id, status: '✅ Level 10 saved' });
    }

    return res.json({
      message: `${results.length} Level 10 MCQs processed.`,
      updated: results
    });
  } catch (err) {
    console.error('❌ Fatal error in Level 10 generation:', err.message);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
};
