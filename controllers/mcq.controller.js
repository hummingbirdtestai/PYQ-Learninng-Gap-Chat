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
        model: 'gpt-4-0613',
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
      model: 'gpt-4-0613',
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

const CLASSIFICATION_PROMPT = `
These are pyqS, Classify the question in each cell into one of the following MBBS subjects:
Anatomy, Physiology, Biochemistry, Pathology, Pharmacology, Microbiology, Forensic Medicine, Community Medicine, ENT, Ophthalmology, General Medicine, Pediatrics, Dermatology, Psychiatry, General Surgery, Orthopedics, Anesthesia, Radiology, Obstetrics and Gynaecology.
Only return the subject name (e.g., "Pharmacology")
`;

exports.classifySubjects = async (req, res) => {
  try {
    // Fetch 10 unclassified MCQs
    const { data: rows, error: fetchError } = await supabase
      .from('mcq_bank')
      .select('id, mcq')
      .is('subject', null)
      .limit(100);

    if (fetchError) throw fetchError;
    if (!rows || rows.length === 0) return res.json({ message: '✅ No unclassified MCQs found.' });

    for (const row of rows) {
      const prompt = `${CLASSIFICATION_PROMPT}\n\nMCQ: ${row.mcq}`;

      const chatResponse = await openai.chat.completions.create({
        model: 'gpt-4',
        messages: [{ role: 'user', content: prompt }],
        temperature: 0,
      });

      const subject = chatResponse.choices?.[0]?.message?.content?.trim() || 'Unclassified';

      await supabase
        .from('mcq_bank')
        .update({ subject })
        .eq('id', row.id);
    }

    res.json({ message: `✅ Classified ${rows.length} MCQs.` });
  } catch (err) {
    console.error('❌ Error classifying MCQs:', err);
    res.status(500).json({ error: 'Internal Server Error', details: err.message });
  }
};

const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

// ✅ Paste your exact GPT prompt here
const PROMPT_TEMPLATE = `🚨 OUTPUT RULES:
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().

🔬 You are an expert medical educator and exam learning strategist.
🎯 Your role is to create a recursive Level 1 MCQ based on the primary MCQ.
🧠 Instructions:
You will be given a primary MCQ with learning gap.
Generate 1 new MCQ based on that gap, using a USMLE-style 4–5 sentence clinical vignette.

1. Provide exactly 5 answer options (A–E), with one correct answer.
2. Write a 2–3 sentence explanation.
3. Identify the key learning gap if this Level 1 MCQ is answered wrong.
4. Include 10 buzzwords, emoji-prefixed and bolded using <strong>...</strong>.

💡 Format your JSON as:
{
  "level_1": {
    "stem": "...",
    "options": { "A": "...", "B": "...", "C": "...", "D": "...", "E": "..." },
    "correct_answer": "B",
    "explanation": "...",
    "learning_gap": "...",
    "buzzwords": ["...", "..."]
  }
}
`;

exports.generateLevel1MCQs = async (req, res) => {
  try {
    const { data: rows, error: fetchError } = await supabase
      .from('mcq_bank')
      .select('id, primary_mcq')
      .is('level_1', null)
      .not('primary_mcq', 'is', null)
      .limit(5);

    if (fetchError) throw fetchError;

    const results = [];

    for (const row of rows) {
      const primary = row.primary_mcq?.primary_mcq;
      const gap = row.primary_mcq?.learning_gap;

      if (!primary || !gap) {
        console.warn(`⚠️ Skipping row ${row.id}: Missing primary_mcq or learning_gap`);
        continue;
      }

      const fullPrompt = `${PROMPT_TEMPLATE}\n\nPrimary MCQ:\n${JSON.stringify(primary, null, 2)}\nLearning Gap: ${gap}`;

      let gptOutput;
      try {
        const gptRes = await openai.chat.completions.create({
          model: 'gpt-4-0613',
          messages: [{ role: 'user', content: fullPrompt }],
          temperature: 0.7
        });

        const raw = gptRes.choices?.[0]?.message?.content?.trim() || '';
        const cleaned = raw.replace(/^```json|```$/g, '').trim();
        gptOutput = JSON.parse(cleaned);
      } catch (err) {
        console.error(`❌ GPT error for row ${row.id}:`, err.message);
        continue;
      }

      const { error: updateError } = await supabase
        .from('mcq_bank')
        .update({ level_1: gptOutput })
        .eq('id', row.id);

      if (updateError) {
        console.error(`❌ Failed to update row ${row.id}`, updateError);
        continue;
      }

      results.push({ id: row.id, status: '✅ Inserted', preview: gptOutput?.level_1?.stem?.slice(0, 100) });
    }

    return res.json({ message: '✅ Level 1 MCQs Generated', count: results.length, results });
  } catch (err) {
    console.error('❌ generateLevel1MCQs error:', err.message);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
};
