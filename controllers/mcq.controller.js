// 🎯 Final Prompt Template
const { v4: uuidv4 } = require('uuid');
const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

const PROMPT_TEMPLATE = `🚨 OUTPUT RULES:
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
All "buzzwords" must be 10 high-yield, bolded HTML-formatted one-liners, each starting with an emoji.`;

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

// ✅ NEW: Structural validator
const isValidMCQGraph = (parsed) => {
  if (!parsed || typeof parsed !== 'object') return false;

  const { primary_mcq, recursive_levels } = parsed;
  if (!primary_mcq || typeof primary_mcq !== 'object') return false;
  if (!primary_mcq.stem || typeof primary_mcq.stem !== 'string') return false;
  if (!primary_mcq.options || typeof primary_mcq.options !== 'object') return false;
  if (!primary_mcq.correct_answer || typeof primary_mcq.correct_answer !== 'string') return false;

  const optionKeys = Object.keys(primary_mcq.options);
  if (optionKeys.length < 4 || optionKeys.length > 5) return false;

  if (!Array.isArray(recursive_levels) || recursive_levels.length !== 10) return false;

  for (const mcq of recursive_levels) {
    if (
      !mcq?.stem ||
      !mcq?.correct_answer ||
      typeof mcq.stem !== 'string' ||
      typeof mcq.correct_answer !== 'string' ||
      !mcq.options ||
      typeof mcq.options !== 'object' ||
      Object.keys(mcq.options).length !== 5
    ) {
      return false;
    }
  }

  return true;
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
- ⚠️ DO NOT return the JSON wrapped inside quotes.
- ⚠️ DO NOT return the object as a string — return only raw valid JSON.
- ❌ NO markdown, no headings, no code block.`;

  const maxAttempts = 3;
  let parsed = null;
  let lastRawOutput = '';

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const gptResponse = await openai.chat.completions.create({
        model: 'gpt-4-0613',
        messages: [{ role: 'user', content: fullPrompt }],
        temperature: 0.7
      });

      lastRawOutput = gptResponse.choices?.[0]?.message?.content || '';

      try {
        parsed = JSON.parse(lastRawOutput);

        // 🧠 Handle stringified JSON
        if (typeof parsed === 'string') {
          parsed = JSON.parse(parsed); // second parse
        }

        if (!isValidMCQGraph(parsed)) {
          throw new Error("GPT response failed structural validation");
        }

        break; // ✅ Valid and parsed successfully

      } catch (err) {
        console.warn(`⚠️ Attempt ${attempt}: GPT response invalid - ${err.message}`);
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
      console.warn(`❌ GPT API failed on attempt ${attempt}: ${gptErr.message}`);
      if (attempt === maxAttempts) {
        return res.status(500).json({
          error: 'GPT API failed after 3 attempts',
          details: gptErr.message
        });
      }
    }
  }

  try {
    const primaryId = await insertMCQ(parsed.primary_mcq, 0, validatePrimaryMCQ, subject_id);
    const recursiveIds = [];

    for (let i = 0; i < parsed.recursive_levels.length; i++) {
      const level = i + 1;
      const mcq = parsed.recursive_levels[i];
      const id = await insertMCQ(mcq, level, validateRecursiveMCQ, subject_id);
      recursiveIds.push(id);
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
    console.error('❌ Error generating MCQ Graph:', err.message);
    return res.status(500).json({
      error: 'Failed to generate MCQ graph',
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

  const prompt = `
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
