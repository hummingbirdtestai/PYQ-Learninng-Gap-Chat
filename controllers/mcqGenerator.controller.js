const { v4: uuidv4 } = require('uuid');
const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

// 🔁 Use your full final prompt here as-is
const PROMPT_TEMPLATE = `🚨 OUTPUT RULES:
Your entire output must be a single valid JSON object.
- DO NOT include \`\`\`json or any markdown syntax.
- DO NOT add explanations, comments, or headings.
- Your output MUST start with { and end with }.
- It must be directly parsable by JSON.parse().
...
`;

// ✅ MCQ validation function
const validateMCQ = (mcq) => {
  if (!mcq?.stem || typeof mcq.stem !== 'string' || !mcq?.options || typeof mcq.options !== 'object' || !mcq?.correct_answer) return false;
  const opts = ['A', 'B', 'C', 'D', 'E'];
  return opts.every(opt => typeof mcq.options[opt] === 'string' && mcq.options[opt].trim() !== '') && opts.includes(mcq.correct_answer);
};

// ✅ Unified controller: Accept raw_mcq_id or full MCQ
exports.generateFromRaw = async (req, res) => {
  try {
    let raw_mcq_id = req.body.raw_mcq_id;

    let rawMCQ;

    // ✅ Case A: Fully external MCQ input
    if (!raw_mcq_id) {
      const { question, options, correct_answer, exam_id, subject_id, raw_mcq_id } = req.body;

      if (!question || !options || !correct_answer || !exam_id || !subject_id) {
        return res.status(400).json({ error: 'Missing fields: question, options, correct_answer, exam_id, subject_id' });
      }

      raw_mcq_id = uuidv4();

      const { error: insertError } = await supabase.from('raw_primary_mcqs').insert([{
        id: raw_mcq_id,
        question,
        options,
        correct_answer,
        exam_id,
        subject_id
      }]);

      if (insertError) {
        console.error('❌ Failed to insert raw MCQ:', insertError.message);
        return res.status(500).json({ error: 'Insert failed' });
      }

      rawMCQ = { id: raw_mcq_id, question, options, correct_answer, exam_id, subject_id };
    }

    // ✅ Case B: Existing raw_mcq_id passed
    if (!rawMCQ) {
      const { data, error } = await supabase.from('raw_primary_mcqs').select('*').eq('id', raw_mcq_id).maybeSingle();
      if (error || !data) return res.status(404).json({ error: 'Raw MCQ not found' });
      rawMCQ = data;
    }

    // ✅ Step 1: Generate GPT prompt
    const prompt = `${PROMPT_TEMPLATE}
Here is the MCQ:
Question: ${rawMCQ.question}
Options: ${Object.entries(rawMCQ.options).map(([k, v]) => `${k}) ${v}`).join('\n')}
Correct Answer: ${rawMCQ.correct_answer}`;

    // ✅ Step 2: Call GPT and parse
    let parsed, raw;
    for (let attempt = 0; attempt < 3; attempt++) {
      const gptRes = await openai.chat.completions.create({
        model: 'gpt-5', // updated to GPT-5
        messages: [{ role: 'user', content: prompt }],
        temperature: 0.7
      });

      raw = gptRes.choices[0].message.content;
      try {
        parsed = JSON.parse(raw);
        break;
      } catch (err) {
        if (attempt === 2) return res.status(500).json({ error: 'Failed to parse GPT response' });
      }
    }

    // ✅ Step 3: Insert into mcqs table
    const insertMCQ = async (mcq, level) => {
      if (!validateMCQ(mcq)) throw new Error(`MCQ at level ${level} failed validation`);
      const newId = uuidv4();
      const { error: insertError } = await supabase.from('mcqs').insert({
        id: newId,
        exam_id: rawMCQ.exam_id,
        subject_id: rawMCQ.subject_id,
        stem: mcq.stem,
        option_a: mcq.options.A,
        option_b: mcq.options.B,
        option_c: mcq.options.C,
        option_d: mcq.options.D,
        option_e: mcq.options.E,
        correct_answer: mcq.correct_answer,
        explanation: mcq.explanation || '',
        learning_gap: mcq.learning_gap || '',
        level,
        mcq_json: mcq
      });
      if (insertError) throw insertError;
      return newId;
    };

    const primaryId = await insertMCQ(parsed.primary_mcq, 0);
    const recursiveIds = [];

    if (!Array.isArray(parsed.recursive_levels)) throw new Error('recursive_levels must be an array');
    if (parsed.recursive_levels.length < 10) console.warn(`⚠️ Only ${parsed.recursive_levels.length} recursive levels found`);

    for (let i = 0; i < parsed.recursive_levels.length; i++) {
      const id = await insertMCQ(parsed.recursive_levels[i], i + 1);
      recursiveIds.push(id);
    }

    // ✅ Step 4: Insert graph
    await supabase.from('mcq_graphs').insert({
      raw_mcq_id,
      exam_id: rawMCQ.exam_id,
      subject_id: rawMCQ.subject_id,
      graph: { primary_mcq: primaryId, recursive_levels: recursiveIds },
      generated: true
    });

    return res.json({
      success: true,
      raw_mcq_id,
      primary_mcq_id: primaryId,
      recursive_ids: recursiveIds
    });
  } catch (err) {
    console.error('❌ Unhandled error:', err);
    return res.status(500).json({ error: err.message });
  }
};
