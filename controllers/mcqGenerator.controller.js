const { v4: uuidv4 } = require('uuid');
const supabase = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

const PROMPT_TEMPLATE = `🚨 OUTPUT RULES:
Your entire output must be a single valid JSON object.
... // OMITTED FOR BREVITY, USE YOUR FULL PROMPT EXACTLY AS IS
`;

const validateMCQ = (mcq) => {
  if (!mcq?.stem || typeof mcq.stem !== 'string' || !mcq?.options || typeof mcq.options !== 'object' || !mcq?.correct_answer) return false;
  const opts = ['A', 'B', 'C', 'D', 'E'];
  return opts.every(opt => typeof mcq.options[opt] === 'string' && mcq.options[opt].trim() !== '') && opts.includes(mcq.correct_answer);
};

exports.generateMCQGraphFromRaw = async (req, res) => {
  const { raw_mcq_id } = req.body;
  if (!raw_mcq_id) return res.status(400).json({ error: 'Missing raw_mcq_id' });

  const { data: rawMCQ, error } = await supabase.from('raw_primary_mcqs').select('*').eq('id', raw_mcq_id).maybeSingle();
  if (!rawMCQ || error) return res.status(404).json({ error: 'Raw MCQ not found' });

  const prompt = `${PROMPT_TEMPLATE}
Here is the MCQ:
Question: ${rawMCQ.question}
Options: ${rawMCQ.options?.join('\n')}
Correct Answer: ${rawMCQ.correct_answer}`;

  let parsed, raw;
  for (let attempt = 0; attempt < 3; attempt++) {
    const gptRes = await openai.chat.completions.create({
      model: 'gpt-4-0613',
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

  try {
    const primaryId = await insertMCQ(parsed.primary_mcq, 0);
    const recursiveIds = [];

    if (!Array.isArray(parsed.recursive_levels)) throw new Error('recursive_levels is not an array');
    if (parsed.recursive_levels.length < 10) console.warn(`⚠️ Only ${parsed.recursive_levels.length} recursive levels found`);

    for (let i = 0; i < parsed.recursive_levels.length; i++) {
      const id = await insertMCQ(parsed.recursive_levels[i], i + 1);
      recursiveIds.push(id);
    }

    await supabase.from('mcq_graphs').insert({
      raw_mcq_id,
      exam_id: rawMCQ.exam_id,
      subject_id: rawMCQ.subject_id,
      graph: { primary_mcq: primaryId, recursive_levels: recursiveIds },
      generated: true
    });

    return res.json({ success: true, primary_mcq_id: primaryId, recursive_ids: recursiveIds });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
};
