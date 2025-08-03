const { v4: uuidv4 } = require('uuid');
const supabase = require('../config/supabaseClient');
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

// ✅ Validates primary MCQ — flexible number of options (4 or 5)
const validatePrimaryMCQ = (mcq) => {
  if (!mcq?.stem || typeof mcq.stem !== 'string' || !mcq?.correct_answer || typeof mcq.correct_answer !== 'string') return false;
  const optionKeys = Object.keys(mcq.options || {});
  return optionKeys.length >= 4 && optionKeys.length <= 5;
};

// ✅ Validates recursive MCQs — must have 5 full options A–E
const validateRecursiveMCQ = (mcq) => {
  const requiredKeys = ['A', 'B', 'C', 'D', 'E'];
  return (
    mcq?.stem &&
    typeof mcq.stem === 'string' &&
    typeof mcq.correct_answer === 'string' &&
    mcq?.options &&
    requiredKeys.every(
      (key) => typeof mcq.options?.[key] === 'string' && mcq.options[key].trim().length > 0
    )
  );
};

// ✅ Insert into Supabase with validation
const insertMCQ = async (mcq, level, validateFn, exam_id, subject_id) => {
  if (!validateFn(mcq)) {
    console.error(`❌ MCQ at level ${level} failed validation:\n`, JSON.stringify(mcq, null, 2));
    throw new Error(`MCQ at level ${level} failed validation`);
  }

  const id = uuidv4();
  const { error } = await supabase.from('mcqs').insert({
    id,
    exam_id,
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

// 🔁 GPT-based generation
exports.generateMCQGraphFromInput = async (req, res) => {
  const { raw_mcq_text, exam_id, subject_id } = req.body;

  if (!raw_mcq_text || !exam_id || !subject_id) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  const fullPrompt = `${PROMPT_TEMPLATE}

Here is the MCQ:

${raw_mcq_text}

- The above contains the full MCQ as entered by a teacher.
- You must identify the question, extract options A–E, and detect the correct answer if present.
- Then follow all previous instructions to reframe it into the required JSON output.`;

  try {
    const gptResponse = await openai.chat.completions.create({
      model: 'gpt-4-0613',
      messages: [{ role: 'user', content: fullPrompt }],
      temperature: 0.7
    });

    const raw = gptResponse.choices?.[0]?.message?.content;
    let parsed;

    try {
      parsed = JSON.parse(raw);
    } catch (parseError) {
      return res.status(500).json({
        error: 'Failed to parse GPT response as JSON',
        details: parseError.message,
        raw_output: raw
      });
    }

    const primaryId = await insertMCQ(parsed.primary_mcq, 0, validatePrimaryMCQ, exam_id, subject_id);
    const recursiveIds = [];

    if (!Array.isArray(parsed.recursive_levels)) {
      return res.status(400).json({ error: 'Invalid GPT response: recursive_levels is not an array' });
    }

    for (let i = 0; i < parsed.recursive_levels.length; i++) {
      const id = await insertMCQ(parsed.recursive_levels[i], i + 1, validateRecursiveMCQ, exam_id, subject_id);
      recursiveIds.push(id);
    }

    await supabase.from('mcq_graphs').insert({
      raw_mcq_id: null,
      exam_id,
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
    return res.status(500).json({ error: 'Failed to generate MCQ graph', details: err.message });
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
