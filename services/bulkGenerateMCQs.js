const supabase = require('../config/supabaseClient');
const openai = require('../config/openaiClient');
const { v4: uuidv4 } = require('uuid');

const PROMPT_TEMPLATE = `🚨 OUTPUT RULES:
Your entire output must be a single valid JSON object.
... (same prompt as you shared above)
`;

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const validateMCQ = (mcq) => {
  const requiredOptions = ['A', 'B', 'C', 'D', 'E'];
  return (
    mcq?.stem &&
    typeof mcq.stem === 'string' &&
    typeof mcq.correct_answer === 'string' &&
    typeof mcq.options === 'object' &&
    requiredOptions.every((opt) => typeof mcq.options[opt] === 'string' && mcq.options[opt].trim() !== '') &&
    requiredOptions.includes(mcq.correct_answer)
  );
};

const generateAndInsert = async (item) => {
  const { id, raw_mcq_id } = item;
  const { data: rawMCQ, error: rawError } = await supabase
    .from('raw_primary_mcqs')
    .select('*')
    .eq('id', raw_mcq_id)
    .maybeSingle();

  if (!rawMCQ || rawError) {
    console.error(`❌ Failed to fetch raw MCQ ${raw_mcq_id}`);
    await supabase.from('mcq_generation_queue').update({
      status: 'failed',
      error_message: 'Raw MCQ fetch failed',
      failed_at: new Date().toISOString()
    }).eq('id', id);
    return;
  }

  const fullPrompt = `${PROMPT_TEMPLATE}

Here is the MCQ:

Question: ${rawMCQ.question}

Options:
${rawMCQ.options?.join('\n')}

Correct Answer: ${rawMCQ.correct_answer}`;

  try {
    let raw, parsed;
    for (let attempt = 1; attempt <= 3; attempt++) {
      const response = await openai.chat.completions.create({
        model: 'gpt-4-0613',
        messages: [{ role: 'user', content: fullPrompt }],
        temperature: 0.7
      });

      raw = response.choices[0].message.content;

      try {
        parsed = JSON.parse(raw);
        break;
      } catch (e) {
        if (attempt === 3) throw new Error('Invalid JSON after 3 retries');
      }
    }

    const insertMCQ = async (mcq, level = null) => {
      if (!validateMCQ(mcq)) {
        throw new Error(`MCQ at level ${level} failed validation`);
      }
      const newId = uuidv4();
      const { error } = await supabase.from('mcqs').insert({
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
      if (error) throw error;
      return newId;
    };

    const primaryId = await insertMCQ(parsed.primary_mcq, 0);
    const recursiveIds = await Promise.all(
      parsed.recursive_levels?.map((mcq, i) => insertMCQ(mcq, i + 1)) || []
    );

    await supabase.from('mcq_graphs').insert({
      raw_mcq_id,
      exam_id: rawMCQ.exam_id,
      subject_id: rawMCQ.subject_id,
      graph: { primary_mcq: primaryId, recursive_levels: recursiveIds },
      generated: true
    });

    await supabase.from('mcq_generation_queue').update({
      status: 'done',
      completed_at: new Date().toISOString()
    }).eq('id', id);

    console.log(`✅ Processed: ${raw_mcq_id}`);
  } catch (err) {
    console.error(`❌ GPT error for ${raw_mcq_id}: ${err.message}`);
    await supabase.from('mcq_generation_queue').update({
      status: 'failed',
      error_message: err.message,
      failed_at: new Date().toISOString()
    }).eq('id', id);
  }
};

(async () => {
  console.log('🚀 Bulk GPT MCQ Generator started...');
  const { data: pendingItems, error } = await supabase
    .from('mcq_generation_queue')
    .select('*')
    .eq('status', 'pending')
    .order('id', { ascending: true });

  if (error || !pendingItems.length) {
    console.log('📭 No pending MCQs found.');
    return;
  }

  for (const item of pendingItems) {
    await supabase.from('mcq_generation_queue').update({ status: 'in_progress' }).eq('id', item.id);
    await generateAndInsert(item);
    await delay(1000); // Optional delay to avoid rate-limiting
  }

  console.log('✅ All pending MCQs processed.');
})();
