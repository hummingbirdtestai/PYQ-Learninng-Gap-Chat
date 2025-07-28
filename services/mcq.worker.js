const { supabase } = require('../config/supabaseClient');
const { openai } = require('../config/openaiClient');

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// Worker that fetches and processes one pending MCQ at a time
async function processNextMCQ() {
  const { data: queueItem, error } = await supabase
    .from('mcq_generation_queue')
    .select('id, raw_mcq_id')
    .eq('status', 'pending')
    .limit(1)
    .maybeSingle();

  if (error || !queueItem) return;

  // Mark as processing
  await supabase
    .from('mcq_generation_queue')
    .update({ status: 'processing', started_at: new Date() })
    .eq('id', queueItem.id);

  const { data: rawMCQ } = await supabase
    .from('raw_primary_mcqs')
    .select('*')
    .eq('id', queueItem.raw_mcq_id)
    .single();

  const prompt = `You are a medical educator... [insert your exact 10-level MCQ graph prompt here] \n\nMCQ: ${rawMCQ.question_with_options}\nCorrect Answer: ${rawMCQ.correct_answer}`;

  try {
    const chatResponse = await openai.chat.completions.create({
      model: 'gpt-4-0613',
      messages: [{ role: 'user', content: prompt }],
      temperature: 0.7
    });

    const raw = chatResponse.choices[0].message.content;

    const parsed = JSON.parse(raw); // Validate output
    await supabase.from('mcq_graphs').insert({
      raw_mcq_id: queueItem.raw_mcq_id,
      generated_json: parsed
    });

    await supabase
      .from('mcq_generation_queue')
      .update({ status: 'done', completed_at: new Date() })
      .eq('id', queueItem.id);
  } catch (err) {
    console.error('❌ GPT failed:', err.message);
    await supabase
      .from('mcq_generation_queue')
      .update({ status: 'failed', error_message: err.message, failed_at: new Date() })
      .eq('id', queueItem.id);
  }
}

module.exports = { processNextMCQ };
