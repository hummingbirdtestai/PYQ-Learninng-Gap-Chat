const { supabase } = require('../config/supabaseClient');
const { openai } = require('../config/openaiClient');

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

async function processNextMCQ() {
  // Atomically pick one pending item and mark as processing
  const { data: nextItem } = await supabase.rpc('fetch_and_lock_next_mcq');

  if (!nextItem) return; // No work

  const { id, raw_mcq_id } = nextItem;

  const { data: rawMCQ, error: rawError } = await supabase
    .from('raw_primary_mcqs')
    .select('*')
    .eq('id', raw_mcq_id)
    .maybeSingle();

  if (!rawMCQ || rawError) {
    console.error(`❌ Failed to fetch raw MCQ: ${raw_mcq_id}`);
    await supabase
      .from('mcq_generation_queue')
      .update({ status: 'failed', error_message: 'Raw MCQ fetch failed', failed_at: new Date() })
      .eq('id', id);
    return;
  }

  const prompt = `You are a medical educator... [Insert your exact final 10-level prompt here]\n\nMCQ: ${rawMCQ.question_with_options}\nCorrect Answer: ${rawMCQ.correct_answer}`;

  try {
    const chatResponse = await openai.chat.completions.create({
      model: 'gpt-4-0613',
      messages: [{ role: 'user', content: prompt }],
      temperature: 0.7
    });

    const raw = chatResponse.choices[0].message.content;

    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch (err) {
      throw new Error('Invalid JSON from GPT');
    }

    await supabase.from('mcq_graphs').insert({
      raw_mcq_id,
      generated_json: parsed
    });

    await supabase
      .from('mcq_generation_queue')
      .update({ status: 'done', completed_at: new Date() })
      .eq('id', id);

    console.log(`✅ Done: ${raw_mcq_id}`);
  } catch (err) {
    console.error(`❌ GPT error: ${err.message}`);
    await supabase
      .from('mcq_generation_queue')
      .update({ status: 'failed', error_message: err.message, failed_at: new Date() })
      .eq('id', id);
  }
}

// Worker loop to run continuously
async function startWorker() {
  while (true) {
    await processNextMCQ();
    await delay(1000); // Optional delay
  }
}

module.exports = { startWorker };
