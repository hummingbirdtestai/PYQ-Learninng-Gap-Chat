const supabase = require('../config/supabaseClient');
const openai = require('../config/openaiClient');
const { v4: uuidv4 } = require('uuid');

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Lock and fetch next pending MCQ from queue
const lockNextPendingMCQ = async () => {
  try {
    const { data, error } = await supabase
      .from('mcq_generation_queue')
      .update({ status: 'in_progress' })
      .eq('status', 'pending')
      .order('id', { ascending: true })
      .limit(1)
      .select();

    if (error) throw error;
    return data?.[0] || null;
  } catch (err) {
    console.error('❌ Error locking next MCQ:', err.message || err);
    return null;
  }
};

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

async function processNextInQueue(workerId = 1) {
  console.log(`🧠 Worker ${workerId}: Looking for next pending MCQ...`);
  const item = await lockNextPendingMCQ();

  if (!item) {
    console.log(`📭 Worker ${workerId}: No pending MCQs in queue.`);
    return false;
  }

  const { id, raw_mcq_id } = item;
  console.log(`🔍 Worker ${workerId}: Found MCQ ID ${raw_mcq_id}`);

  const { data: rawMCQ, error: rawError } = await supabase
    .from('raw_primary_mcqs')
    .select('*')
    .eq('id', raw_mcq_id)
    .maybeSingle();

  if (!rawMCQ || rawError) {
    console.error(`❌ Worker ${workerId}: Failed to fetch raw MCQ: ${raw_mcq_id}`);
    await supabase
      .from('mcq_generation_queue')
      .update({
        status: 'failed',
        error_message: 'Raw MCQ fetch failed',
        failed_at: new Date().toISOString()
      })
      .eq('id', id);
    return false;
  }

  const fullPrompt = `${PROMPT_TEMPLATE}

Here is the MCQ:

Question: ${rawMCQ.question}

Options:
${rawMCQ.options?.join('\n')}

Correct Answer: ${rawMCQ.correct_answer}`;

  try {
    console.log(`🤖 Worker ${workerId}: Sending to OpenAI...`);

    let raw;
    let parsed;
    let attempts = 0;

    while (attempts < 3) {
      const chatResponse = await openai.chat.completions.create({
        model: 'gpt-4-0613',
        messages: [{ role: 'user', content: fullPrompt }],
        temperature: 0.7
      });

      raw = chatResponse.choices[0].message.content;
      console.log(`📦 GPT Raw Output (Attempt ${attempts + 1}):\n${raw}`);

      try {
        parsed = JSON.parse(raw);
        break;
      } catch (err) {
        attempts++;
        console.warn(`⚠️ Worker ${workerId}: Invalid JSON from GPT (Attempt ${attempts})`);
        if (attempts === 3) throw new Error("Invalid JSON after 3 retries");
      }
    }

    const insertMCQ = async (mcq, level = null) => {
      const newId = uuidv4();
      const { error: insertError } = await supabase.from('mcqs').insert({
        id: newId,
        exam_id: rawMCQ.exam_id,
        subject_id: rawMCQ.subject_id,
        level,
        mcq_json: mcq
      });

      if (insertError) throw insertError;
      return newId;
    };

    const primaryId = await insertMCQ(parsed.primary_mcq, 0);
    const recursiveIds = [];

    for (let i = 0; i < parsed.recursive_levels.length; i++) {
      const id = await insertMCQ(parsed.recursive_levels[i], i + 1);
      recursiveIds.push(id);
    }

    const uuidGraph = {
      primary_mcq: primaryId,
      recursive_levels: recursiveIds
    };

    await supabase.from('mcq_graphs').insert({
      raw_mcq_id,
      exam_id: rawMCQ.exam_id,
      subject_id: rawMCQ.subject_id,
      graph: uuidGraph,
      generated: true
    });

    await supabase
      .from('mcq_generation_queue')
      .update({ status: 'done', completed_at: new Date().toISOString() })
      .eq('id', id);

    console.log(`✅ Worker ${workerId}: Successfully processed ${raw_mcq_id}`);
    return true;
  } catch (err) {
    console.error(`❌ Worker ${workerId}: GPT error for ${raw_mcq_id}: ${err.message}`);
    await supabase
      .from('mcq_generation_queue')
      .update({
        status: 'failed',
        error_message: err.message,
        failed_at: new Date().toISOString()
      })
      .eq('id', id);
    return false;
  }
}

module.exports = { processNextInQueue };

// Optional: run as single-worker script
if (require.main === module) {
  (async () => {
    console.log('🚀 Single MCQ Worker started (direct run)');
    while (true) {
      const done = await processNextInQueue(1);
      if (!done) break;
      await delay(1000);
    }
  })();
}
