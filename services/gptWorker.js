const { supabase } = require('../config/supabaseClient');
const { Configuration, OpenAIApi } = require('openai');
const dotenv = require('dotenv');
dotenv.config();

const configuration = new Configuration({
  apiKey: process.env.OPENAI_API_KEY,
});
const openai = new OpenAIApi(configuration);

const PROMPT_TEMPLATE = `🔬 You are an expert medical educator and exam learning strategist.

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

All "buzzwords" must be 10 high-yield, bolded HTML-formatted one-liners, each starting with an emoji.`

async function runWorker(workerId) {
  console.log(`🧵 Worker ${workerId} started.`);

  while (true) {
    const { data: task } = await supabase
      .from('mcq_generation_queue')
      .select('id, raw_mcq_id')
      .eq('status', 'pending')
      .limit(1)
      .maybeSingle();

    if (!task) {
      await new Promise((r) => setTimeout(r, 4000));
      continue;
    }

    await supabase
      .from('mcq_generation_queue')
      .update({ status: 'in_progress', started_at: new Date() })
      .eq('id', task.id);

    try {
      const { data: mcq } = await supabase
        .from('raw_primary_mcqs')
        .select('*')
        .eq('id', task.raw_mcq_id)
        .single();

      const fullPrompt = `${PROMPT_TEMPLATE}\n\nHere is the MCQ:\n\nQuestion: ${mcq.question}\n\nOptions:\n${mcq.options.join('\n')}\n\nCorrect Answer: ${mcq.correct_answer}`;

      const response = await openai.createChatCompletion({
        model: "gpt-4",
        messages: [{ role: "user", content: fullPrompt }],
        temperature: 0.7
      });

      const output = response.data.choices[0].message.content;

      let parsed;
      try {
        parsed = JSON.parse(output);
      } catch (jsonErr) {
        throw new Error("❌ Invalid JSON from ChatGPT: " + jsonErr.message);
      }

      await supabase.from('mcq_graphs').insert({
        raw_mcq_id: mcq.id,
        exam_id: mcq.exam_id,
        subject_id: mcq.subject_id,
        graph: parsed,
        generated: true
      });

      await supabase
        .from('mcq_generation_queue')
        .update({ status: 'done', finished_at: new Date() })
        .eq('id', task.id);

      console.log(`✅ Worker ${workerId} completed task ${task.id}`);
    } catch (err) {
      await supabase
        .from('mcq_generation_queue')
        .update({
          status: 'failed',
          finished_at: new Date(),
          error: err.message
        })
        .eq('id', task.id);

      console.error(`❌ Worker ${workerId} failed task ${task.id}: ${err.message}`);
    }
  }
}

module.exports = { runWorker };
