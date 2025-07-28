require('dotenv').config();
const axios = require('axios');
const { parse } = require('csv-parse/sync');
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

async function importMCQs({ sheetUrl, examId, subjectId }) {
  try {
    const response = await axios.get(sheetUrl);
    const records = parse(response.data, { columns: false, skip_empty_lines: true });

    const mcqs = records.map(([questionBlock, correctAnswer]) => {
      const lines = questionBlock.split('\n').map(l => l.trim());
      const questionText = lines[0];
      const options = lines.slice(1, 6);
      return {
        exam_id: examId,
        subject_id: subjectId,
        question: questionText,
        options,
        correct_answer: correctAnswer.trim(),
      };
    });

    const { data, error } = await supabase.from('raw_primary_mcqs').insert(mcqs);
    if (error) throw error;

    console.log(`✅ Imported ${mcqs.length} MCQs.`);
    return { success: true, count: mcqs.length };
  } catch (err) {
    console.error('❌ Import failed:', err.message);
    return { success: false, error: err.message };
  }
}

module.exports = { importMCQs };
