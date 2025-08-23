const { supabase } = require('../config/supabaseClient');

async function getStudentFacts(studentId) {
  // Example: gaps closed, next checkpoint, etc.
  const { data: answers } = await supabase
    .from('student_answers')
    .select('is_correct, subject_id, answered_at')
    .eq('student_id', studentId)
    .gte('answered_at', new Date(Date.now() - 24*60*60*1000).toISOString());

  const gapsClosed = answers?.filter(a => a.is_correct).length || 0;
  
  return {
    gaps_closed: gapsClosed,
    subject: "Pharmacology", // you can fetch from subject_id
    next_topic: "Renal Pathology",
    target_questions: 20
  };
}

module.exports = { getStudentFacts };
