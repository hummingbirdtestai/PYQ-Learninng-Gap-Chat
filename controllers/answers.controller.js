const { supabase } = require('../config/supabaseClient');

// Save a student answer
exports.saveAnswer = async (req, res) => {
  try {
    const { student_id, subject_id, mcq_id, selected_option, is_correct, mcq_index, correct_answer, exam_id } = req.body;

    if (!student_id || !subject_id || !mcq_id) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    const { error } = await supabase
      .from("student_answers")
      .insert([{
        student_id,
        subject_id,
        mcq_id,
        selected_option,
        is_correct,
        mcq_index,
        correct_answer,
        exam_id
      }]);

    if (error) throw error;
    return res.json({ message: "✅ Answer saved successfully" });
  } catch (err) {
    console.error("saveAnswer error:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
};
