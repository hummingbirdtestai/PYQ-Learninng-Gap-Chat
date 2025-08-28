const { supabase } = require('../config/supabaseClient');

// Save / update student progress
exports.saveProgress = async (req, res) => {
  try {
    const {
      student_id,
      subject_id,
      last_completed_seq,
      last_phase,
      last_mcq_index,
      is_completed
    } = req.body;

    if (!student_id || !subject_id) {
      return res.status(400).json({ error: "student_id and subject_id are required" });
    }

    const { error } = await supabase
      .from("student_progress")
      .upsert({
        student_id,
        subject_id,
        last_completed_seq,
        last_phase,
        last_mcq_index,
        is_completed,
        updated_at: new Date(),
      }, { onConflict: ["student_id", "subject_id"] }); // ✅ must be array

    if (error) throw error;

    return res.json({ message: "✅ Progress saved successfully" });
  } catch (err) {
    console.error("saveProgress error:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
};
