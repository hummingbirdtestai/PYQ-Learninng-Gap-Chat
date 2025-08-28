const { supabase } = require("../config/supabaseClient");

// Save a student answer
exports.saveAnswer = async (req, res) => {
  try {
    const { student_id, mcq_id, selected_option } = req.body;

    if (!student_id || !mcq_id || !selected_option) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    // Insert into student_answers
    const { data, error } = await supabase
      .from("student_answers")
      .insert([
        {
          student_id,
          mcq_id,
          selected_option,
        },
      ])
      .select("id, student_id, mcq_id, selected_option, correct_answer, is_correct");

    if (error) {
      console.error("Insert error:", error);
      return res.status(500).json({ error: "Failed to save answer" });
    }

    return res.status(201).json({ success: true, data: data?.[0] });
  } catch (err) {
    console.error("saveAnswer error:", err);
    return res.status(500).json({ error: "Internal Server Error" });
  }
};
