const {supabase} = require('../config/supabaseClient');

exports.getAdaptiveStart = async (req, res) => {
  const { userId, examId, subjectId, limit = 5 } = req.query;
  if (!userId || !examId || !subjectId) {
    return res.status(400).json({ error: "Missing parameters" });
  }

  // Get first N graphs for the subject/exam
  const { data: graphs, error } = await supabase
    .from("mcq_graphs")
    .select("*")
    .eq("exam_id", examId)
    .eq("subject_id", subjectId)
    .order("primary_seq", { ascending: true })
    .limit(limit);

  if (error) throw error;

  // Reset/initialize resume tracker
  await supabase
    .from("mcq_resume_tracker")
    .upsert({
      user_id: userId,
      exam_id: examId,
      subject_id: subjectId,
      last_primary_mcq_id: graphs[0]?.primary_mcq_id || null,
      last_level_played: 0
    });

  res.json({ graphs });
};

exports.getAdaptiveResume = async (req, res) => {
  const { userId, examId, subjectId, limit = 5 } = req.query;
  if (!userId || !examId || !subjectId) {
    return res.status(400).json({ error: "Missing parameters" });
  }

  // Get last saved progress
  const { data: tracker } = await supabase
    .from("mcq_resume_tracker")
    .select("*")
    .eq("user_id", userId)
    .eq("exam_id", examId)
    .eq("subject_id", subjectId)
    .single();

  if (!tracker) {
    return res.status(404).json({ error: "No resume data found" });
  }

  // Fetch next graphs starting from last_primary_mcq_id
  const { data: graphs, error } = await supabase
    .from("mcq_graphs")
    .select("*")
    .eq("exam_id", examId)
    .eq("subject_id", subjectId)
    .gte("primary_seq", tracker.primary_seq) // start from last played
    .order("primary_seq", { ascending: true })
    .limit(limit);

  if (error) throw error;

  res.json({
    graphs,
    resumeFrom: {
      mcq_id: tracker.last_primary_mcq_id,
      level: tracker.last_level_played
    }
  });
};
