const supabase = require('../config/supabaseClient');

// 1. Submit Responses in Batch
exports.submitResponses = async (req, res) => {
  const { responses } = req.body;

  const { error } = await supabase.from('responses').upsert(responses, {
    onConflict: ['user_id', 'mcq_id'],
    ignoreDuplicates: false,
  });

  if (error) return res.status(500).json({ error: error.message });
  res.json({ message: `✅ ${responses.length} responses saved.` });
};

// 2. Submit Final Quiz Score
exports.submitQuizScore = async (req, res) => {
  const { user_id, exam_id, subject_id, raw_mcq_id, score } = req.body;

  const { error } = await supabase.from('mcq_quiz_scores').upsert([{
    user_id,
    exam_id,
    subject_id,
    raw_mcq_id,
    score,
    updated_at: new Date(),
  }], { onConflict: ['user_id', 'raw_mcq_id'] });

  if (error) return res.status(500).json({ error: error.message });
  res.json({ message: '✅ Score updated.' });
};

// 3. Get Resume Progress
exports.getResumeProgress = async (req, res) => {
  const { userId, examId, subjectId } = req.params;

  const { data, error } = await supabase
    .from('responses')
    .select('mcq_id, is_correct, skipped_due_to_timeout, answer_time')
    .eq('user_id', userId)
    .eq('exam_id', examId)
    .eq('subject_id', subjectId)
    .order('answer_time', { ascending: false });

  if (error) return res.status(500).json({ error: error.message });
  res.json({ progress: data });
};

// 4. Update Progress (per-MCQ or fallback)
exports.updateProgress = async (req, res) => {
  const {
    user_id,
    mcq_id,
    selected_option,
    is_correct,
    skipped_due_to_timeout,
    answer_time,
    start_time
  } = req.body;

  const { error } = await supabase.from('responses').upsert([{
    user_id,
    mcq_id,
    selected_option,
    is_correct,
    skipped_due_to_timeout,
    answer_time,
    start_time,
  }]);

  if (error) return res.status(500).json({ error: error.message });
  res.json({ message: '✅ Progress updated.' });
};

// 5. Get Full MCQ Graph (Optional for Admin)
exports.getMCQGraph = async (req, res) => {
  const { examId, subjectId } = req.params;

  const { data, error } = await supabase
    .from('mcq_graphs')
    .select('*')
    .eq('exam_id', examId)
    .eq('subject_id', subjectId);

  if (error) return res.status(500).json({ error: error.message });
  res.json({ graphs: data });
};

exports.getNextMCQBatch = async (req, res) => {
  const { userId, examId, subjectId } = req.query;

  if (!userId || !examId || !subjectId) {
    return res.status(400).json({ error: 'Missing required query parameters.' });
  }

  try {
    // Step 1: Get all graph entries for the user’s exam + subject
    const { data: graphs, error: graphError } = await supabase
      .from('mcq_graphs')
      .select('id, raw_mcq_id, graph')
      .eq('exam_id', examId)
      .eq('subject_id', subjectId)
      .order('created_at', { ascending: true })
      .limit(50); // Pull more if needed to skip attempted ones

    if (graphError) throw graphError;

    // Step 2: Get progress for this user
    const { data: progressData, error: progressError } = await supabase
      .from('mcq_progress')
      .select('raw_mcq_id, primary_index, secondary_index')
      .eq('user_id', userId)
      .eq('exam_id', examId)
      .eq('subject_id', subjectId);

    if (progressError) throw progressError;

    const progressMap = new Map();
    progressData.forEach(p => {
      progressMap.set(p.raw_mcq_id, {
        primary_index: p.primary_index,
        secondary_index: p.secondary_index,
      });
    });

    // Step 3: Prepare next batch (skip already completed ones)
    const batch = [];

    for (let g of graphs) {
      const mcqChain = g.graph?.primary_mcq ? [g.graph.primary_mcq, ...g.graph.recursive_levels] : [];
      const existingProgress = progressMap.get(g.raw_mcq_id);

      if (existingProgress?.primary_index >= 9 && existingProgress?.secondary_index >= 9) {
        continue; // skip if already completed
      }

      batch.push({
        mcq_graph_id: g.id,
        raw_mcq_id: g.raw_mcq_id,
        progress: existingProgress || { primary_index: -1, secondary_index: -1 },
        mcqs: mcqChain,
      });

      if (batch.length >= 20) break;
    }

    const remaining_count = Math.max(0, graphs.length - batch.length);

    return res.json({ batch, remaining_count });
  } catch (err) {
    console.error('Error fetching next batch:', err.message);
    return res.status(500).json({ error: 'Server error fetching MCQs.' });
  }
};
