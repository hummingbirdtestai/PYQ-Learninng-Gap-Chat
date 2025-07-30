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

  try {
    // Step 1: Fetch all graph entries
    const { data: graphs, error: graphError } = await supabase
      .from('mcq_graphs')
      .select('raw_mcq_id, graph')
      .eq('exam_id', examId)
      .eq('subject_id', subjectId);

    if (graphError) throw graphError;

    const allUUIDs = [];
    graphs.forEach((g) => {
      if (g.graph?.primary_mcq) allUUIDs.push(g.graph.primary_mcq);
      if (Array.isArray(g.graph?.recursive_levels)) {
        allUUIDs.push(...g.graph.recursive_levels);
      }
    });

    // Step 2: Resolve UUIDs to actual MCQs
    const { data: mcqs, error: mcqError } = await supabase
      .from('mcqs')
      .select('id, level, mcq_json')
      .in('id', allUUIDs);

    if (mcqError) throw mcqError;

    const mcqMap = new Map();
    mcqs.forEach((m) => mcqMap.set(m.id, m.mcq_json));

    // Step 3: Reconstruct full graph
    const reconstructed = graphs.map((g) => ({
      raw_mcq_id: g.raw_mcq_id,
      graph: {
        primary_mcq: mcqMap.get(g.graph.primary_mcq),
        recursive_levels: g.graph.recursive_levels.map((id) => mcqMap.get(id)).filter(Boolean),
      },
    }));

    return res.json({ graphs: reconstructed });
  } catch (err) {
    console.error('❌ Error in getMCQGraph:', err.message);
    return res.status(500).json({ error: 'Server error resolving MCQ graphs.' });
  }
};

// POST /adaptive/mcqs/next-action
exports.handleNextAction = async (req, res) => {
  const {
    user_id,
    exam_id,
    subject_id,
    raw_mcq_id,
    selected_option,
    is_correct,
    answer_time,
    start_time,
    skipped_due_to_timeout,
  } = req.body;

  if (!user_id || !exam_id || !subject_id || !raw_mcq_id || !selected_option) {
    return res.status(400).json({ error: 'Missing required fields.' });
  }

  try {
    // 1. Save response to "responses" table
    const { error: storeError } = await supabase.from('responses').upsert([{
      user_id,
      mcq_id: raw_mcq_id,
      selected_option,
      is_correct,
      skipped_due_to_timeout,
      answer_time,
      start_time,
      exam_id,
      subject_id,
    }]);

    if (storeError) throw storeError;

    // 2. Log score in "mcq_quiz_scores" table
    const score = is_correct ? 4 : -1;
    const { error: scoreError } = await supabase.from('mcq_quiz_scores').upsert([{
      user_id,
      exam_id,
      subject_id,
      raw_mcq_id,
      score,
      updated_at: new Date(),
    }], { onConflict: ['user_id', 'raw_mcq_id'] });

    if (scoreError) throw scoreError;

    // 3. Get next MCQ from mcq_graphs (based on current progress)
    const { data: graphs, error: graphError } = await supabase
      .from('mcq_graphs')
      .select('id, raw_mcq_id, graph')
      .eq('exam_id', exam_id)
      .eq('subject_id', subject_id)
      .order('created_at', { ascending: true });

    if (graphError) throw graphError;

    const { data: progressData } = await supabase
      .from('mcq_progress')
      .select('*')
      .eq('user_id', user_id)
      .eq('exam_id', exam_id)
      .eq('subject_id', subject_id);

    const progressMap = new Map();
    progressData?.forEach(p => progressMap.set(p.raw_mcq_id, p));

    let nextMCQ = null;

    for (const graph of graphs) {
      const mcqChain = graph.graph?.primary_mcq ? [graph.graph.primary_mcq, ...graph.graph.recursive_levels] : [];
      const progress = progressMap.get(graph.raw_mcq_id) || { primary_index: -1, secondary_index: -1 };

      const nextIndex = progress.secondary_index + 1;
      if (nextIndex < mcqChain.length) {
        nextMCQ = {
          mcq_graph_id: graph.id,
          raw_mcq_id: graph.raw_mcq_id,
          progress,
          next_mcq: mcqChain[nextIndex],
          next_index: nextIndex,
        };
        break;
      }
    }

    if (!nextMCQ) {
      return res.status(200).json({ message: '✅ All MCQs completed.', next_mcq: null });
    }

    return res.json(nextMCQ);
  } catch (err) {
    console.error('❌ Error in next-action API:', err.message);
    return res.status(500).json({ error: 'Server error.' });
  }
};

