const {supabase} = require('../config/supabaseClient');

// 1. Submit Responses in Batch
exports.submitResponsesBatch = async (req, res) => {
  const { responses } = req.body;

  if (!Array.isArray(responses) || responses.length === 0) {
    return res.status(400).json({ error: 'No responses provided.' });
  }

  const valid = responses.every(r =>
    r.user_id && r.mcq_id && 'selected_option' in r
  );

  if (!valid) {
    return res.status(400).json({ error: 'Missing fields in one or more responses.' });
  }

  try {
    const { error } = await supabase.from('responses').upsert(responses, {
      onConflict: ['user_id', 'mcq_id'],
      ignoreDuplicates: false,
    });
    if (error) throw error;
    res.json({ message: `✅ ${responses.length} responses saved.` });
  } catch (err) {
    console.error('❌ Error saving batch responses:', err.message);
    res.status(500).json({ error: err.message });
  }
};

// 2. Submit Final Quiz Score
exports.submitQuizScore = async (req, res) => {
  const { user_id, exam_id, subject_id, raw_mcq_id, score } = req.body;

  try {
    const { error } = await supabase.from('mcq_quiz_scores').upsert([{
      user_id,
      exam_id,
      subject_id,
      raw_mcq_id,
      score,
      updated_at: new Date(),
    }], { onConflict: ['user_id', 'raw_mcq_id'] });

    if (error) throw error;
    res.json({ message: '✅ Score updated.' });
  } catch (err) {
    console.error('❌ Error updating score:', err.message);
    res.status(500).json({ error: err.message });
  }
};

// 3. Get Resume Progress
exports.getResumeProgress = async (req, res) => {
  const { userId, examId, subjectId } = req.params;

  try {
    const { data, error } = await supabase
      .from('responses')
      .select('mcq_id, is_correct, skipped_due_to_timeout, answer_time')
      .eq('user_id', userId)
      .eq('exam_id', examId)
      .eq('subject_id', subjectId)
      .order('answer_time', { ascending: false });

    if (error) throw error;
    res.json({ progress: data });
  } catch (err) {
    console.error('❌ Error fetching resume progress:', err.message);
    res.status(500).json({ error: err.message });
  }
};

// 4. Update Progress (single MCQ)
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

  try {
    const { error } = await supabase.from('responses').upsert([{
      user_id,
      mcq_id,
      selected_option,
      is_correct,
      skipped_due_to_timeout,
      answer_time,
      start_time,
    }]);

    if (error) throw error;
    res.json({ message: '✅ Progress updated.' });
  } catch (err) {
    console.error('❌ Error updating progress:', err.message);
    res.status(500).json({ error: err.message });
  }
};

// 5. Get Full Graph
exports.getMCQGraph = async (req, res) => {
  const { examId, subjectId } = req.params;

  try {
    const { data: graphs, error: graphError } = await supabase
      .from('mcq_graphs')
      .select('raw_mcq_id, graph')
      .eq('exam_id', examId)
      .eq('subject_id', subjectId);

    if (graphError) throw graphError;

    const allUUIDs = [];
    graphs.forEach(g => {
      if (g.graph?.primary_mcq) allUUIDs.push(g.graph.primary_mcq);
      if (Array.isArray(g.graph?.recursive_levels)) {
        allUUIDs.push(...g.graph.recursive_levels);
      }
    });

    const { data: mcqs, error: mcqError } = await supabase
      .from('mcqs')
      .select('id, mcq_json')
      .in('id', allUUIDs);

    if (mcqError) throw mcqError;

    const mcqMap = new Map(mcqs.map(m => [m.id, m.mcq_json]));

    const reconstructed = graphs.map(g => ({
      raw_mcq_id: g.raw_mcq_id,
      graph: {
        primary_mcq: mcqMap.get(g.graph.primary_mcq),
        recursive_levels: (g.graph.recursive_levels || []).map(id => mcqMap.get(id)).filter(Boolean),
      },
    }));

    return res.json({ graphs: reconstructed });
  } catch (err) {
    console.error('❌ Error in getMCQGraph:', err.message);
    res.status(500).json({ error: 'Server error resolving MCQ graphs.' });
  }
};

// 6. Get Next Batch
exports.getNextMCQBatch = async (req, res) => {
  const { userId, examId, subjectId } = req.query;

  if (!userId || !examId || !subjectId) {
    return res.status(400).json({ error: 'Missing query parameters.' });
  }

  try {
    const { data: graphs } = await supabase
      .from('mcq_graphs')
      .select('id, raw_mcq_id, graph')
      .eq('exam_id', examId)
      .eq('subject_id', subjectId)
      .order('created_at', { ascending: true })
      .limit(50);

    const { data: progressData } = await supabase
      .from('mcq_progress')
      .select('raw_mcq_id, primary_index, secondary_index')
      .eq('user_id', userId)
      .eq('exam_id', examId)
      .eq('subject_id', subjectId);

    const progressMap = new Map(progressData.map(p => [p.raw_mcq_id, p]));

    const allUUIDs = [];
    graphs.forEach(g => {
      if (g.graph?.primary_mcq) allUUIDs.push(g.graph.primary_mcq);
      if (Array.isArray(g.graph?.recursive_levels)) {
        allUUIDs.push(...g.graph.recursive_levels);
      }
    });

    if (allUUIDs.length === 0) {
      return res.json({ batch: [], remaining_count: 0 });
    }

    const { data: mcqs } = await supabase
      .from('mcqs')
      .select('id, mcq_json')
      .in('id', allUUIDs);

    const mcqMap = new Map(mcqs.map(m => [m.id, m.mcq_json]));

    const batch = [];

    for (let g of graphs) {
      const chain = g.graph?.primary_mcq
        ? [g.graph.primary_mcq, ...(g.graph.recursive_levels || [])]
        : [];

      const progress = progressMap.get(g.raw_mcq_id);
      if (progress?.primary_index >= 9 && progress?.secondary_index >= 9) continue;

      const fullChain = chain.map(id => mcqMap.get(id)).filter(Boolean);
      if (fullChain.length === 0) continue;

      batch.push({
        mcq_graph_id: g.id,
        raw_mcq_id: g.raw_mcq_id,
        progress,
        mcq_chain: fullChain,
      });
    }

    return res.json({ batch, remaining_count: batch.length });
  } catch (err) {
    console.error('❌ Error fetching next batch:', err.message);
    res.status(500).json({ error: 'Server error fetching MCQs.' });
  }
};

// 7. Next Action
exports.handleNextAction = async (req, res) => {
  const {
    user_id, exam_id, subject_id, raw_mcq_id,
    selected_option, is_correct, answer_time,
    start_time, skipped_due_to_timeout
  } = req.body;

  if (!user_id || !exam_id || !subject_id || !raw_mcq_id || !selected_option) {
    return res.status(400).json({ error: 'Missing required fields.' });
  }

  try {
    await supabase.from('responses').upsert([{
      user_id, mcq_id: raw_mcq_id, selected_option,
      is_correct, skipped_due_to_timeout, answer_time,
      start_time, exam_id, subject_id,
    }]);

    const score = is_correct ? 4 : -1;
    await supabase.from('mcq_quiz_scores').upsert([{
      user_id, exam_id, subject_id, raw_mcq_id,
      score, updated_at: new Date(),
    }], { onConflict: ['user_id', 'raw_mcq_id'] });

    const { data: graphs } = await supabase
      .from('mcq_graphs')
      .select('id, raw_mcq_id, graph')
      .eq('exam_id', exam_id)
      .eq('subject_id', subject_id)
      .order('created_at', { ascending: true });

    const allUUIDs = graphs.flatMap(g =>
      [g.graph?.primary_mcq, ...(g.graph?.recursive_levels || [])]
    ).filter(Boolean);

    if (allUUIDs.length === 0) {
      return res.status(200).json({ message: '✅ All MCQs completed.', next_mcq: null });
    }

    const { data: mcqs } = await supabase
      .from('mcqs')
      .select('id, mcq_json')
      .in('id', allUUIDs);

    const mcqMap = new Map(mcqs.map(m => [m.id, m.mcq_json]));

    const { data: progressData } = await supabase
      .from('mcq_progress')
      .select('*')
      .eq('user_id', user_id)
      .eq('exam_id', exam_id)
      .eq('subject_id', subject_id);

    const progressMap = new Map(progressData.map(p => [p.raw_mcq_id, p]));

    for (const graph of graphs) {
      const uuidChain = graph.graph?.primary_mcq
        ? [graph.graph.primary_mcq, ...(graph.graph.recursive_levels || [])]
        : [];

      const progress = progressMap.get(graph.raw_mcq_id) || { primary_index: -1, secondary_index: -1 };
      const nextIndex = progress.secondary_index + 1;

      if (nextIndex < uuidChain.length) {
        return res.json({
          mcq_graph_id: graph.id,
          raw_mcq_id: graph.raw_mcq_id,
          progress,
          next_mcq: mcqMap.get(uuidChain[nextIndex]),
          next_index: nextIndex,
        });
      }
    }

    return res.status(200).json({ message: '✅ All MCQs completed.', next_mcq: null });
  } catch (err) {
    console.error('❌ Error in next-action:', err.message);
    res.status(500).json({ error: 'Server error.' });
  }
};

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
