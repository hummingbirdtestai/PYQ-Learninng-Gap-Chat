const { supabase } = require('../config/supabaseClient');

/**
 * GET /adaptive/pyq/start
 * Query: userId, examId, subjectId, limit?
 */
exports.getAdaptiveStart = async (req, res) => {
  try {
    const { userId, examId, subjectId } = req.query;
    const limit = parseInt(req.query.limit ?? '5', 10);

    if (!userId || !examId || !subjectId) {
      return res.status(400).json({ error: 'Missing parameters' });
    }

    // Fetch first N graphs for the subject/exam by primary_seq
    const { data: graphs, error } = await supabase
      .from('mcq_graphs')
      .select('*')
      .eq('exam_id', examId)
      .eq('subject_id', subjectId)
      .order('primary_seq', { ascending: true })
      .limit(limit);

    if (error) throw error;

    if (!graphs || graphs.length === 0) {
      return res.status(404).json({ error: 'No MCQ graphs found for this exam/subject' });
    }

    // Reset/initialize resume tracker to the first item of this batch
    await supabase
      .from('mcq_resume_tracker')
      .upsert({
        user_id: userId,
        exam_id: examId,
        subject_id: subjectId,
        last_primary_mcq_id: graphs[0]?.primary_mcq_id || null,
        last_level_played: 0
      }, { onConflict: 'user_id,exam_id,subject_id' });

    return res.json({ graphs });
  } catch (err) {
    console.error('getAdaptiveStart error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
};

/**
 * GET /adaptive/pyq/resume
 * Query: userId, examId, subjectId, limit?
 * Resumes from the last saved primary MCQ id and level.
 */
exports.getAdaptiveResume = async (req, res) => {
  try {
    const { userId, examId, subjectId } = req.query;
    const limit = parseInt(req.query.limit ?? '5', 10);

    if (!userId || !examId || !subjectId) {
      return res.status(400).json({ error: 'Missing parameters' });
    }

    // 1) Load tracker
    const { data: tracker, error: trackerErr } = await supabase
      .from('mcq_resume_tracker')
      .select('*')
      .eq('user_id', userId)
      .eq('exam_id', examId)
      .eq('subject_id', subjectId)
      .single();

    if (trackerErr && trackerErr.code !== 'PGRST116') throw trackerErr; // PGRST116 = no rows
    if (!tracker) {
      return res.status(404).json({ error: 'No resume data found' });
    }

    // 2) Find the primary_seq of the last_primary_mcq_id
    let startSeq = null;
    if (tracker.last_primary_mcq_id) {
      const { data: startRow, error: seqErr } = await supabase
        .from('mcq_graphs')
        .select('primary_seq')
        .eq('exam_id', examId)
        .eq('subject_id', subjectId)
        .eq('primary_mcq_id', tracker.last_primary_mcq_id)
        .single();

      if (seqErr && seqErr.code !== 'PGRST116') throw seqErr;
      startSeq = startRow?.primary_seq ?? null;
    }

    // 3) Load next batch from that sequence (include current so FE can decide)
    const query = supabase
      .from('mcq_graphs')
      .select('*')
      .eq('exam_id', examId)
      .eq('subject_id', subjectId)
      .order('primary_seq', { ascending: true })
      .limit(limit);

    if (startSeq !== null) {
      query.gte('primary_seq', startSeq);
    }

    const { data: graphs, error: graphsErr } = await query;
    if (graphsErr) throw graphsErr;

    return res.json({
      graphs: graphs || [],
      resumeFrom: {
        mcq_id: tracker.last_primary_mcq_id,
        level: tracker.last_level_played
      }
    });
  } catch (err) {
    console.error('getAdaptiveResume error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
};

/**
 * POST /adaptive/pyq/submit
 * Body: {
 *   userId, examId, subjectId,
 *   responses: [{ user_id, mcq_id, selected_option, ... }, ...],
 *   // optional hint so we can precisely move the resume pointer:
 *   resumeHint: { last_primary_mcq_id, last_level_played }
 * }
 *
 * NOTE: Adjust the 'responses' upsert shape/columns to match your schema.
 */
exports.submitBatchResponses = async (req, res) => {
  try {
    const { userId, examId, subjectId, responses, resumeHint } = req.body || {};

    if (!userId || !examId || !subjectId) {
      return res.status(400).json({ error: 'Missing userId/examId/subjectId' });
    }

    if (!Array.isArray(responses) || responses.length === 0) {
      return res.status(400).json({ error: 'No responses provided' });
    }

    // Validate basic shape (adjust as per your schema)
    const valid = responses.every(r =>
      r.user_id && r.mcq_id && Object.prototype.hasOwnProperty.call(r, 'selected_option')
    );
    if (!valid) {
      return res.status(400).json({ error: 'One or more responses missing required fields.' });
    }

    // 1) Save responses (upsert on user_id + mcq_id)
    const { error: respErr } = await supabase.from('responses').upsert(responses, {
      onConflict: ['user_id', 'mcq_id'],
      ignoreDuplicates: false
    });
    if (respErr) throw respErr;

    // 2) Update resume pointer if the client provides a precise hint
    if (resumeHint?.last_primary_mcq_id !== undefined || resumeHint?.last_level_played !== undefined) {
      const { error: upErr } = await supabase
        .from('mcq_resume_tracker')
        .upsert({
          user_id: userId,
          exam_id: examId,
          subject_id: subjectId,
          last_primary_mcq_id: resumeHint.last_primary_mcq_id ?? null,
          last_level_played: resumeHint.last_level_played ?? 0
        }, { onConflict: 'user_id,exam_id,subject_id' });

      if (upErr) throw upErr;
    }

    return res.json({ message: `✅ ${responses.length} responses saved.` });
  } catch (err) {
    console.error('submitBatchResponses error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
};
