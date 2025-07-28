const { supabase } = require('../config/supabaseClient');
const { v4: uuidv4 } = require('uuid');

/**
 * @swagger
 * /generation/queue:
 *   post:
 *     summary: Queue all raw MCQs for GPT generation
 *     tags: [Generation]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [examId, subjectId]
 *             properties:
 *               examId:
 *                 type: string
 *                 example: "a9c1e690-4f18-4c62-b3e9-f47384135a20"
 *               subjectId:
 *                 type: string
 *                 example: "e7b2e1e7-5cbe-4b4e-b5ea-d0741e51e0e1"
 *     responses:
 *       201:
 *         description: MCQs queued successfully
 *       200:
 *         description: No MCQs found to queue
 *       400:
 *         description: Missing examId or subjectId
 *       500:
 *         description: Server error or Supabase failure
 */
exports.queueMCQGeneration = async (req, res) => {
  const { examId, subjectId } = req.body;

  if (!examId || !subjectId) {
    return res.status(400).json({ error: '❌ examId and subjectId are required.' });
  }

  const { data: mcqs, error: fetchError } = await supabase
    .from('raw_primary_mcqs')
    .select('id')
    .eq('exam_id', examId)
    .eq('subject_id', subjectId);

  if (fetchError) {
    return res.status(500).json({ error: '❌ Failed to fetch raw MCQs.', details: fetchError.message });
  }

  if (!mcqs || mcqs.length === 0) {
    return res.status(200).json({ message: 'ℹ️ No MCQs found for this exam and subject.', queued: 0 });
  }

  const now = new Date().toISOString();

  const queueItems = mcqs.map(mcq => ({
    id: uuidv4(),
    raw_mcq_id: mcq.id,
    status: 'pending',
    queued_at: now
  }));

  const { error: insertError } = await supabase
    .from('mcq_generation_queue')
    .insert(queueItems);

  if (insertError) {
    return res.status(500).json({ error: '❌ Failed to insert into queue.', details: insertError.message });
  }

  return res.status(201).json({
    message: `✅ ${queueItems.length} MCQs added to generation queue.`,
    queued: queueItems.length
  });
};

/**
 * @swagger
 * /generation/status:
 *   get:
 *     summary: Get current GPT generation queue status
 *     tags: [Generation]
 *     responses:
 *       200:
 *         description: Queue summary
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 total:
 *                   type: integer
 *                 pending:
 *                   type: integer
 *                 processing:
 *                   type: integer
 *                 completed:
 *                   type: integer
 *                 failed:
 *                   type: integer
 *       500:
 *         description: Failed to fetch status
 */
exports.getGenerationStatus = async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('mcq_generation_queue')
      .select('status');

    if (error) {
      return res.status(500).json({ error: '❌ Failed to fetch status.', details: error.message });
    }

    const summary = {
      total: data.length,
      pending: 0,
      processing: 0,
      completed: 0,
      failed: 0
    };

    data.forEach(item => {
      const status = item.status || 'unknown';
      if (summary[status] !== undefined) {
        summary[status]++;
      }
    });

    return res.status(200).json(summary);
  } catch (err) {
    return res.status(500).json({ error: '❌ Server error.', details: err.message });
  }
};

/**
 * @swagger
 * /generation/results:
 *   get:
 *     summary: Get all generated MCQ graphs
 *     tags: [Generation]
 *     responses:
 *       200:
 *         description: List of generated MCQ graphs
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: object
 *                 properties:
 *                   raw_mcq_id:
 *                     type: string
 *                   generated_json:
 *                     type: object
 *                   created_at:
 *                     type: string
 *       500:
 *         description: Failed to fetch generated results
 */
exports.getGeneratedResults = async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('mcq_graphs')
      .select('raw_mcq_id, generated_json, created_at')
      .order('created_at', { ascending: false });

    if (error) {
      return res.status(500).json({ error: '❌ Failed to fetch results.', details: error.message });
    }

    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json({ error: '❌ Server error.', details: err.message });
  }
};
