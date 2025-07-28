const { supabase } = require('../config/supabaseClient');
const { v4: uuidv4 } = require('uuid');

/**
 * @swagger
 * /generation/queue:
 *   post:
 *     summary: Queue all raw MCQs for GPT generation
 *     tags:
 *       - Generation
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - examId
 *               - subjectId
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
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 message:
 *                   type: string
 *                 queued:
 *                   type: integer
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
