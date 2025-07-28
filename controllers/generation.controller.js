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
 *                 example: "exam-uuid"
 *               subjectId:
 *                 type: string
 *                 example: "subject-uuid"
 *     responses:
 *       201:
 *         description: MCQs queued successfully
 *       400:
 *         description: Missing examId or subjectId
 *       500:
 *         description: Server error or Supabase failure
 */
exports.queueMCQGeneration = async (req, res) => {
  const { examId, subjectId } = req.body;

  if (!examId || !subjectId) {
    return res.status(400).json({ error: 'examId and subjectId are required.' });
  }

  const { data: mcqs, error } = await supabase
    .from('raw_primary_mcqs')
    .select('id')
    .eq('exam_id', examId)
    .eq('subject_id', subjectId);

  if (error) return res.status(500).json({ error: error.message });

  const queue = mcqs.map(mcq => ({
    id: uuidv4(),
    raw_mcq_id: mcq.id,
    status: 'pending'
  }));

  const { error: insertError } = await supabase
    .from('mcq_generation_queue')
    .insert(queue);

  if (insertError) return res.status(500).json({ error: insertError.message });

  return res.status(201).json({
    message: `✅ ${queue.length} MCQs added to generation queue.`,
    queued: queue.length
  });
};
