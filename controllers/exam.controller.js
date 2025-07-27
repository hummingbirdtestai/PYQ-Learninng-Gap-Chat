const { supabase } = require('../utils/supabaseClient');

/**
 * @swagger
 * /exams:
 *   post:
 *     summary: Create one or multiple exams
 *     tags: [Exams]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             oneOf:
 *               - type: object
 *                 required: [name]
 *                 properties:
 *                   name:
 *                     type: string
 *               - type: array
 *                 items:
 *                   type: object
 *                   required: [name]
 *                   properties:
 *                     name:
 *                       type: string
 *     responses:
 *       201:
 *         description: Exam(s) created successfully
 *       400:
 *         description: Missing required fields
 *       500:
 *         description: Failed to create exam(s)
 */
exports.createExam = async (req, res) => {
  let exams = [];

  if (Array.isArray(req.body)) {
    exams = req.body.filter(e => e.name);
  } else if (req.body?.name) {
    exams = [req.body];
  }

  if (exams.length === 0) {
    return res.status(400).json({ error: 'Missing required field: name' });
  }

  try {
    const { data, error } = await supabase
      .from('exams')
      .insert(exams)
      .select();

    if (error) throw error;

    res.status(201).json({ message: 'Exam(s) created', exams: data });
  } catch (err) {
    console.error('❌ Error creating exams:', err.message);
    res.status(500).json({ error: 'Failed to create exams' });
  }
};

/**
 * @swagger
 * /exams/{examId}/subjects:
 *   post:
 *     summary: Create one or multiple subjects under a specific exam
 *     tags: [Exams]
 *     parameters:
 *       - in: path
 *         name: examId
 *         required: true
 *         schema:
 *           type: string
 *         description: UUID of the exam
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             oneOf:
 *               - type: object
 *                 required: [name]
 *                 properties:
 *                   name:
 *                     type: string
 *                   code:
 *                     type: string
 *                   description:
 *                     type: string
 *               - type: array
 *                 items:
 *                   type: object
 *                   required: [name]
 *                   properties:
 *                     name:
 *                       type: string
 *                     code:
 *                       type: string
 *                     description:
 *                       type: string
 *     responses:
 *       201:
 *         description: Subject(s) created successfully
 *       400:
 *         description: Missing required fields
 *       500:
 *         description: Failed to create subject(s)
 */
exports.createSubjectUnderExam = async (req, res) => {
  const { examId } = req.params;

  if (!examId) {
    return res.status(400).json({ error: 'Missing required parameter: examId' });
  }

  let subjects = [];

  if (Array.isArray(req.body)) {
    subjects = req.body.filter(s => s.name).map(s => ({ ...s, exam_id: examId }));
  } else if (req.body?.name) {
    subjects = [{ ...req.body, exam_id: examId }];
  }

  if (subjects.length === 0) {
    return res.status(400).json({ error: 'Missing subject name(s)' });
  }

  try {
    const { data, error } = await supabase
      .from('subjects')
      .insert(subjects)
      .select();

    if (error) throw error;

    res.status(201).json({ message: 'Subject(s) created', subjects: data });
  } catch (err) {
    console.error('❌ Error creating subjects:', err.message);
    res.status(500).json({ error: 'Failed to create subjects' });
  }
};

/**
 * @swagger
 * /exams:
 *   get:
 *     summary: Get all exams
 *     tags: [Exams]
 *     responses:
 *       200:
 *         description: List of all exams
 *       500:
 *         description: Failed to fetch exams
 */
exports.getAllExams = async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('exams')
      .select('id, name');

    if (error) throw error;

    res.status(200).json({ exams: data });
  } catch (err) {
    console.error('❌ Error fetching exams:', err.message);
    res.status(500).json({ error: 'Failed to fetch exams' });
  }
};

/**
 * @swagger
 * /exams/with-subjects:
 *   get:
 *     summary: Get all exams with their subjects
 *     tags: [Exams]
 *     responses:
 *       200:
 *         description: A list of exams with their subjects
 *       500:
 *         description: Failed to fetch exams with subjects
 */
exports.getExamsWithSubjects = async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('exams')
      .select(`
        id,
        name,
        subjects (
          id,
          name,
          code,
          description
        )
      `);

    if (error) throw error;

    res.status(200).json({ exams: data });
  } catch (err) {
    console.error('❌ Error fetching exams with subjects:', err.message);
    res.status(500).json({ error: 'Failed to fetch exams with subjects' });
  }
};
