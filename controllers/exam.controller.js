const { supabase } = require('../utils/supabaseClient');

/**
 * @swagger
 * /exams:
 *   post:
 *     summary: Create a new exam
 *     tags: [Exams]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [name, code]
 *             properties:
 *               name:
 *                 type: string
 *                 example: NEET PG
 *               code:
 *                 type: string
 *                 example: neetpg
 *               description:
 *                 type: string
 *                 example: Postgraduate entrance exam in India
 *     responses:
 *       201:
 *         description: Exam created successfully
 *       400:
 *         description: Missing required fields
 *       500:
 *         description: Failed to create exam
 */
exports.createExam = async (req, res) => {
  const { name, code, description } = req.body;

  if (!name || !code) {
    return res.status(400).json({ error: 'Missing required fields: name or code' });
  }

  try {
    const { data, error } = await supabase
      .from('exams')
      .insert([{ name, code, description }])
      .select();

    if (error) throw error;

    res.status(201).json({ message: 'Exam created', exam: data[0] });
  } catch (err) {
    console.error('❌ Error creating exam:', err.message);
    res.status(500).json({ error: 'Failed to create exam' });
  }
};

/**
 * @swagger
 * /exams/{examId}/subjects:
 *   post:
 *     summary: Create a new subject under a specific exam
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
 *             type: object
 *             required: [name]
 *             properties:
 *               name:
 *                 type: string
 *                 example: Anatomy
 *               code:
 *                 type: string
 *                 example: anat
 *               description:
 *                 type: string
 *                 example: Study of human structure
 *     responses:
 *       201:
 *         description: Subject created successfully
 *       400:
 *         description: Missing required fields
 *       500:
 *         description: Failed to create subject
 */
exports.createSubjectUnderExam = async (req, res) => {
  const { examId } = req.params;
  const { name, code, description } = req.body;

  if (!examId || !name) {
    return res.status(400).json({ error: 'Missing required fields: examId or name' });
  }

  try {
    const { data, error } = await supabase
      .from('subjects')
      .insert([{ exam_id: examId, name, code, description }])
      .select();

    if (error) throw error;

    res.status(201).json({ message: 'Subject created', subject: data[0] });
  } catch (err) {
    console.error('❌ Error creating subject:', err.message);
    res.status(500).json({ error: 'Failed to create subject' });
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
      .select('id, name, code, description');

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
        code,
        description,
        subjects:subjects (
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
