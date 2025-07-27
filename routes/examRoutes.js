// examRoutes.js
const express = require('express');
const router = express.Router();
const { supabase } = require('../utils/supabaseClient');

/**
 * @swagger
 * tags:
 *   - name: Exams
 *     description: Manage Exams and Subjects
 */

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
 *                 example: India's national post-graduate entrance exam
 *     responses:
 *       201:
 *         description: Exam created
 *       400:
 *         description: Missing fields
 *       500:
 *         description: Database error
 */
router.post('/exams', async (req, res) => {
  const { name, code, description } = req.body;
  if (!name || !code) return res.status(400).json({ error: 'Missing name or code' });

  const { data, error } = await supabase
    .from('exams')
    .insert([{ name, code, description }])
    .select();

  if (error) return res.status(500).json({ error: error.message });
  res.status(201).json({ message: 'Exam created', exam: data[0] });
});

/**
 * @swagger
 * /subjects:
 *   post:
 *     summary: Create a new subject under an exam
 *     tags: [Exams]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [exam_id, name]
 *             properties:
 *               exam_id:
 *                 type: string
 *                 example: 6f03b8e6-bd58-4be4-a1b4-e979fc703e19
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
 *         description: Subject created
 *       400:
 *         description: Missing fields
 *       500:
 *         description: Database error
 */
router.post('/subjects', async (req, res) => {
  const { exam_id, name, code, description } = req.body;
  if (!exam_id || !name) return res.status(400).json({ error: 'Missing exam_id or name' });

  const { data, error } = await supabase
    .from('subjects')
    .insert([{ exam_id, name, code, description }])
    .select();

  if (error) return res.status(500).json({ error: error.message });
  res.status(201).json({ message: 'Subject created', subject: data[0] });
});

/**
 * @swagger
 * /exams-with-subjects:
 *   get:
 *     summary: Get all exams with their subjects
 *     tags: [Exams]
 *     responses:
 *       200:
 *         description: Returns all exams and their related subjects
 */
router.get('/exams-with-subjects', async (req, res) => {
  const { data: exams, error } = await supabase.from('exams').select(`
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

  if (error) return res.status(500).json({ error: error.message });
  res.json({ exams });
});

module.exports = router;
