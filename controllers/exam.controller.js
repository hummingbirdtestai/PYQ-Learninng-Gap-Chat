const express = require('express');
const router = express.Router();
const examController = require('../controllers/exam.controller');

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
 *               code:
 *                 type: string
 *               description:
 *                 type: string
 *     responses:
 *       201:
 *         description: Exam created
 */
router.post('/', examController.createExam);

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
 *               code:
 *                 type: string
 *               description:
 *                 type: string
 *     responses:
 *       201:
 *         description: Subject created
 */
router.post('/:examId/subjects', examController.createSubjectUnderExam);

/**
 * @swagger
 * /exams/with-subjects:
 *   get:
 *     summary: Get all exams with their subjects
 *     tags: [Exams]
 *     responses:
 *       200:
 *         description: A list of exams with their subjects
 */
router.get('/with-subjects', examController.getExamsWithSubjects);

/**
 * @swagger
 * /exams:
 *   get:
 *     summary: Get all exams
 *     tags: [Exams]
 *     responses:
 *       200:
 *         description: List of all exams
 */
router.get('/', examController.getAllExams);

module.exports = router;
