// routes/exam.routes.js
const express = require('express');
const router = express.Router();
const examController = require('../controllers/exam.controller');

// ✅ Create a new exam
// POST /exams
router.post('/', examController.createExam);

// ✅ Create a subject under a given exam
// POST /exams/:examId/subjects
router.post('/:examId/subjects', examController.createSubjectUnderExam);

// ✅ Get all exams with their subjects
// GET /exams/with-subjects
router.get('/with-subjects', examController.getExamsWithSubjects);

// ✅ Get all exams (flat list)
// GET /exams
router.get('/', examController.getAllExams);

module.exports = router;
