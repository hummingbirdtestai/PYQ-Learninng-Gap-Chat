// routes/exam.routes.js
const express = require('express');
const router = express.Router();
const examController = require('../controllers/exam.controller');

// ❌ WRONG → maybe you're doing this:
router.post('/exams', examController.createExam); // <- If createExam is undefined

// ✅ RIGHT → Only if createExam is a valid function
router.post('/', examController.createExam);
router.post('/:examId/subjects', examController.createSubjectUnderExam);
router.get('/exams-with-subjects', examController.getExamsWithSubjects);
router.get('/', examController.getAllExams);

module.exports = router;
