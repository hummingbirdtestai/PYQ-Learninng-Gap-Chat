// routes/exam.routes.js
const express = require('express');
const router = express.Router();
const examController = require('../controllers/exam.controller');

// POST /exams – Create a new exam
router.post('/exams', examController.createExam);

// POST /subjects – Create a subject under a given exam
router.post('/subjects', examController.createSubject);

// GET /exams-with-subjects – Get all exams with their subjects
router.get('/exams-with-subjects', examController.getExamsWithSubjects);

module.exports = router;
