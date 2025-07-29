const express = require('express');
const router = express.Router();
const { 
  controller.getMCQGraph, 
  controller.submitResponses, 
  controller.submitQuizScore, 
  controller.getResumeProgress, 
  controller.updateProgress, 
  controller.getNextMCQBatch,
  adaptiveController.handleNextAction 
} = require('../controllers/adaptive.controller');

// 1. Deliver full MCQ graph for a subject in an exam
router.get('/adaptive/mcqs/:examId/:subjectId', controller.getMCQGraph);

// 2. Submit batch of user responses
router.post('/adaptive/responses/batch', controller.submitResponses);

// 3. Submit final quiz score
router.post('/adaptive/score/submit', controller.submitQuizScore);

// 4. Get progress to resume session
router.get('/adaptive/progress/:userId/:examId/:subjectId', controller.getResumeProgress);

// 5. Update progress after each MCQ or batch
router.post('/adaptive/progress/update', controller.updateProgress);

// ✅ 6. Fetch next batch of 20 MCQs for user (skips completed ones)
router.get('/adaptive-mcqs/next-batch', controller.getNextMCQBatch);

router.post('/adaptive/mcqs/next-action', adaptiveController.handleNextAction);

module.exports = router;
