const express = require('express');
const router = express.Router();

const {
  getMCQGraph,
  submitResponses,
  submitQuizScore,
  getResumeProgress,
  updateProgress,
  getNextMCQBatch,
  handleNextAction
} = require('../controllers/adaptive.controller');

// Routes
router.get('/adaptive/mcqs/:examId/:subjectId', getMCQGraph);
router.post('/adaptive/responses/batch', submitResponses);
router.post('/adaptive/score/submit', submitQuizScore);
router.get('/adaptive/progress/:userId/:examId/:subjectId', getResumeProgress);
router.post('/adaptive/progress/update', updateProgress);
router.get('/adaptive-mcqs/next-batch', getNextMCQBatch);
router.post('/adaptive/mcqs/next-action', handleNextAction);

module.exports = router;
