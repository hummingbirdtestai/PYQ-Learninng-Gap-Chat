
const express = require('express');
const router = express.Router();

const {
  getMCQGraph,
  submitResponsesBatch, // ✅ Corrected function name
  submitQuizScore,
  getResumeProgress,
  updateProgress,
  getNextMCQBatch,
  handleNextAction,
  submitBatchResponses,
  getAdaptiveStart
} = require('../controllers/adaptive.controller');

// Routes
router.get('/adaptive/mcqs/:examId/:subjectId', getMCQGraph);
router.post('/adaptive/responses/batch', submitResponsesBatch); // ✅ Updated here too
router.post('/adaptive/score/submit', submitQuizScore);
router.get('/adaptive/progress/:userId/:examId/:subjectId', getResumeProgress);
router.post('/adaptive/progress/update', updateProgress);
router.get('/adaptive-mcqs/next-batch', getNextMCQBatch);
router.post('/adaptive/mcqs/next-action', handleNextAction);
router.get('/adaptive/pyq/start', getAdaptiveStart);
router.post('/adaptive/pyq/submit', submitBatchResponses);

module.exports = router;
