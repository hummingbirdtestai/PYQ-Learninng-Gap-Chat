const express = require('express');
const router = express.Router();
const generationController = require('../controllers/generation.controller');

// ✅ POST /generation/queue — Queue MCQs for generation
router.post('/queue', generationController.queueMCQGeneration);

// ✅ GET /generation/status — Get generation queue status
router.get('/status', generationController.getGenerationStatus);

// ✅ GET /generation/results — Get all generated MCQ outputs
router.get('/results', generationController.getGenerationResults);

module.exports = router;
