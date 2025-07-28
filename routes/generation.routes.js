const express = require('express');
const router = express.Router();
const controller = require('../controllers/generation.controller');

router.post('/queue', controller.queueMCQGeneration);
router.get('/status', controller.getGenerationStatus);
router.get('/results', controller.getGeneratedResults);

module.exports = router;
