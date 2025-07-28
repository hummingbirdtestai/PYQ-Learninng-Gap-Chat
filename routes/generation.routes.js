const express = require('express');
const router = express.Router();
const { queueMCQGeneration, getGenerationStatus } = require('../controllers/generation.controller');

router.post('/queue', queueMCQGeneration);
router.get('/status', getGenerationStatus);

module.exports = router;
