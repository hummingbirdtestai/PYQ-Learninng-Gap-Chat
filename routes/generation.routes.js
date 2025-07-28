const express = require('express');
const router = express.Router();
const { queueMCQGeneration } = require('../controllers/generation.controller');

router.post('/queue', queueMCQGeneration);

module.exports = router;
