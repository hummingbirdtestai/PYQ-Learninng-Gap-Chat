const express = require('express');
const router = express.Router();

// 👇 Add this import (it was missing)
const adaptive = require('../controllers/adaptive.controller');

// Routes
router.get('/adaptive/pyq/start', adaptive.getAdaptiveStart);
router.get('/adaptive/pyq/resume', adaptive.getAdaptiveResume);
router.post('/adaptive/pyq/submit', adaptive.submitBatchResponses);

module.exports = router;
