
const express = require('express');
const router = express.Router();

const {
  getAdaptiveStart,
  getAdaptiveResume,
  submitBatchResponses,
} = require('../controllers/adaptive.controller');

// Routes
router.get('/adaptive/pyq/start', adaptive.getAdaptiveStart);
router.get('/adaptive/pyq/resume', adaptive.getAdaptiveResume); // optional, but handy
router.post('/adaptive/pyq/submit', adaptive.submitBatchResponses);

module.exports = router;
