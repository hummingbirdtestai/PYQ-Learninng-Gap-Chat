
const express = require('express');
const router = express.Router();

const {
  submitBatchResponses,
  getAdaptiveStart
} = require('../controllers/adaptive.controller');

// Routes
router.get('/adaptive/pyq/start', getAdaptiveStart);
router.post('/adaptive/pyq/submit', submitBatchResponses);

module.exports = router;
