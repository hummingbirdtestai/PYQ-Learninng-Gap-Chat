const express = require('express');
const { getDailyBriefing } = require('../controllers/briefing.controller');
const { authenticate } = require('../middleware/auth'); // your JWT middleware

const router = express.Router();

// Daily briefing route (protected)
router.get('/daily-briefing', authenticate, getDailyBriefing);

module.exports = router;
