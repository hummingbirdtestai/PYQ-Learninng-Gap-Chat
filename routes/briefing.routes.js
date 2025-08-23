const express = require('express');
const { getDailyBriefing } = require('../controllers/briefing.controller');
const { authenticate } = require('../middleware/auth'); // ✅ JWT middleware

const router = express.Router();

// ✅ Daily briefing route (protected with JWT)
router.get('/daily-briefing', authenticate, getDailyBriefing);

module.exports = router;
