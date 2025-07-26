// Auth Routes
const express = require('express');
const router = express.Router();
const { startOTP, verifyOTP } = require('../controllers/auth.controller');

router.post('/otp/start', startOTP);
router.post('/otp/verify', verifyOTP);

module.exports = router;
