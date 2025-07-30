const express = require('express');
const router = express.Router();

// ✅ Import the function correctly
const { generateFromRaw } = require('../controllers/generation.controller');

// ✅ Route for external + internal MCQ generation
router.post('/generate-mcqs/from-raw', generateFromRaw);

module.exports = router;
