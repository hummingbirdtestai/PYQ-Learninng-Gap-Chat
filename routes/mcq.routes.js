// MCQ Routes
const express = require('express');
const router = express.Router();
const { generateMCQGraphFromInput } = require('../controllers/mcq.controller');

// POST /mcqs/generate-from-input
router.post('/mcqs/generate-from-input', generateMCQGraphFromInput);

module.exports = router;
