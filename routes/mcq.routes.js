// MCQ Routes
const express = require('express');
const router = express.Router();
const { generateMCQGraphFromInput,insertMCQGraphFromJson } = require('../controllers/mcq.controller');

// POST /mcqs/generate-from-input
router.post('/mcqs/generate-from-input', generateMCQGraphFromInput);
router.post('/mcqs/insert-from-json', insertMCQGraphFromJson);

module.exports = router;
