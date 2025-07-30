// MCQ Routes
const express = require('express');
const router = express.Router();
const {
  generateMCQGraphFromInput,
  insertMCQGraphFromJson
} = require('../controllers/mcq.controller');

// POST: Auto-generate graph from raw MCQ text using GPT
router.post('/mcqs/generate-from-input', generateMCQGraphFromInput);

// POST: Insert pre-generated graph JSON into database
router.post('/mcqs/insert-from-json', insertMCQGraphFromJson);

module.exports = router;
