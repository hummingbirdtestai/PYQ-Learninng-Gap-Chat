const express = require('express');
const router = express.Router();

const {
  generateMCQGraphFromInput,      // POST /mcqs/generate-from-input
  insertMCQGraphFromJson,         // POST /mcqs/insert-from-json
  saveDraftGraph,                 // POST /mcqs/graph/save-draft
  processMCQGraph                 // POST /mcqs/graph/process/:graphId
} = require('../controllers/mcq.controller');

// ✅ Route: Auto-generate MCQ graph from raw MCQ text using GPT
router.post('/mcqs/generate-from-input', generateMCQGraphFromInput);

// ✅ Route: Insert a full pre-generated MCQ graph JSON directly into DB
router.post('/mcqs/insert-from-json', insertMCQGraphFromJson);

// ✅ Route: Save GPT-generated graph as draft (raw JSON, no parsing yet)
router.post('/mcqs/graph/save-draft', saveDraftGraph);

// ✅ Route: Parse + process a saved graph into individual MCQs
router.post('/mcqs/graph/process/:graphId', processMCQGraph);

module.exports = router;
