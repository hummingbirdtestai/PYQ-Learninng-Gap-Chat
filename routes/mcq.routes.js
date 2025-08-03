// routes/mcq.routes.js

const express = require('express');
const router = express.Router();

const {
  generateMCQGraphFromInput,      // POST /mcqs/generate-from-input
  insertMCQGraphFromJson,         // POST /mcqs/insert-from-json
  saveDraftGraph,                 // POST /mcqs/graph/save-draft
  processMCQGraph                 // POST /mcqs/graph/process/:graphId
} = require('../controllers/mcq.controller');

// ✅ Auto-generate MCQ graph from raw input using GPT
router.post('/mcqs/generate-from-input', generateMCQGraphFromInput);

// ✅ Insert a pre-generated graph JSON manually
router.post('/mcqs/insert-from-json', insertMCQGraphFromJson);

// ✅ Save a GPT-generated graph as draft (no parsing yet)
router.post('/mcqs/graph/save-draft', saveDraftGraph);

// ✅ Process a saved graph and insert individual MCQs
router.post('/mcqs/graph/process/:graphId', processMCQGraph);

module.exports = router;
