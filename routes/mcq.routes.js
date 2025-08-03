const express = require('express');
const router = express.Router();

const {
  generateMCQGraphFromInput,      // /mcqs/generate-from-input
  insertMCQGraphFromJson          // /mcqs/insert-from-json
} = require('../controllers/mcq.controller');

const {
  saveDraftMCQGraph,              // /mcqs/graph/save-draft
  processMCQGraphById             // /mcqs/graph/process/:graphId
} = require('../controllers/mcqGraph.controller');

// ✅ Existing routes
router.post('/mcqs/generate-from-input', generateMCQGraphFromInput);
router.post('/mcqs/insert-from-json', insertMCQGraphFromJson);

// ✅ New routes for Graph draft + processing
router.post('/mcqs/graph/save-draft', saveDraftMCQGraph);
router.post('/mcqs/graph/process/:graphId', processMCQGraphById);

module.exports = router;
