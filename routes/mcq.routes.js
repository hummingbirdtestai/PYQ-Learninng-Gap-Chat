const express = require('express');
const router = express.Router();

const {
  generateAndSaveGraphDraft,   // ✅ NEW: Save draft graph from raw_text + subject_id
  processGraphById             // ✅ NEW: Process graph and insert MCQs
} = require('../controllers/mcq.controller');

// ✅ Save GPT-generated MCQ graph draft (raw_text + subject_id)
router.post('/mcqs/graph/save-draft', generateAndSaveGraphDraft);

// ✅ Process the draft graph and insert MCQs into `mcqs` table
router.post('/mcqs/graph/process/:graphId', processGraphById);

module.exports = router;
