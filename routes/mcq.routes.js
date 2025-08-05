// routes/mcq.routes.js

const express = require('express');
const router = express.Router();

const mcqController = require('../controllers/mcq.controller');

const {
  generateMCQGraphFromInput,       // Optional legacy API — not required if not using
  insertMCQGraphFromJson,          // Optional legacy API — not required if not using
  generateAndSaveGraphDraft,       // ✅ NEW: Save draft graph from raw_text + subject_id
  processGraphById                 // ✅ NEW: Process graph and insert MCQs
} = mcqController;

// ⚠️ Optional legacy routes (comment out if not used)
router.post('/mcqs/generate-from-input', generateMCQGraphFromInput);
router.post('/mcqs/insert-from-json', insertMCQGraphFromJson);

// ✅ Save GPT-generated MCQ graph draft (raw_text + subject_id)
router.post('/mcqs/graph/save-draft', generateAndSaveGraphDraft);

// ✅ Process the draft graph and insert MCQs into `mcqs` table
router.post('/mcqs/graph/process/:graphId', processGraphById);

// ✅ New: Classify untagged MCQs into MBBS subjects using GPT
router.post('/classify-subjects', mcqController.classifySubjects);

module.exports = router;
