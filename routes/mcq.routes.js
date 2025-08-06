const express = require('express');
const router = express.Router();

const mcqController = require('../controllers/mcq.controller');

const {
  generateMCQGraphFromInput,       // Optional legacy API — not required if not using
  insertMCQGraphFromJson,          // Optional legacy API — not required if not using
  generateAndSaveGraphDraft,       // ✅ Save draft graph from raw_text + subject_id
  processGraphById,                // ✅ Process graph and insert MCQs
  classifySubjects,                // ✅ Classify MCQs to MBBS subjects using GPT
  generatePrimaryMCQs              // ✅ NEW: Generate primary MCQs via GPT and store in `primary_mcq` column
} = mcqController;

// ⚠️ Optional legacy routes (comment out if not used)
router.post('/mcqs/generate-from-input', generateMCQGraphFromInput);
router.post('/mcqs/insert-from-json', insertMCQGraphFromJson);

// ✅ Save GPT-generated MCQ graph draft (raw_text + subject_id)
router.post('/mcqs/graph/save-draft', generateAndSaveGraphDraft);

// ✅ Process the draft graph and insert MCQs into `mcqs` table
router.post('/mcqs/graph/process/:graphId', processGraphById);

// ✅ Classify untagged MCQs into MBBS subjects using GPT
router.post('/classify-subjects', classifySubjects);

// ✅ NEW: Generate primary MCQs and store in `primary_mcq` column (based on prompt)
router.post('/mcqs/generate-primary', generatePrimaryMCQs);

module.exports = router;
