const express = require('express');
const router = express.Router();

const mcqController = require('../controllers/mcq.controller');

const {
  // Legacy or optional routes — use only if needed
  generateMCQGraphFromInput,       // 🔁 Convert raw MCQ text into full MCQ graph using GPT
  insertMCQGraphFromJson,          // 🔁 Insert full MCQ graph manually via JSON

  // GPT-based MCQ Generation and Processing
  generateAndSaveGraphDraft,       // ✅ Save raw MCQ graph draft (unprocessed)
  processGraphById,                // ✅ Process graph: insert all MCQs into database

  // Subject Classification
  classifySubjects,                // ✅ Auto-classify MCQs into 19 MBBS subjects

  // Primary MCQ Generation
  generatePrimaryMCQs,             // ✅ Generate formatted primary_mcq from raw input

  // Recursive Learning Gap Remediation
  generateLevel1ForMCQBank         // ✅ Generate Level 1 MCQ for each primary MCQ
} = mcqController;

// -----------------------------------------
// 🧠 GPT-BASED MCQ FLOW ROUTES
// -----------------------------------------

// 🧠 GPT: Generate MCQ graph from raw text
router.post('/mcqs/generate-from-input', generateMCQGraphFromInput);

// 🧠 GPT: Insert MCQ graph via JSON
router.post('/mcqs/insert-from-json', insertMCQGraphFromJson);

// 🧠 GPT: Save raw graph as draft
router.post('/mcqs/graph/save-draft', generateAndSaveGraphDraft);

// 🧠 GPT: Process graph by ID and insert MCQs
router.post('/mcqs/graph/process/:graphId', processGraphById);

// 🧠 GPT: Auto-classify MCQs by subject
router.post('/classify-subjects', classifySubjects);

// 🧠 GPT: Generate primary_mcq from unstructured text
router.post('/mcqs/generate-primary', generatePrimaryMCQs);

// 🧠 GPT: Generate Level 1 MCQ from learning_gap (Recursive)
router.post('/mcqs/generate-level1', generateLevel1ForMCQBank);

module.exports = router;
