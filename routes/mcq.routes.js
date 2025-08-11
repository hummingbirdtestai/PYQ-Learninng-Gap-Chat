const express = require('express');
const router = express.Router();

// Main controller (everything except primary generation)
const mcqController = require('../controllers/mcq.controller');

// New dedicated controller for concurrent primary generation (gpt-5-mini)
const primaryGen = require('../controllers/primaryGen.controller');

const {
  generateMCQGraphFromInput,       // Legacy (optional)
  insertMCQGraphFromJson,          // Legacy (optional)
  generateAndSaveGraphDraft,       // ✅ Save draft graph from raw_text + subject_id
  processGraphById,                // ✅ Process graph and insert MCQs
  classifySubjects,                // ✅ Classify MCQs using GPT (subject text update)
  // 🔁 DO NOT take generatePrimaryMCQs from mcqController anymore
  generateLevel1ForMCQBank,
  generateLevel2ForMCQBank,
  generateLevel3ForMCQBank,
  generateLevel4ForMCQBank,
  generateLevel5ForMCQBank,
  generateLevel6ForMCQBank,
  generateLevel7ForMCQBank,
  generateLevel8ForMCQBank,
  generateLevel9ForMCQBank,
  classifySubjectsV2
} = mcqController;

// ⚠️ Optional legacy routes — only enable if used
router.post('/mcqs/generate-from-input', generateMCQGraphFromInput);
router.post('/mcqs/insert-from-json', insertMCQGraphFromJson);

// ✅ GPT-based MCQ generation and processing
router.post('/mcqs/graph/save-draft', generateAndSaveGraphDraft);
router.post('/mcqs/graph/process/:graphId', processGraphById);

// ✅ Auto-classify MCQs by subject using GPT
router.post('/classify-subjects', classifySubjects);

// ✅ Generate Primary MCQs (concurrent, multi-worker safe; uses gpt-5-mini)
//    Query params supported: ?limit=40&concurrency=3
router.post('/mcqs/generate-primary', primaryGen.generatePrimaryMCQs);

// ✅ Generate Recursive Level 1 MCQs from learning_gap of primary_mcq
router.post('/mcqs/generate-level1-from-bank', generateLevel1ForMCQBank);

router.post('/mcqs/generate-level2-from-bank', generateLevel2ForMCQBank);

// ✅ Generate Recursive Level 3 MCQs from level_2's learning gap
router.post('/mcqs/generate-level3', generateLevel3ForMCQBank);

router.post('/mcqs/generate-level4', generateLevel4ForMCQBank);

router.post('/mcqs/generate-level5', generateLevel5ForMCQBank);

router.post('/mcqs/generate-level6', generateLevel6ForMCQBank);

router.post('/mcqs/generate-level7', generateLevel7ForMCQBank);

router.post('/mcqs/generate-level8', generateLevel8ForMCQBank);

router.post('/mcqs/generate-level9', generateLevel9ForMCQBank);

router.post('/classify-subjects-v2', classifySubjectsV2);

module.exports = router;
