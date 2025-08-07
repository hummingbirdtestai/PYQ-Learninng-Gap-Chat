const express = require('express');
const router = express.Router();

const mcqController = require('../controllers/mcq.controller');

const {
  generateMCQGraphFromInput,       // Legacy (optional)
  insertMCQGraphFromJson,          // Legacy (optional)
  generateAndSaveGraphDraft,       // ✅ Save draft graph from raw_text + subject_id
  processGraphById,                // ✅ Process graph and insert MCQs
  classifySubjects,                // ✅ Classify MCQs using GPT
  generatePrimaryMCQs,             // ✅ Generate primary MCQs and store in primary_mcq
  generateLevel1ForMCQBank,
  generateLevel2ForMCQBank,
  generateLevel3ForMCQBank,
  generateLevel4ForMCQBank,
  generateLevel5ForMCQBank,
  generateLevel6ForMCQBank,
  generateLevel7ForMCQBank,
  generateLevel8ForMCQBank,
  generateLevel9ForMCQBank,
  generateLevel10ForMCQBank
} = mcqController;

// ⚠️ Optional legacy routes — only enable if used
router.post('/mcqs/generate-from-input', generateMCQGraphFromInput);
router.post('/mcqs/insert-from-json', insertMCQGraphFromJson);

// ✅ GPT-based MCQ generation and processing
router.post('/mcqs/graph/save-draft', generateAndSaveGraphDraft);
router.post('/mcqs/graph/process/:graphId', processGraphById);

// ✅ Auto-classify MCQs by subject using GPT
router.post('/classify-subjects', classifySubjects);

// ✅ Generate Primary MCQs (Step 1 of recursion)
router.post('/mcqs/generate-primary', generatePrimaryMCQs);

// ✅ Generate Recursive Level 1 MCQs from learning_gap of primary_mcq
router.post('/mcqs/generate-level1-from-bank', generateLevel1ForMCQBank);

router.post('/mcqs/generate-level2', generateLevel2ForMCQBank);

// ✅ Generate Recursive Level 3 MCQs from level_2's learning gap
router.post('/mcqs/generate-level3', generateLevel3ForMCQBank);

router.post('/mcqs/generate-level4', generateLevel4ForMCQBank);

router.post('/mcqs/generate-level5', generateLevel5ForMCQBank);

router.post('/mcqs/generate-level6', generateLevel6ForMCQBank);

router.post('/mcqs/generate-level7', generateLevel7ForMCQBank);

router.post('/mcqs/generate-level8', generateLevel8ForMCQBank);

router.post('/mcqs/generate-level9', generateLevel9ForMCQBank);

router.post('/mcqs/generate-level10', generateLevel10ForMCQBank);


module.exports = router;
