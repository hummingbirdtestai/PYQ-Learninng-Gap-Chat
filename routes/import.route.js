const express = require('express');
const router = express.Router();
const { importMCQs } = require('../services/importMCQsFromSheet');

router.post('/import-mcqs', async (req, res) => {
  const { sheetUrl, examId, subjectId } = req.body;

  if (!sheetUrl || !examId || !subjectId) {
    return res.status(400).json({ error: 'sheetUrl, examId, and subjectId are required.' });
  }

  const result = await importMCQs({ sheetUrl, examId, subjectId });

  if (result.success) {
    res.status(200).json({ message: `Imported ${result.count} MCQs.` });
  } else {
    res.status(500).json({ error: result.error });
  }
});

module.exports = router;
