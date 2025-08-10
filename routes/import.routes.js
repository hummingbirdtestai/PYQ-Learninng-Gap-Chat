// routes/import.routes.js

const express = require('express');
const router = express.Router();

/**
 * @swagger
 * /api/import-mcqs:
 *   post:
 *     summary: Import MCQs from a public Google Sheet CSV
 *     tags:
 *       - Import MCQs
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - sheetUrl
 *               - examId
 *               - subjectId
 *             properties:
 *               sheetUrl:
 *                 type: string
 *                 example: "https://docs.google.com/spreadsheets/d/e/.../pub?output=csv"
 *               examId:
 *                 type: string
 *                 example: "a9c1e690-4f18-4c62-b3e9-f47384135a20"
 *               subjectId:
 *                 type: string
 *                 example: "e7b2e1e7-5cbe-4b4e-b5ea-d0741e51e0e1"
 *     responses:
 *       200:
 *         description: Successfully imported MCQs
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 message:
 *                   type: string
 *       400:
 *         description: Missing required fields
 *       500:
 *         description: Import failed
 */
router.post('/import-mcqs', async (req, res) => {
  const { sheetUrl, examId, subjectId } = req.body;

  if (!sheetUrl || !examId || !subjectId) {
    return res.status(400).json({ error: 'sheetUrl, examId, and subjectId are required.' });
  }

  const result = await importMCQs({ sheetUrl, examId, subjectId });

  if (result.success) {
    res.status(200).json({ message: `✅ Imported ${result.count} MCQs.` });
  } else {
    res.status(500).json({ error: result.error });
  }
});

module.exports = router;
