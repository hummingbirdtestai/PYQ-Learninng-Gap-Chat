const express = require('express');
const router = express.Router();
const { generateMCQGraphFromRaw } = require('../controllers/mcqGenerator.controller');

router.post('/generate-mcqs/from-raw', generateMCQGraphFromRaw);

module.exports = router;
