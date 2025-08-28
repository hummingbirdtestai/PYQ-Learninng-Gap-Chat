const express = require("express");
const router = express.Router();
const { saveAnswer } = require("../controllers/answers.controller");

// Save student answer
router.post("/answers", saveAnswer);

module.exports = router;
