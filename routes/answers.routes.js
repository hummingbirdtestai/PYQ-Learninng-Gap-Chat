const express = require("express");
const router = express.Router();
const { saveAnswer } = require("../controllers/answers.controller");

router.post("/answers", saveAnswer);

module.exports = router;
