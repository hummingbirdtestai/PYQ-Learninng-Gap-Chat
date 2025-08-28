const express = require("express");
const { saveAnswer } = require("../controllers/answersController");

const router = express.Router();

// POST /answers
router.post("/", saveAnswer);

module.exports = router;
