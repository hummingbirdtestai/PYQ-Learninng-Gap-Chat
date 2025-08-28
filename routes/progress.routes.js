const express = require("express");
const router = express.Router();
const { saveProgress } = require("../controllers/progress.controller");

router.post("/progress", saveProgress);

module.exports = router;
