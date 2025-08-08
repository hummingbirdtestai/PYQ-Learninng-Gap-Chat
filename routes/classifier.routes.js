// routes/classifier.routes.js
const express = require('express');
const router = express.Router();
const ctrl = require('../controllers/classifier.controller');

router.post('/classify/subjects/run', ctrl.classifySubjectsRun);

module.exports = router;
