const express = require('express');
const router = express.Router();
const controller = require('../controllers/generation.controller');

router.get('/status', controller.getGenerationStatus);

module.exports = router;
