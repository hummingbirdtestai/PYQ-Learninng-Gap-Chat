const express = require('express');
const router = express.Router();

// import the controller
const { cleanGraphsForMCQBank } = require('../controllers/cleanGraphsForMCQBank');

// route → GET /api/clean-graphs
router.get('/clean-graphs', cleanGraphsForMCQBank);

module.exports = router;
