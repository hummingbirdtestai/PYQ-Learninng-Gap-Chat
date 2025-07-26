const express = require('express');
const router = express.Router();
const { getAllColleges, getCollegeById } = require('../controllers/college.controller');

// GET all colleges
router.get('/', getAllColleges);

// GET one college by ID
router.get('/:id', getCollegeById);

module.exports = router;
