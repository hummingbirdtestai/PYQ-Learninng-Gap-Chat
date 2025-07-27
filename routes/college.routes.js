const express = require('express');
const router = express.Router();

const {
  getAllColleges,
  getCollegeById,
  createCollege
} = require('../controllers/college.controller');

// ✅ GET all colleges
router.get('/', getAllColleges);

// ✅ GET one college by ID
router.get('/:id', getCollegeById);

// ✅ POST a new college
router.post('/', createCollege);

module.exports = router;
