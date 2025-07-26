// User Routes
const express = require('express');
const router = express.Router();
const { registerUser, getUserById } = require('../controllers/user.controller');

// Register new student
router.post('/register', registerUser);

// Get user profile by ID
router.get('/:id', getUserById);

module.exports = router;
