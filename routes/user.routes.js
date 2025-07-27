const express = require('express');
const router = express.Router();
const {
  registerUser,
  getUserById,
  getUserStatusByPhone
} = require('../controllers/user.controller');

// Register new student
router.post('/register', registerUser);

// Get user profile by ID
router.get('/:id', getUserById);

// Check activation status by phone
router.get('/status/:phone', getUserStatusByPhone);

router.patch('/:id/toggle-activation', toggleActivationStatus);

module.exports = router;
