const express = require('express');
const router = express.Router();

const {
  registerUser,
  getUserById,
  getUserStatusByPhone,     // ✅ Added
  toggleActivationByPhone,  // ✅ Added
  getUserByPhone            // ✅ Added
} = require('../controllers/user.controller');

router.post('/register', registerUser);
router.get('/:id', getUserById);
router.get('/status/:phone', getUserStatusByPhone); // ✅ Status by phone
router.patch('/phone/:phone/toggle-activation', toggleActivationByPhone); // ✅ Toggle activation
router.get('/phone/:phone', getUserByPhone); // ✅ Get user by phone

module.exports = router;
