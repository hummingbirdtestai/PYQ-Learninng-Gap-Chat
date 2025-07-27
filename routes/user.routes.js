const express = require('express');
const router = express.Router();
const {
  registerUser,
  getUserById,
  getUserStatusByPhone,
  toggleUserActivation
} = require('../controllers/user.controller');

router.post('/register', registerUser);
router.get('/:id', getUserById);
router.get('/status/:phone', getUserStatusByPhone);
router.patch('/:id/toggle-activation', toggleUserActivation); // ✅ NEW

module.exports = router;
