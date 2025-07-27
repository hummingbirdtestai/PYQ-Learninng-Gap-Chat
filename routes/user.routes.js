const express = require('express');
const router = express.Router();
const {
  registerUser,
  getUserById,
  getUserStatusByPhone,
  toggleActivationByPhone,
  getUserByPhone
} = require('../controllers/user.controller');

router.post('/register', registerUser);
router.get('/:id', getUserById);
router.get('/status/:phone', getUserStatusByPhone);
router.patch('/phone/:phone/toggle-activation', toggleActivationByPhone); // ✅ NEW
router.get('/phone/:phone', getUserByPhone);

module.exports = router;
