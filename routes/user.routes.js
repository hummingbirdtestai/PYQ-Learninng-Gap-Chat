// routes/user.routes.js
const express = require('express');
const router = express.Router();

const userController = require('../controllers/user.controller').default;

router.post('/register', userController.registerUser);
router.get('/:id', userController.getUserById);
router.get('/status/:phone', userController.getUserStatusByPhone);
router.patch('/phone/:phone/toggle-activation', userController.toggleActivationByPhone);
router.get('/phone/:phone', userController.getUserByPhone);

module.exports = router;
