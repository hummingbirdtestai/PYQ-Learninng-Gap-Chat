// controllers/user.controller.js
const { supabase } = require('../config/supabaseClient');
const { last10 } = require('../utils/phone'); // ← make sure this exists (you shared it earlier)

/**
 * @swagger
 * /users/register:
 *   post:
 *     tags:
 *       - Users
 *     summary: Register a new student (minimal fields)
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - country_code
 *               - phone
 *               - name
 *             properties:
 *               country_code:
 *                 type: string
 *                 description: E.164 country code
 *                 example: '+91'
 *               phone:
 *                 type: string
 *                 description: 10-digit local phone number (no country code)
 *                 example: '9999999999'
 *               name:
 *                 type: string
 *                 example: 'John Doe'
 *     responses:
 *       201:
 *         description: User registered successfully
 *       400:
 *         description: Invalid input
 *       409:
 *         description: User already registered
 *       500:
 *         description: Registration failed
 */
async function registerUser(req, res) {
  try {
    const { country_code, phone, name } = req.body || {};

    const cc = String(country_code || '').trim();
    const ten = last10(phone || '');
    const fullName = String(name || '').trim();

    if (!cc || !ten || !fullName) {
      return res
        .status(400)
        .json({ error: 'Missing required fields: country_code, phone, and name are required' });
    }
    if (cc !== '+91') {
      return res.status(400).json({ error: 'Unsupported country_code. Only +91 is allowed.' });
    }
    if (!/^\d{10}$/.test(ten)) {
      return res.status(400).json({ error: 'Phone must be a 10-digit number' });
    }

    // Duplicate check by phone
    const { data: existing, error: checkErr } = await supabase
      .from('users')
      .select('id')
      .eq('phone', ten)
      .limit(1);

    if (checkErr) return res.status(500).json({ error: checkErr.message || 'Database error' });
    if (Array.isArray(existing) && existing.length > 0) {
      return res.status(409).json({ error: 'User already registered' });
    }

    // Insert minimal row
    const { data, error } = await supabase
      .from('users')
      .insert({
        country_code: cc,
        phone: ten,
        name: fullName,
        is_active: false, // default for new registrations
      })
      .select()
      .single();

    if (error) return res.status(500).json({ error: error.message || 'Registration failed' });

    return res.status(201).json(data);
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'Registration failed' });
  }
}

/**
 * @swagger
 * /users/{id}:
 *   get:
 *     tags:
 *       - Users
 *     summary: Get user profile
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: User profile
 *       404:
 *         description: User not found
 *       500:
 *         description: Error fetching user
 */
async function getUserById(req, res) {
  const { id } = req.params;
  try {
    const { data, error } = await supabase.from('users').select('*').eq('id', id).maybeSingle();
    if (error) return res.status(500).json({ error: error.message });
    if (!data) return res.status(404).json({ error: 'User not found' });
    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'Error fetching user' });
  }
}

/**
 * @swagger
 * /users/phone/{phone}:
 *   get:
 *     tags:
 *       - Users
 *     summary: Get full user details by phone number (10 digits)
 *     parameters:
 *       - in: path
 *         name: phone
 *         required: true
 *         schema:
 *           type: string
 *           example: '9876543210'
 *     responses:
 *       200:
 *         description: Full user profile
 *       404:
 *         description: User not found
 *       400:
 *         description: Bad phone format
 */
async function getUserByPhone(req, res) {
  const ten = last10(req.params.phone || '');
  if (!/^\d{10}$/.test(ten)) {
    return res.status(400).json({ error: 'Phone must be a 10-digit number' });
  }

  try {
    const { data, error } = await supabase.from('users').select('*').eq('phone', ten).maybeSingle();
    if (error) return res.status(500).json({ error: error.message });
    if (!data) return res.status(404).json({ error: 'User not found' });
    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'Error fetching user' });
  }
}

/**
 * @swagger
 * /users/status/{phone}:
 *   get:
 *     tags:
 *       - Users
 *     summary: Check if user is active (by 10-digit phone)
 *     parameters:
 *       - in: path
 *         name: phone
 *         required: true
 *         schema:
 *           type: string
 *           example: '9999999999'
 *     responses:
 *       200:
 *         description: Activation status
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 phone:
 *                   type: string
 *                 is_active:
 *                   type: boolean
 *       404:
 *         description: User not found
 *       400:
 *         description: Bad phone format
 */
async function getUserStatusByPhone(req, res) {
  const ten = last10(req.params.phone || '');
  if (!/^\d{10}$/.test(ten)) {
    return res.status(400).json({ error: 'Phone must be a 10-digit number' });
  }

  try {
    const { data, error } = await supabase
      .from('users')
      .select('phone, is_active')
      .eq('phone', ten)
      .maybeSingle();

    if (error) return res.status(500).json({ error: error.message });
    if (!data) return res.status(404).json({ error: 'User not found' });

    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'Error fetching status' });
  }
}

/**
 * @swagger
 * /users/phone/{phone}/toggle-activation:
 *   patch:
 *     tags:
 *       - Users
 *     summary: Toggle user's active status by 10-digit phone
 *     parameters:
 *       - in: path
 *         name: phone
 *         required: true
 *         schema:
 *           type: string
 *           example: '9876543210'
 *     responses:
 *       200:
 *         description: is_active status toggled
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 phone:
 *                   type: string
 *                 is_active:
 *                   type: boolean
 *       404:
 *         description: User not found
 *       400:
 *         description: Bad phone format
 */
async function toggleActivationByPhone(req, res) {
  const ten = last10(req.params.phone || '');
  if (!/^\d{10}$/.test(ten)) {
    return res.status(400).json({ error: 'Phone must be a 10-digit number' });
  }

  try {
    const { data: user, error: fetchErr } = await supabase
      .from('users')
      .select('is_active')
      .eq('phone', ten)
      .maybeSingle();

    if (fetchErr) return res.status(500).json({ error: fetchErr.message });
    if (!user) return res.status(404).json({ error: 'User not found' });

    const next = !user.is_active;

    const { data, error: updateErr } = await supabase
      .from('users')
      .update({ is_active: next })
      .eq('phone', ten)
      .select('phone, is_active')
      .maybeSingle();

    if (updateErr) return res.status(500).json({ error: updateErr.message });

    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'Error updating user' });
  }
}

// ✅ default export (CommonJS)
exports.default = {
  registerUser,
  getUserById,
  getUserByPhone,
  getUserStatusByPhone,
  toggleActivationByPhone,
};
