const supabase = require('../config/supabaseClient');

/**
 * @swagger
 * /users/register:
 *   post:
 *     tags:
 *       - Users
 *     summary: Register student
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - country_code
 *               - phone
 *               - email
 *               - name
 *             properties:
 *               country_code:
 *                 type: string
 *                 example: '+91'
 *               phone:
 *                 type: string
 *                 example: '9999999999'
 *               email:
 *                 type: string
 *                 example: 'student@example.com'
 *               name:
 *                 type: string
 *                 example: 'John Doe'
 *               photograph_url:
 *                 type: string
 *                 example: 'https://example.com/photo.jpg'
 *               medical_college_id:
 *                 type: string
 *                 format: uuid
 *               year_of_joining:
 *                 type: integer
 *                 example: 2022
 *     responses:
 *       201:
 *         description: User registered successfully
 *       409:
 *         description: User already registered
 *       500:
 *         description: Registration failed
 */
exports.registerUser = async (req, res) => {
  const {
    country_code,
    phone,
    email,
    name,
    photograph_url,
    medical_college_id,
    year_of_joining
  } = req.body;

  if (!country_code || !phone || !email || !name) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  try {
    const { data: existingUser, error: checkError } = await supabase
      .from('users')
      .select('id')
      .eq('phone', phone)
      .limit(1);

    if (checkError) return res.status(500).json({ error: checkError.message });

    if (existingUser.length > 0) {
      return res.status(409).json({ error: 'User already registered' });
    }

    const { data, error } = await supabase
      .from('users')
      .insert({
        country_code,
        phone,
        email,
        name,
        photograph_url: photograph_url || null,
        medical_college_id: medical_college_id || null,
        year_of_joining: year_of_joining || null,
        is_active: false
      })
      .select()
      .single();

    if (error) return res.status(500).json({ error: error.message });

    res.status(201).json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

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
 *       500:
 *         description: Error fetching user
 */
exports.getUserById = async (req, res) => {
  const { id } = req.params;

  try {
    const { data, error } = await supabase
      .from('users')
      .select('*')
      .eq('id', id)
      .single();

    if (error) return res.status(500).json({ error: error.message });

    res.status(200).json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

/**
 * @swagger
 * /users/phone/{phone}:
 *   get:
 *     tags:
 *       - Users
 *     summary: Get full user details by phone number
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
 */
exports.getUserByPhone = async (req, res) => {
  let { phone } = req.params;

  // ✅ Normalize phone to exclude any +91 or +XX prefix
  if (phone.startsWith('+91')) {
    phone = phone.substring(3);
  }

  try {
    const { data, error } = await supabase
      .from('users')
      .select('*')
      .eq('phone', phone)
      .single();

    if (error || !data) {
      return res.status(404).json({ error: 'User not found' });
    }

    res.status(200).json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

/**
 * @swagger
 * /users/status/{phone}:
 *   get:
 *     tags:
 *       - Users
 *     summary: Check if user is active
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
 */
exports.getUserStatusByPhone = async (req, res) => {
  const { phone } = req.params;

  try {
    const { data, error } = await supabase
      .from('users')
      .select('phone, is_active')
      .eq('phone', phone)
      .single();

    if (error || !data) {
      return res.status(404).json({ error: 'User not found' });
    }

    res.status(200).json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

/**
 * @swagger
 * /users/phone/{phone}/toggle-activation:
 *   patch:
 *     tags: [Users]
 *     summary: Toggle user's active status using phone number
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
 */
exports.toggleActivationByPhone = async (req, res) => {
  const { phone } = req.params;

  try {
    const { data: user, error } = await supabase
      .from('users')
      .select('is_active')
      .eq('phone', phone)
      .single();

    if (error || !user) {
      return res.status(404).json({ error: 'User not found' });
    }

    const newStatus = !user.is_active;

    const { data, updateError } = await supabase
      .from('users')
      .update({ is_active: newStatus })
      .eq('phone', phone)
      .select('phone, is_active')
      .single();

    if (updateError) return res.status(500).json({ error: updateError.message });

    res.status(200).json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
