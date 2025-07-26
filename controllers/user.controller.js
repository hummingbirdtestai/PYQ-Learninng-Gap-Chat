// User Controller
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
 *               - medical_college_id
 *               - year_of_joining
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
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 id:
 *                   type: string
 *                 phone:
 *                   type: string
 *                 country_code:
 *                   type: string
 *                 email:
 *                   type: string
 *                 name:
 *                   type: string
 *                 medical_college_id:
 *                   type: string
 *                 year_of_joining:
 *                   type: integer
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

  const { data, error } = await supabase.from('users').insert({
    country_code,
    phone,
    email,
    name,
    photograph_url,
    medical_college_id,
    year_of_joining
  }).select().single();

  if (error) return res.status(500).json({ error: error.message });

  res.status(201).json(data);
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
 */
exports.getUserById = async (req, res) => {
  const { id } = req.params;

  const { data, error } = await supabase
    .from('users')
    .select('*')
    .eq('id', id)
    .single();

  if (error) return res.status(500).json({ error: error.message });

  res.status(200).json(data);
};
