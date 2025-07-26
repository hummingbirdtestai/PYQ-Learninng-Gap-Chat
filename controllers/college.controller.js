const supabase = require('../config/supabaseClient');

/**
 * @swagger
 * /colleges:
 *   get:
 *     tags: [Colleges]
 *     summary: Get all medical colleges
 *     responses:
 *       200:
 *         description: List of colleges
 */
exports.getAllColleges = async (req, res) => {
  const { data, error } = await supabase
    .from('medical_colleges')
    .select('*')
    .order('name', { ascending: true });

  if (error) return res.status(500).json({ error: error.message });

  res.status(200).json(data);
};

/**
 * @swagger
 * /colleges/{id}:
 *   get:
 *     tags: [Colleges]
 *     summary: Get a college by ID
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: College found
 *       404:
 *         description: Not found
 */
exports.getCollegeById = async (req, res) => {
  const { id } = req.params;

  const { data, error } = await supabase
    .from('medical_colleges')
    .select('*')
    .eq('id', id)
    .single();

  if (error) return res.status(404).json({ error: 'College not found' });

  res.status(200).json(data);
};
