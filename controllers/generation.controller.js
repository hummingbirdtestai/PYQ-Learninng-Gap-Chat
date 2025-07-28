const { supabase } = require('../config/supabaseClient');

exports.getGenerationStatus = async (req, res) => {
  const { data, error } = await supabase
    .from('mcq_generation_queue')
    .select('status, count(*)')
    .group('status');

  if (error) return res.status(500).json({ error });

  res.json({ statusCounts: data });
};
