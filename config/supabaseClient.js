// config/supabaseClient.js
const { createClient } = require('@supabase/supabase-js');

const supabaseUrl =
  process.env.SUPABASE_URL;

const supabaseKey =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||   // ✅ preferred on server
  process.env.SUPABASE_ANON_KEY ||           // ok if you only have anon
  process.env.SUPABASE_KEY;                  // legacy var name fallback

if (!supabaseUrl) {
  throw new Error('Missing SUPABASE_URL');
}
if (!supabaseKey) {
  throw new Error('Missing Supabase key. Set SUPABASE_SERVICE_ROLE_KEY or SUPABASE_ANON_KEY.');
}

const supabase = createClient(supabaseUrl, supabaseKey, {
  auth: { persistSession: false },
});

module.exports = { supabase };
