// config/supabaseClient.js
const { createClient } = require('@supabase/supabase-js');

// ✅ Load from environment
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY; // server-only key

if (!supabaseUrl) {
  throw new Error("❌ Missing SUPABASE_URL in environment");
}
if (!supabaseServiceRoleKey) {
  throw new Error("❌ Missing SUPABASE_SERVICE_ROLE_KEY in environment. 
    Never use the anon key on backend, always use the service role key.");
}

// ✅ Create Supabase client with service role key
const supabase = createClient(supabaseUrl, supabaseServiceRoleKey, {
  auth: {
    persistSession: false, // backend doesn’t need to persist sessions
    autoRefreshToken: false,
  },
});

module.exports = { supabase };
