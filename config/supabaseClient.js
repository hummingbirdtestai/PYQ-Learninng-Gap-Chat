const { createClient } = require('@supabase/supabase-js');

console.log("🚀 SUPABASE_URL:", process.env.SUPABASE_URL);
console.log("🚀 SUPABASE_SERVICE_ROLE_KEY length:", process.env.SUPABASE_SERVICE_ROLE_KEY?.length);


const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY; // must be service role

if (!supabaseUrl || !supabaseKey) {
  console.error("❌ Missing Supabase env vars", { url: !!supabaseUrl, key: !!supabaseKey });
  throw new Error("Supabase configuration missing");
}

const supabase = createClient(supabaseUrl, supabaseKey, {
  auth: { persistSession: false, autoRefreshToken: false }
});

module.exports = { supabase };
