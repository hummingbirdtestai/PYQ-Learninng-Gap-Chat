// middleware/auth.js
const { createClient } = require('@supabase/supabase-js');

// ✅ Make sure keys exist
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseAnonKey) {
  console.error("❌ Missing SUPABASE_URL or SUPABASE_ANON_KEY in environment");
  throw new Error("Supabase auth client cannot start without anon key + url");
}

// ✅ Use anon client for verifying JWTs
const supabaseAuth = createClient(supabaseUrl, supabaseAnonKey, {
  auth: { persistSession: false, autoRefreshToken: false },
});

exports.authenticate = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader) return res.status(401).json({ error: "No token provided" });

    const token = authHeader.split(" ")[1];
    if (!token) return res.status(401).json({ error: "Malformed Authorization header" });

    // Verify with Supabase anon client
    const { data, error } = await supabaseAuth.auth.getUser(token);

    if (error || !data?.user) {
      console.error("❌ Auth failed:", error?.message || "No user in token");
      return res.status(401).json({ error: "Invalid or expired token" });
    }

    req.user = data.user;
    next();
  } catch (err) {
    console.error("❌ Auth middleware error:", err);
    return res.status(500).json({ error: "Auth check failed" });
  }
};
