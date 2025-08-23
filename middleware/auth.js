// middleware/auth.js
const { createClient } = require('@supabase/supabase-js');

// ✅ Use anon key for verifying JWTs
const supabaseAuth = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY,
  {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  }
);

exports.authenticate = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader) {
      return res.status(401).json({ error: "No token provided" });
    }

    const token = authHeader.split(" ")[1];
    if (!token) {
      return res.status(401).json({ error: "Malformed Authorization header" });
    }

    // 🔑 Verify token with Supabase (anon client)
    const { data, error } = await supabaseAuth.auth.getUser(token);

    if (error || !data?.user) {
      console.error("❌ Auth failed:", error?.message || "No user in token");
      return res.status(401).json({ error: "Invalid or expired token" });
    }

    // Attach user info to request
    req.user = data.user;
    next();
  } catch (err) {
    console.error("❌ Auth middleware error:", err);
    return res.status(500).json({ error: "Auth check failed" });
  }
};
