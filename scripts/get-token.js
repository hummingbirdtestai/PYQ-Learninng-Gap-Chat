// scripts/get-token.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

// ✅ Use anon key (safe for client login flows)
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

(async () => {
  const phone = '+919704927613'; // 👉 change to your phone number
  const otp = process.argv[2];   // Pass OTP if available

  if (!otp) {
    // Step 1: send OTP
    const { error: sendError } = await supabase.auth.signInWithOtp({ phone });
    if (sendError) {
      console.error('❌ Error sending OTP:', sendError);
      return;
    }
    console.log(`📲 OTP sent to ${phone}! Run again with: node scripts/get-token.js <OTP>`);
    return;
  }

  // Step 2: verify OTP
  const { data, error } = await supabase.auth.verifyOtp({
    phone,
    token: otp,
    type: 'sms'
  });

  if (error) {
    console.error('❌ Error verifying OTP:', error);
  } else {
    console.log('✅ Your access token:', data.session.access_token);
  }
})();
