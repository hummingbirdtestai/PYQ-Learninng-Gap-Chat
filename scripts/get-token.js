// scripts/get-token.js
import { createClient } from '@supabase/supabase-js';

// Load env vars (make sure you have them in Railway or in local .env)
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

(async () => {
  // Step 1: request OTP
  const { error: sendError } = await supabase.auth.signInWithOtp({
    phone: '+919704927613' // 👉 change to your phone number
  });

  if (sendError) {
    console.error('❌ Error sending OTP:', sendError);
    return;
  }

  console.log('📲 OTP sent to your phone! Now verify...');

  // Step 2: after OTP arrives, paste it below
  const otp = process.argv[2]; // pass OTP as argument

  if (!otp) {
    console.log('👉 Run again with: node scripts/get-token.js <OTP>');
    return;
  }

  const { data, error } = await supabase.auth.verifyOtp({
    phone: '+919704927613',
    token: otp,
    type: 'sms'
  });

  if (error) {
    console.error('❌ Error verifying OTP:', error);
  } else {
    console.log('✅ Your access token:', data.session.access_token);
  }
})();
