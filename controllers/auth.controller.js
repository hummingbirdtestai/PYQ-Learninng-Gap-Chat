// controllers/auth.controller.js
const twilioClient = require('../utils/twilioClient');
const { supabase } = require('../config/supabaseClient');
const { toE164, last10 } = require('../utils/phone');
const { v4: uuidv4 } = require('uuid');

exports.verifyOTP = async (req, res) => {
  const { phone, otp } = req.body;
  if (!phone || !otp) {
    return res.status(400).json({ error: 'Phone and OTP are required' });
  }

  try {
    const phoneE164 = toE164(phone, '+91');
    console.log(
      '🔐 verifyOTP → service:',
      process.env.TWILIO_VERIFY_SERVICE_SID,
      'to:',
      phoneE164,
      'code:',
      otp
    );

    // ✅ Step 1: Verify OTP with Twilio
    const verificationCheck = await twilioClient.verify.v2
      .services(process.env.TWILIO_VERIFY_SERVICE_SID)
      .verificationChecks.create({ to: phoneE164, code: otp });

    console.log(
      '📲 Twilio verification status:',
      verificationCheck.status,
      'errorCode:',
      verificationCheck.errorCode || null
    );

    if (verificationCheck.status !== 'approved') {
      return res.status(401).json({
        error: 'Invalid OTP',
        twilio_status: verificationCheck.status,
        twilio_error_code: verificationCheck.errorCode ?? null
      });
    }

    // ✅ Step 2: Lookup user in Supabase
    const country = '+91';
    const ten = last10(phoneE164);

    const { data: user, error } = await supabase
      .from('users')
      .select('*')
      .eq('country_code', country)
      .eq('phone', ten)
      .limit(1)
      .maybeSingle();

    if (error) {
      console.error('❌ Supabase error:', error?.message || error);
      return res
        .status(500)
        .json({ error: error?.message || 'Database error' });
    }

    // ✅ Step 3: Return result
    if (user) {
      // Existing user
      return res.status(200).json({
        message: 'OTP verified',
        isNewUser: false,
        userId: user.id,
        user
      });
    } else {
      // New user → generate a temp UUID for frontend registration
      const tempId = uuidv4();
      return res.status(200).json({
        message: 'OTP verified',
        isNewUser: true,
        userId: tempId, // frontend will use this for registration
        user: null
      });
    }
  } catch (error) {
    console.error('❌ OTP verification error:', error?.message || error);
    const msg =
      error?.message === 'INVALID_PHONE_FORMAT'
        ? 'Invalid phone number'
        : error?.message || 'Verification failed';
    return res.status(500).json({ error: msg });
  }
};
