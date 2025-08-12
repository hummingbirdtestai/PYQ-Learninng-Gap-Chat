// controllers/auth.controller.js
const twilioClient = require('../utils/twilioClient');
const supabase = require('../config/supabaseClient');
const { toE164, last10 } = require('../utils/phone');

/**
 * POST /auth/otp/start
 * Body: { phone: string }  // can be "8008393103" or "+918008393103"
 */
exports.startOTP = async (req, res) => {
  const { phone } = req.body;
  if (!phone) return res.status(400).json({ error: 'Phone number is required' });

  try {
    const phoneE164 = toE164(phone, '+91');

    // Debug so you can confirm the same service + "to" are used in both endpoints
    console.log('📤 startOTP → service:', process.env.TWILIO_VERIFY_SERVICE_SID, 'to:', phoneE164);

    const otpResponse = await twilioClient.verify.v2
      .services(process.env.TWILIO_VERIFY_SERVICE_SID)
      .verifications.create({ to: phoneE164, channel: 'sms' });

    console.log('✅ OTP sent, SID:', otpResponse.sid);

    return res.status(200).json({ message: 'OTP sent', sid: otpResponse.sid });
  } catch (error) {
    console.error('❌ Error sending OTP:', error?.message || error);
    const msg = error?.message === 'INVALID_PHONE_FORMAT' ? 'Invalid phone number' : (error?.message || 'Failed to send OTP');
    return res.status(500).json({ error: msg });
  }
};

/**
 * POST /auth/otp/verify
 * Body: { phone: string, otp: string }
 */
exports.verifyOTP = async (req, res) => {
  const { phone, otp } = req.body;

  if (!phone || !otp) {
    return res.status(400).json({ error: 'Phone and OTP are required' });
  }

  try {
    const phoneE164 = toE164(phone, '+91');

    console.log('🔐 verifyOTP → service:', process.env.TWILIO_VERIFY_SERVICE_SID, 'to:', phoneE164, 'code:', otp);

    const verificationCheck = await twilioClient.verify.v2
      .services(process.env.TWILIO_VERIFY_SERVICE_SID)
      .verificationChecks
      .create({ to: phoneE164, code: otp });

    console.log('📲 Twilio verification status:', verificationCheck.status, 'errorCode:', verificationCheck.errorCode || null);

    if (verificationCheck.status !== 'approved') {
      // Expose Twilio details while testing
      return res.status(401).json({
        error: 'Invalid OTP',
        twilio_status: verificationCheck.status,
        twilio_error_code: verificationCheck.errorCode ?? null
      });
    }

    // ── Align with your DB schema: country_code + 10-digit phone ─────────────
    // If your users table stores separate columns (recommended per your spec):
    const country = '+91';
    const ten = last10(phoneE164);

    const { data: user, error } = await supabase
      .from('users')
      .select('*')
      .eq('country_code', country)
      .eq('phone', ten)
      .limit(1)
      .maybeSingle(); // avoids 500 when no row

    if (error) {
      console.error('❌ Supabase error:', error?.message || error);
      return res.status(500).json({ error: error?.message || 'Database error' });
    }

    return res.status(200).json({
      message: 'OTP verified',
      isNewUser: !user,
      user: user || null
    });
  } catch (error) {
    console.error('❌ OTP verification error:', error?.message || error);
    const msg = error?.message === 'INVALID_PHONE_FORMAT' ? 'Invalid phone number' : (error?.message || 'Verification failed');
    return res.status(500).json({ error: msg });
  }
};
