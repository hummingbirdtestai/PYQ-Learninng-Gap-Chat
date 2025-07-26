// Auth Controller
const twilioClient = require('../utils/twilioClient');
const supabase = require('../config/supabaseClient');

exports.startOTP = async (req, res) => {
  const { phone } = req.body;
  if (!phone) return res.status(400).json({ error: 'Phone number is required' });

  try {
    console.log('📤 Sending OTP to:', phone);

    const otpResponse = await twilioClient.verify.v2.services(process.env.TWILIO_VERIFY_SERVICE_SID)
      .verifications
      .create({ to: phone, channel: 'sms' });

    console.log('✅ OTP sent, SID:', otpResponse.sid);

    res.status(200).json({ message: 'OTP sent', sid: otpResponse.sid });
  } catch (error) {
    console.error('❌ Error sending OTP:', error.message);
    res.status(500).json({ error: error.message });
  }
};

exports.verifyOTP = async (req, res) => {
  const { phone, otp } = req.body;

  if (!phone || !otp) {
    return res.status(400).json({ error: 'Phone and OTP are required' });
  }

  try {
    console.log('🔐 Verifying OTP for:', phone, '| OTP:', otp);

    const verificationCheck = await twilioClient.verify.v2.services(process.env.TWILIO_VERIFY_SERVICE_SID)
      .verificationChecks
      .create({ to: phone, code: otp });

    console.log('📲 Twilio verification status:', verificationCheck.status);

    if (verificationCheck.status === 'approved') {
      const { data: user, error } = await supabase
        .from('users')
        .select('*')
        .eq('phone', phone)
        .limit(1)
        .single();

      if (error && error.code !== 'PGRST116') {
        console.error('❌ Supabase error:', error.message);
        return res.status(500).json({ error: error.message });
      }

      const isNewUser = !user;

      res.status(200).json({
        message: 'OTP verified',
        isNewUser,
        user: user || null
      });
    } else {
      console.warn('⚠️ Invalid OTP attempt for:', phone);
      res.status(401).json({ error: 'Invalid OTP' });
    }
  } catch (error) {
    console.error('❌ OTP verification error:', error.message);
    res.status(500).json({ error: error.message });
  }
};
