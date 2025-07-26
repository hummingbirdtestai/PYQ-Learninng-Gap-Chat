// Auth Controller
const twilioClient = require('../utils/twilioClient');
const supabase = require('../config/supabaseClient');

exports.startOTP = async (req, res) => {
  const { phone } = req.body;
  if (!phone) return res.status(400).json({ error: 'Phone number is required' });

  try {
    const otpResponse = await twilioClient.verify.v2.services(process.env.TWILIO_VERIFY_SERVICE_SID)
      .verifications
      .create({ to: phone, channel: 'sms' });

    res.status(200).json({ message: 'OTP sent', sid: otpResponse.sid });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

exports.verifyOTP = async (req, res) => {
  const { phone, code } = req.body;

  try {
    const verificationCheck = await twilioClient.verify.v2.services(process.env.TWILIO_VERIFY_SERVICE_SID)
      .verificationChecks
      .create({ to: phone, code });

    if (verificationCheck.status === 'approved') {
      // Check user existence
      const { data: user, error } = await supabase
        .from('users')
        .select('*')
        .eq('phone', phone)
        .limit(1)
        .single();

      if (error && error.code !== 'PGRST116') return res.status(500).json({ error: error.message });

      const isNewUser = !user;
      res.status(200).json({ message: 'OTP verified', isNewUser, user });
    } else {
      res.status(401).json({ error: 'Invalid OTP' });
    }
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
