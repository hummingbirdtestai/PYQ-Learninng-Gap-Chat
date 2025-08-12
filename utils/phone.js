// Logger utility
// utils/phone.js
// Converts raw input to E.164 for India by default.
// Accepts: "9876543210", "+919876543210", "98 76-54 3210"
// Returns: "+919876543210" or throws INVALID_PHONE_FORMAT
exports.toE164 = (raw, defaultCountry = '+91') => {
  const s = String(raw || '').trim();
  if (!s) throw new Error('INVALID_PHONE_FORMAT');

  // Already E.164
  if (/^\+\d{8,15}$/.test(s)) return s;

  // Strip non-digits
  const digits = s.replace(/\D/g, '');

  // If starts with 91 and is 12 digits, assume India E.164 without the '+'
  if (digits.length === 12 && digits.startsWith('91')) return `+${digits}`;

  // If 10 digits, bolt on default country
  if (digits.length === 10) return `${defaultCountry}${digits}`;

  throw new Error('INVALID_PHONE_FORMAT');
};

// Returns the last 10 digits (used when your DB stores phone separately from country_code)
exports.last10 = (raw) => {
  const digits = String(raw || '').replace(/\D/g, '');
  return digits.slice(-10);
};
