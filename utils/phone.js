// utils/phone.js
// Normalize any Indian phone to E.164 and get last 10 digits

// Returns "+919876543210" if input is "9876543210", "+919876543210", "98 76-54 3210", etc.
// Throws INVALID_PHONE_FORMAT if not normalizable.
exports.toE164 = (raw, defaultCountry = '+91') => {
  const s = String(raw || '').trim();
  if (!s) throw new Error('INVALID_PHONE_FORMAT');

  // Already E.164?
  if (/^\+\d{8,15}$/.test(s)) return s;

  const digits = s.replace(/\D/g, '');

  // "91xxxxxxxxxx" (12 digits) → "+91xxxxxxxxxx"
  if (digits.length === 12 && digits.startsWith('91')) return `+${digits}`;

  // 10-digit Indian local → "+91xxxxxxxxxx"
  if (digits.length === 10) return `${defaultCountry}${digits}`;

  throw new Error('INVALID_PHONE_FORMAT');
};

// Always returns last 10 digits (useful when DB stores country_code + phone separately)
exports.last10 = (raw) => {
  const digits = String(raw || '').replace(/\D/g, '');
  return digits.slice(-10);
};
