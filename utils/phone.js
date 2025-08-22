// utils/phone.js
function toE164(raw, defaultCountry = '+91') {
  const s = String(raw || '').trim();
  if (!s) throw new Error('INVALID_PHONE_FORMAT');

  if (/^\+\d{8,15}$/.test(s)) return s;

  const digits = s.replace(/\D/g, '');

  if (digits.length === 12 && digits.startsWith('91')) return `+${digits}`;
  if (digits.length === 10) return `${defaultCountry}${digits}`;

  throw new Error('INVALID_PHONE_FORMAT');
}

function last10(raw) {
  const digits = String(raw || '').replace(/\D/g, '');
  return digits.slice(-10);
}

module.exports = { toE164, last10 };  // ✅ now destructuring import works
