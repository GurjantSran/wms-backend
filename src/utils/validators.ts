// File Path = warehouse-backend/src/utils/validators.ts
export const validateEmail = (email: string): boolean => {
  const regex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return regex.test(email);
};

export const validateUsername = (username: string): boolean => {
  return username.length >= 3 && username.length <= 50;
};

/**
 * Validate password strength
 * Requirements:
 * - Minimum 8 characters
 * - At least one uppercase letter
 * - At least one lowercase letter
 * - At least one number
 * Returns object with isValid and message
 */
export const validatePassword = (password: string): { isValid: boolean; message?: string } => {
  if (password.length < 8) {
    return { isValid: false, message: 'Password must be at least 8 characters long' };
  }
  if (!/[A-Z]/.test(password)) {
    return { isValid: false, message: 'Password must contain at least one uppercase letter' };
  }
  if (!/[a-z]/.test(password)) {
    return { isValid: false, message: 'Password must contain at least one lowercase letter' };
  }
  if (!/[0-9]/.test(password)) {
    return { isValid: false, message: 'Password must contain at least one number' };
  }
  return { isValid: true };
};

// Legacy simple validation for backward compatibility
export const validatePasswordSimple = (password: string): boolean => {
  return password.length >= 8;
};

export const validateWarehouseCode = (code: string): boolean => {
  return code.length >= 2 && code.length <= 10;
};

export const validateWSN = (wsn: string): boolean => {
  return wsn.length > 0 && wsn.length <= 255;
};

// Sanitize string input to prevent XSS
export const sanitizeInput = (input: string): string => {
  return input
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#x27;')
    .trim();
};

// Validate positive integer
export const validatePositiveInteger = (value: unknown): boolean => {
  const num = Number(value);
  return Number.isInteger(num) && num > 0;
};
