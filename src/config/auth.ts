// File Path = warehouse-backend/src/config/auth.ts
import jwt from 'jsonwebtoken';

// Ensure a JWT secret is available in non-test environments
if (!process.env.JWT_SECRET && process.env.NODE_ENV !== 'test') {
  throw new Error("JWT_SECRET is missing");
}

// Use a test secret when running tests; otherwise the presence check above guarantees a value
const JWT_SECRET: string = process.env.JWT_SECRET ?? (process.env.NODE_ENV === 'test' ? 'test_jwt_secret' : '');
const JWT_EXPIRY = process.env.JWT_EXPIRY || '24h'; // 24 hours — reduced from 7d for security

export interface JWTPayload {
  userId: number;
  username: string;
  full_name: string;
  role: string;
  warehouseId?: number;
}

export const generateToken = (payload: JWTPayload): string => {
  // JWT_SECRET is guaranteed to be a string in non-test envs; cast to jwt.Secret for type safety
  return jwt.sign(payload, JWT_SECRET as jwt.Secret, {
    expiresIn: JWT_EXPIRY as string,
  } as jwt.SignOptions);
};

export const verifyToken = (token: string): JWTPayload => {
  try {
    const decoded = jwt.verify(token, JWT_SECRET as jwt.Secret) as jwt.JwtPayload | JWTPayload;
    // Basic runtime validation
    if (!decoded || typeof decoded !== 'object' || !('userId' in decoded)) {
      throw new Error('Invalid token payload');
    }
    return decoded as JWTPayload;
  } catch (error) {
    throw new Error('Invalid token');
  }
};
