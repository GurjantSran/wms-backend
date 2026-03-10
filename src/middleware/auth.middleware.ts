// File Path = warehouse-backend/src/middleware/auth.middleware.ts
import { Request, Response, NextFunction } from 'express';
import { verifyToken, JWTPayload } from '../config/auth';
import { queryNoRetry } from '../config/database';
import crypto from 'crypto';

declare global {
  namespace Express {
    interface Request {
      user?: JWTPayload;
    }
  }
}

// Helper to hash token
const hashToken = (token: string): string => {
  return crypto.createHash('sha256').update(token).digest('hex');
};

// ─── Session Validation Cache ───────────────────────────────────────────────
// Every API request was hitting DB for session validation — the #1 cause of pool
// exhaustion under load. Cache valid sessions for 30s to reduce DB hits by ~90%.
const sessionCache = new Map<string, { valid: boolean; expiry: number }>();
const SESSION_CACHE_TTL = 30000; // 30 seconds

// Cleanup stale entries periodically
const sessionCacheCleanup = setInterval(() => {
  const now = Date.now();
  for (const [key, val] of sessionCache) {
    if (val.expiry < now) sessionCache.delete(key);
  }
}, 300000); // every 5 minutes
if (sessionCacheCleanup.unref) sessionCacheCleanup.unref();

export const authMiddleware = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ error: 'Unauthorized: No token provided' });
    }

    const token = authHeader.slice(7);
    const user = verifyToken(token);

    // Check if session is still active (token not invalidated)
    try {
      const tokenHash = hashToken(token);

      // Check cache first — avoids DB hit for recently-validated sessions
      const cached = sessionCache.get(tokenHash);
      if (cached && cached.expiry > Date.now()) {
        if (!cached.valid) {
          return res.status(401).json({ error: 'Session expired. Please login again.', code: 'SESSION_INVALIDATED' });
        }
        // Cache hit — skip DB query
        req.user = user;
        return next();
      }

      const sessionResult = await queryNoRetry(
        'SELECT id FROM active_sessions WHERE token_hash = $1 AND is_active = true AND expires_at > NOW()',
        [tokenHash]
      );

      if (sessionResult.rows.length === 0) {
        // Session was invalidated (logged out by admin) — cache the negative result too
        sessionCache.set(tokenHash, { valid: false, expiry: Date.now() + SESSION_CACHE_TTL });
        return res.status(401).json({ error: 'Session expired. Please login again.', code: 'SESSION_INVALIDATED' });
      }

      // Cache successful validation
      sessionCache.set(tokenHash, { valid: true, expiry: Date.now() + SESSION_CACHE_TTL });
    } catch (dbError: any) {
      const errMsg = dbError.message || '';
      const isConnectionError = errMsg.includes('timeout') || errMsg.includes('connection') || errMsg.includes('ECONNREFUSED');

      if (isConnectionError) {
        // Fail open for connection/timeout errors — token was already verified via JWT
        // This prevents cascading 503 failures when DB pool is momentarily stressed
        console.warn('[AUTH] Session validation skipped (DB connection issue):', errMsg);
      } else {
        // Fail closed for actual query errors (e.g., table doesn't exist)
        console.error('[AUTH] Session validation DB error:', errMsg);
        return res.status(503).json({ error: 'Service temporarily unavailable. Please try again.', code: 'DB_ERROR' });
      }
    }

    req.user = user;
    next();
  } catch (error: any) {
    res.status(401).json({ error: 'Unauthorized: Invalid token' });
  }
};

// Invalidate a specific session from cache (called on logout/admin-kill)
export const invalidateSessionCache = (tokenHash: string) => {
  sessionCache.delete(tokenHash);
};

// Clear entire session cache (called on mass logout)
export const clearSessionCache = () => {
  sessionCache.clear();
};

export const adminOnly = (req: Request, res: Response, next: NextFunction) => {
  if (req.user?.role !== 'admin') {
    return res.status(403).json({ error: 'Forbidden: Admin access required' });
  }
  next();
};

// Role-based middleware
export const hasRole = (...allowedRoles: string[]) => {
  return (req: Request, res: Response, next: NextFunction) => {
    if (!req.user) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    if (!allowedRoles.includes(req.user.role)) {
      return res.status(403).json({ error: `Forbidden: Requires ${allowedRoles.join(' or ')} role` });
    }

    next();
  };
};
