// File Path = warehouse-backend/src/middleware/rateLimit.middleware.ts
import { Request, Response, NextFunction } from 'express';

/**
 * Simple in-memory rate limiter
 * Each limiter instance gets its own independent store to prevent cross-counting.
 * For production at scale, use Redis-based solution.
 */

interface RateLimitEntry {
    count: number;
    resetTime: number;
}

/**
 * Create a rate limit middleware with its own isolated store.
 * @param maxRequests - Maximum requests allowed per window
 * @param windowMs - Time window in milliseconds
 * @param name - Unique limiter name (used in logs and response headers)
 */
export const rateLimit = (maxRequests: number, windowMs: number, name: string = 'general') => {
    // Each limiter gets its own store — prevents cross-counting between limiters
    const store = new Map<string, RateLimitEntry>();

    // Periodic cleanup to prevent memory leaks (runs every 5 minutes)
    const CLEANUP_INTERVAL = 5 * 60 * 1000;
    let lastCleanup = Date.now();

    return (req: Request, res: Response, next: NextFunction) => {
        // Skip rate limiting in test environment
        if (process.env.NODE_ENV === 'test') return next();

        // Use forwarded IP if behind proxy (Render, Vercel), fallback to socket IP
        const ip = (req.headers['x-forwarded-for'] as string)?.split(',')[0]?.trim()
            || req.ip
            || req.socket.remoteAddress
            || 'unknown';
        const now = Date.now();

        // Periodic cleanup of expired entries
        if (now - lastCleanup > CLEANUP_INTERVAL) {
            lastCleanup = now;
            for (const [k, v] of store) {
                if (v.resetTime < now) store.delete(k);
            }
        }

        const entry = store.get(ip);

        // No entry or window expired → start fresh
        if (!entry || entry.resetTime < now) {
            store.set(ip, { count: 1, resetTime: now + windowMs });
            return next();
        }

        // Increment count
        entry.count++;

        // Check if limit exceeded
        if (entry.count > maxRequests) {
            const remainingTime = Math.ceil((entry.resetTime - now) / 1000);
            return res.status(429).json({
                error: 'Too many requests',
                message: `Rate limit exceeded (${name}). Please try again after ${remainingTime} seconds.`,
                retryAfter: remainingTime
            });
        }

        next();
    };
};

// ─── Limiter Instances ──────────────────────────────────────────────────────
// Each instance has its own isolated counter — they DO NOT stack.

/**
 * Login brute-force protection: 10 attempts per 15 minutes per IP.
 * Applied ONLY to POST /api/auth/login.
 * This is the tightest limiter — protects against credential stuffing.
 */
export const loginRateLimit = rateLimit(10, 15 * 60 * 1000, 'login');

/**
 * Sensitive write operations: 30 requests per 15 minutes per IP.
 * Applied to CUD (Create/Update/Delete) routes on permissions and backups.
 * NOT applied to read/GET endpoints — those use the general API limiter only.
 */
export const sensitiveWriteRateLimit = rateLimit(30, 15 * 60 * 1000, 'sensitive-write');

/**
 * General API rate limit: 300 requests per minute per IP.
 * Applied globally to all routes.
 * Skips /api/sessions/heartbeat and /api/health to avoid penalizing keep-alive polling.
 * Set to 300/min (up from 200) because authenticated users doing real work
 * (e.g. loading permissions page) can burst 8-10 requests simultaneously.
 */
export const apiRateLimit = rateLimit(300, 60 * 1000, 'api');

/**
 * Wrapper that skips the global rate limiter for high-frequency polling endpoints.
 * Heartbeat fires every 30s per tab — this should never eat into the global budget.
 */
export const globalRateLimitWithExclusions = (req: Request, res: Response, next: NextFunction) => {
    // Skip rate limiting for keep-alive and monitoring endpoints
    if (req.path === '/api/sessions/heartbeat' ||
        req.path === '/api/health' ||
        req.path === '/api/wake') {
        return next();
    }
    return apiRateLimit(req, res, next);
};
