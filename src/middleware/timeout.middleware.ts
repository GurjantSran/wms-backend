// File Path = warehouse-backend/src/middleware/timeout.middleware.ts
import { Request, Response, NextFunction } from 'express';

/**
 * Request timeout middleware
 * Prevents requests from hanging indefinitely
 * Increased timeouts for remote Supabase database
 * 
 * @param timeoutMs - Timeout in milliseconds (default: 60 seconds)
 */
export const requestTimeout = (timeoutMs: number = 60000) => {
    return (req: Request, res: Response, next: NextFunction) => {
        // Set timeout
        const timeout = setTimeout(() => {
            if (!res.headersSent) {
                res.status(408).json({
                    error: 'Request Timeout',
                    message: 'Request took too long to process. Please try again or use filters to reduce data.',
                    timeout: `${timeoutMs / 1000}s`,
                    isRetryable: true
                });
            }
        }, timeoutMs);

        // Clear timeout when response finishes
        res.on('finish', () => {
            clearTimeout(timeout);
        });

        // Also clear on close (client disconnect)
        res.on('close', () => {
            clearTimeout(timeout);
        });

        next();
    };
};

/**
 * Longer timeout for file uploads and bulk operations (10 minutes)
 */
export const uploadTimeout = requestTimeout(10 * 60 * 1000);

/**
 * Extra long timeout for massive bulk uploads (30 minutes) - for 500K-5M rows
 */
export const bulkUploadTimeout = requestTimeout(30 * 60 * 1000);

/**
 * Backup/Restore operations timeout (20 minutes)
 */
export const backupTimeout = requestTimeout(20 * 60 * 1000);

/**
 * Standard timeout for API requests (60 seconds)
 * Increased from 30s due to remote Supabase database latency
 */
export const apiTimeout = requestTimeout(60 * 1000);

/**
 * Extended timeout for data-heavy list requests (2 minutes)
 */
export const listTimeout = requestTimeout(2 * 60 * 1000);
