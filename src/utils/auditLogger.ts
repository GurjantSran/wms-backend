// File Path = warehouse-backend/src/utils/auditLogger.ts
// Audit logging for security-sensitive operations.
// Logs to database (error_logs table with 'AUDIT' prefix) and console.
import { query } from '../config/database';
import logger from './logger';

export type AuditAction =
    | 'PERMISSION_CHANGE'
    | 'ROLE_CHANGE'
    | 'USER_OVERRIDE_CHANGE'
    | 'WAREHOUSE_ACCESS_CHANGE'
    | 'USER_PASSWORD_CHANGE'
    | 'BACKUP_DOWNLOAD'
    | 'BACKUP_CREATE'
    | 'BACKUP_RESTORE'
    | 'BACKUP_DELETE'
    | 'USER_CREATE'
    | 'USER_DELETE'
    | 'SESSION_INVALIDATE'
    | 'LOGIN_SUCCESS'
    | 'LOGIN_FAILED';

interface AuditEntry {
    action: AuditAction;
    performedBy: string;        // username of who performed the action
    targetUser?: string;         // user affected (if applicable)
    details: string;             // human-readable description
    ipAddress?: string;          // client IP
    metadata?: Record<string, unknown>; // additional structured data
}

/**
 * Log a security-relevant audit event.
 * Written to error_logs table with [AUDIT] prefix for easy filtering.
 * Non-blocking — does not throw on failure.
 */
export const logAudit = async (entry: AuditEntry): Promise<void> => {
    const message = `[AUDIT] ${entry.action}: ${entry.details}`;
    const metadataStr = entry.metadata ? JSON.stringify(entry.metadata) : '';

    // Log to console immediately
    logger.info(message, {
        action: entry.action,
        performedBy: entry.performedBy,
        targetUser: entry.targetUser,
        ip: entry.ipAddress,
    });

    // Persist to database (non-blocking)
    try {
        await query(
            `INSERT INTO error_logs (message, endpoint, method, username, stack_trace)
       VALUES ($1, $2, $3, $4, $5)`,
            [
                message,
                entry.action,                                    // store action in endpoint column
                'AUDIT',                                         // method column = 'AUDIT' for filtering
                entry.performedBy,
                `target=${entry.targetUser || 'N/A'} ip=${entry.ipAddress || 'N/A'} ${metadataStr}`.trim()
            ]
        );
    } catch (err) {
        // Never crash on audit log failure
        console.error('[AUDIT] Failed to persist audit log:', (err as Error).message);
    }
};

/**
 * Helper to extract IP from request
 */
export const getClientIp = (req: any): string => {
    return (req.headers?.['x-forwarded-for'] as string)?.split(',')[0]?.trim()
        || req.ip
        || req.socket?.remoteAddress
        || 'unknown';
};
