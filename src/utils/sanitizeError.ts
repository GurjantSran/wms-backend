// File Path = warehouse-backend/src/utils/sanitizeError.ts
// Utility to sanitize error messages before sending to clients.
// Prevents internal details (SQL queries, stack traces, file paths) from leaking.

const SENSITIVE_PATTERNS = [
    /SELECT\s+/i,
    /INSERT\s+/i,
    /UPDATE\s+/i,
    /DELETE\s+/i,
    /FROM\s+/i,
    /WHERE\s+/i,
    /JOIN\s+/i,
    /relation\s+"?\w+"?\s+does\s+not\s+exist/i,
    /column\s+"?\w+"?\s+does\s+not\s+exist/i,
    /duplicate\s+key\s+value/i,
    /syntax\s+error\s+at\s+or\s+near/i,
    /password\s+authentication\s+failed/i,
    /ECONNREFUSED/i,
    /ETIMEDOUT/i,
    /at\s+\w+\s+\(.*:\d+:\d+\)/i, // stack trace patterns
];

/**
 * Returns a safe error message for client responses.
 * In production, internal/database error details are replaced with a generic message.
 * In development, the original message is preserved for debugging.
 */
export const sanitizeErrorMessage = (error: any, fallback = 'An internal error occurred'): string => {
    const message = error?.message || fallback;

    // Always sanitize in production
    if (process.env.NODE_ENV === 'production') {
        for (const pattern of SENSITIVE_PATTERNS) {
            if (pattern.test(message)) {
                return fallback;
            }
        }
    }

    return message;
};

/**
 * Shorthand for use in catch blocks:
 * catch (error: any) {
 *   res.status(500).json({ error: safeError(error) });
 * }
 */
export const safeError = (error: any, fallback = 'An internal error occurred'): string => {
    return sanitizeErrorMessage(error, fallback);
};
