// File Path = warehouse-backend/src/utils/logger.ts
// Production-ready logger utility

const isProduction = process.env.NODE_ENV === 'production';
const isDevelopment = process.env.NODE_ENV === 'development';
const logLevel = process.env.LOG_LEVEL || (isProduction ? 'warn' : 'debug');

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

const levels: Record<LogLevel, number> = {
    debug: 0,
    info: 1,
    warn: 2,
    error: 3,
};

const shouldLog = (level: LogLevel): boolean => {
    return levels[level] >= levels[logLevel as LogLevel];
};

const formatMessage = (level: string, message: string, meta?: object): string => {
    const timestamp = new Date().toISOString();
    const metaStr = meta ? ` ${JSON.stringify(meta)}` : '';
    return `[${timestamp}] [${level.toUpperCase()}] ${message}${metaStr}`;
};

export const logger = {
    debug: (message: string, meta?: object) => {
        if (shouldLog('debug')) {
            console.log(formatMessage('debug', message, meta));
        }
    },

    info: (message: string, meta?: object) => {
        if (shouldLog('info')) {
            console.info(formatMessage('info', message, meta));
        }
    },

    warn: (message: string, meta?: object) => {
        if (shouldLog('warn')) {
            console.warn(formatMessage('warn', message, meta));
        }
    },

    error: (message: string, error?: Error | unknown, meta?: object) => {
        if (shouldLog('error')) {
            const errorMeta = error instanceof Error
                ? { errorMessage: error.message, stack: isProduction ? undefined : error.stack }
                : { error };
            console.error(formatMessage('error', message, { ...errorMeta, ...meta }));
        }
    },

    // For request logging
    request: (method: string, path: string, statusCode: number, duration: number) => {
        if (shouldLog('info')) {
            console.info(formatMessage('info', `${method} ${path} ${statusCode} ${duration}ms`));
        }
    },
};

export default logger;
