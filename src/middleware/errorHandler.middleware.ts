// File Path = warehouse-backend/src/middleware/errorHandler.middleware.ts
import { Request, Response, NextFunction } from 'express';
import logger from '../utils/logger';
import { query } from '../config/database';

// Parse stack trace to get file and line info
const parseStackTrace = (stack: string | undefined): string => {
  if (!stack) return '';

  // Get relevant lines (skip first line which is the error message)
  const lines = stack.split('\n').slice(1, 6); // Get first 5 stack frames

  // Clean up and format
  return lines
    .map(line => line.trim())
    .filter(line => line.startsWith('at '))
    .join('\n');
};

// Enhanced error logging to database
const logErrorToDb = async (
  message: string,
  endpoint: string,
  method: string,
  username: string,
  stackTrace: string
) => {
  try {
    await query(
      `INSERT INTO error_logs (message, endpoint, method, username, stack_trace) 
       VALUES ($1, $2, $3, $4, $5)`,
      [
        message.substring(0, 500),
        endpoint,
        method,
        username,
        stackTrace.substring(0, 2000) // Limit stack trace size
      ]
    );
  } catch {
    // Silent fail - don't break app if logging fails
  }
};

// Identify error types for user-friendly messages
const getErrorDetails = (error: any): { message: string; statusCode: number; isRetryable: boolean } => {
  const errorMessage = error.message?.toLowerCase() || '';
  const errorCode = error.code || '';

  // Database connection timeout
  if (errorMessage.includes('timeout exceeded when trying to connect') ||
    errorMessage.includes('connection timeout') ||
    errorCode === 'ETIMEDOUT') {
    return {
      message: 'Database connection is slow. Please try again in a moment.',
      statusCode: 503,
      isRetryable: true
    };
  }

  // Query timeout
  if (errorMessage.includes('query read timeout') ||
    errorMessage.includes('statement timeout') ||
    errorMessage.includes('canceling statement due to statement timeout')) {
    return {
      message: 'Request is taking too long. Please try again or use filters to reduce data.',
      statusCode: 504,
      isRetryable: true
    };
  }

  // Network errors
  if (errorCode === 'ECONNREFUSED' ||
    errorCode === 'ECONNRESET' ||
    errorCode === 'ENOTFOUND' ||
    errorMessage.includes('network') ||
    errorMessage.includes('socket hang up')) {
    return {
      message: 'Network connection issue. Please check your internet and try again.',
      statusCode: 503,
      isRetryable: true
    };
  }

  // Pool exhaustion
  if (errorMessage.includes('cannot acquire a connection') ||
    errorMessage.includes('too many clients') ||
    errorMessage.includes('connection pool') ||
    errorMessage.includes('pool is draining')) {
    return {
      message: 'Server is busy. Please wait a moment and try again.',
      statusCode: 503,
      isRetryable: true
    };
  }

  // Default error - sanitize internal details in production
  const isProduction = process.env.NODE_ENV === 'production';
  const safeMessage = isProduction && (error.statusCode || 500) >= 500
    ? 'An internal error occurred. Please try again.'
    : error.message || 'An unexpected error occurred. Please try again.';
  
  return {
    message: safeMessage,
    statusCode: error.statusCode || 500,
    isRetryable: false
  };
};

export const errorHandler = (
  error: any,
  req: Request,
  res: Response,
  next: NextFunction
) => {
  // CRITICAL: Check if headers already sent to prevent double-response crash
  if (res.headersSent) {
    console.error('⚠️ Headers already sent, cannot send error response:', error.message);
    return next(error);
  }

  // Log to console with full details
  logger.error('API Error', error, {
    endpoint: req.path,
    method: req.method,
    user: req.user?.username || 'anonymous',
    stack: error.stack
  });

  // Parse stack trace for file/line info
  const stackTrace = parseStackTrace(error.stack);

  // Log to database (async - don't wait) - skip if it's a DB error
  const errorDetails = getErrorDetails(error);
  if (!errorDetails.isRetryable) {
    logErrorToDb(
      error.message || 'Unknown error',
      req.path,
      req.method,
      req.user?.username || 'anonymous',
      stackTrace
    );
  }

  res.status(errorDetails.statusCode).json({
    error: errorDetails.message,
    isRetryable: errorDetails.isRetryable,
    timestamp: new Date().toISOString(),
    path: req.path,
  });
};
