// File Path = warehouse-backend/src/middleware/validation.middleware.ts
import { Request, Response, NextFunction } from 'express';

/**
 * Validate request body has required fields
 */
export const validateRequired = (fields: string[]) => {
    return (req: Request, res: Response, next: NextFunction) => {
        const missing: string[] = [];

        for (const field of fields) {
            if (!req.body[field]) {
                missing.push(field);
            }
        }

        if (missing.length > 0) {
            return res.status(400).json({
                error: 'Missing required fields',
                missing: missing
            });
        }

        next();
    };
};

/**
 * Sanitize string inputs - remove dangerous characters
 */
export const sanitizeInput = (req: Request, res: Response, next: NextFunction) => {
    if (req.body) {
        Object.keys(req.body).forEach(key => {
            if (typeof req.body[key] === 'string') {
                // Remove potential XSS characters
                req.body[key] = req.body[key]
                    .replace(/<script[^>]*>.*?<\/script>/gi, '')
                    .replace(/<[^>]+>/g, '')
                    .trim();
            }
        });
    }
    next();
};

/**
 * Validate WSN format (basic check)
 */
export const validateWSN = (req: Request, res: Response, next: NextFunction) => {
    const wsn = req.body.wsn || req.params.wsn || req.query.wsn;

    if (wsn && typeof wsn === 'string') {
        if (wsn.length < 3 || wsn.length > 255) {
            return res.status(400).json({
                error: 'Invalid WSN format',
                details: 'WSN must be between 3 and 255 characters'
            });
        }
    }

    next();
};

/**
 * Validate date format (YYYY-MM-DD)
 */
export const validateDate = (fieldName: string) => {
    return (req: Request, res: Response, next: NextFunction) => {
        const dateValue = req.body[fieldName];

        if (dateValue) {
            const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
            if (!dateRegex.test(dateValue)) {
                return res.status(400).json({
                    error: `Invalid ${fieldName} format`,
                    details: 'Expected format: YYYY-MM-DD'
                });
            }
        }

        next();
    };
};

/**
 * Validate warehouse ID exists
 */
export const validateWarehouseId = (req: Request, res: Response, next: NextFunction) => {
    const warehouseId = req.body.warehouse_id || req.params.warehouseId;

    if (warehouseId !== undefined && warehouseId !== null) {
        const id = Number(warehouseId);
        if (isNaN(id) || id <= 0) {
            return res.status(400).json({
                error: 'Invalid warehouse_id',
                details: 'Must be a positive number'
            });
        }
    }

    next();
};
