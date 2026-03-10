// File Path = warehouse-backend/src/routes/inventory.routes.ts
import express from 'express';
import {
    getInventorySummary,
    getAvailableStock,
    getStockByStatus,
    getMovementHistory
}
    from '../controllers/inventory.controller';
import { authMiddleware, } from '../middleware/auth.middleware';
import { injectWarehouseFilter, requirePermission } from '../middleware/rbac.middleware';

const router = express.Router();

// All inventory routes require authentication + warehouse isolation
router.use(authMiddleware);
router.use(injectWarehouseFilter);

// Get inventory summary for warehouse
router.get('/summary', requirePermission('feature:inventory:view'), getInventorySummary);

// Get available stock with pagination
router.get('/available-stock', requirePermission('feature:inventory:view'), getAvailableStock);

// Get stock filtered by status
router.get('/by-status', requirePermission('feature:inventory:view'), getStockByStatus);

// Get movement history for a WSN
router.get('/movement-history', requirePermission('feature:inventory:view'), getMovementHistory);

export default router;
