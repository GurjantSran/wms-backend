// File Path = warehouse-backend/src/routes/cache.routes.ts
// Cache routes for frontend IndexedDB sync - Fast WSN lookups
import { Router } from 'express';
import { authMiddleware } from '../middleware/auth.middleware';
import { injectWarehouseFilter } from '../middleware/rbac.middleware';
import * as cacheController from '../controllers/cache.controller';

const router = Router();

// All routes require authentication + warehouse isolation
router.use(authMiddleware);
router.use(injectWarehouseFilter);

// GET /api/cache/pending - Pending inventory for Inbound page
// Returns master_data items NOT yet received and NOT rejected
router.get('/pending', cacheController.getPendingInventory);

// GET /api/cache/available - Available inventory for QC/Picking/Outbound
// Returns items currently in warehouse (not yet dispatched)
router.get('/available', cacheController.getAvailableInventory);

// GET /api/cache/stats - Cache statistics for monitoring
router.get('/stats', cacheController.getCacheStats);

export default router;
