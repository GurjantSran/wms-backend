// File Path = warehouse-backend/src/routes/dashboard.routes.ts
import { Router } from 'express';
import * as dashboardController from '../controllers/dashboard.controller';
import { authMiddleware } from '../middleware/auth.middleware';
import { injectWarehouseFilter, requirePermission } from '../middleware/rbac.middleware';

const router = Router();

// All dashboard routes require authentication
router.use(authMiddleware);

// Get inventory pipeline (Inbound + QC + Picking + Outbound joined)
router.get('/inventory-pipeline', injectWarehouseFilter, requirePermission('feature:dashboard:view'), dashboardController.getInventoryPipeline);

// Get inventory metrics
router.get('/inventory-metrics', injectWarehouseFilter, requirePermission('feature:dashboard:view'), dashboardController.getInventoryMetrics);

// Get activity logs
router.get('/activity-logs', injectWarehouseFilter, requirePermission('feature:dashboard:view'), dashboardController.getActivityLogs);

// ✅ NEW: Export data with filters
router.get('/export-data', injectWarehouseFilter, requirePermission('feature:dashboard:view'), dashboardController.getInventoryDataForExport);

// ✅ PIVOT TABLE APIs - Server-side aggregation for large data
router.get('/pivot-summary', injectWarehouseFilter, requirePermission('feature:dashboard:view'), dashboardController.getPivotSummary);
router.get('/pivot-filters', injectWarehouseFilter, requirePermission('feature:dashboard:view'), dashboardController.getPivotFilters);
router.get('/pivot-drilldown', injectWarehouseFilter, requirePermission('feature:dashboard:view'), dashboardController.getPivotDrilldown);
router.get('/pivot-export-all', injectWarehouseFilter, requirePermission('feature:dashboard:view'), dashboardController.getPivotExportAll);

export default router;