// File Path = warehouse-backend/src/routes/reports.routes.ts
import express, { Router } from 'express';
import { authMiddleware, hasRole } from '../middleware/auth.middleware';
import { injectWarehouseFilter, requirePermission } from '../middleware/rbac.middleware';
import * as reportsController from '../controllers/reports.controller';

const router: Router = express.Router();

// All routes require authentication + warehouse isolation
router.use(authMiddleware);
router.use(injectWarehouseFilter);

// Inventory Reports
router.get('/current-stock',
    requirePermission('feature:reports:view'),
    reportsController.getCurrentStockReport
);

router.get('/stock-movement',
    requirePermission('feature:reports:view'),
    reportsController.getStockMovementReport
);

// Module-wise Reports
router.get('/inbound',
    requirePermission('feature:reports:view'),
    reportsController.getInboundReport
);

router.get('/outbound',
    requirePermission('feature:reports:view'),
    reportsController.getOutboundReport
);

router.get('/qc',
    requirePermission('feature:reports:view'),
    reportsController.getQCReport
);

router.get('/picking',
    requirePermission('feature:reports:view'),
    reportsController.getPickingReport
);

// Performance Reports
router.get('/user-performance',
    requirePermission('feature:reports:view'),
    reportsController.getUserPerformanceReport
);

router.get('/warehouse-summary',
    requirePermission('feature:reports:view'),
    reportsController.getWarehouseSummary
);

// Export Reports
router.get('/export',
    requirePermission('feature:reports:export'),
    reportsController.exportReportToExcel
);

// Analytics Endpoints
router.get('/trend-analysis',
    requirePermission('feature:reports:view'),
    reportsController.getTrendAnalysis
);

router.get('/qc-analysis',
    requirePermission('feature:reports:view'),
    reportsController.getQCAnalysis
);

router.get('/performance-metrics',
    requirePermission('feature:reports:view'),
    reportsController.getPerformanceMetrics
);

router.get('/exception-reports',
    requirePermission('feature:reports:view'),
    reportsController.getExceptionReports
);

export default router;
