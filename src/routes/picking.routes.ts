// File Path = warehouse-backend/src/routes/picking.routes.ts
import { Router } from 'express';
import { authMiddleware, hasRole, } from '../middleware/auth.middleware';
import {
  requirePermission,
  requireWarehouseAccess,
  injectWarehouseFilter
} from '../middleware/rbac.middleware';
import {
  getSourceByWSN,
  multiPickingEntry,
  getPickingList,
  getCustomers,
  checkWSNExists,
  getExistingWSNs,
  getBatches,
  deleteBatch,
  getBrands,
  getCategories,
  savePickingDraft,
  loadPickingDraft,
  clearPickingDraft,
  syncPickingRows,
} from '../controllers/picking.controller';

const router = Router();

// All picking routes require authentication
router.use(authMiddleware);

// View routes - require view permission
router.get('/source-by-wsn', requirePermission('feature:picking:view'), getSourceByWSN);
router.get('/list', injectWarehouseFilter, requirePermission('feature:picking:view'), getPickingList);
router.get('/customers', requirePermission('feature:picking:view'), getCustomers);
router.get('/check-wsn', requirePermission('feature:picking:view'), checkWSNExists);
router.get('/existing-wsns', injectWarehouseFilter, requirePermission('feature:picking:view'), getExistingWSNs);
router.get('/batches', injectWarehouseFilter, requirePermission('feature:picking:view'), getBatches);
router.get('/brands', requirePermission('feature:picking:view'), getBrands);
router.get('/categories', requirePermission('feature:picking:view'), getCategories);

// Multi-entry draft persistence routes (save/load/clear draft from database)
router.get('/draft', requirePermission('feature:picking:view'), loadPickingDraft);
router.put('/draft', requirePermission('feature:picking:create'), savePickingDraft);
router.delete('/draft', requirePermission('feature:picking:create'), clearPickingDraft);

// Real-time multi-entry row sync across devices (SSE relay, no DB write)
router.post('/sync-rows', requirePermission('feature:picking:create'), syncPickingRows);

// Create routes - require create permission
router.post('/multi-entry', requireWarehouseAccess, requirePermission('feature:picking:create'), multiPickingEntry);

// Delete routes - require delete permission
router.delete('/batch/:batchId', injectWarehouseFilter, requirePermission('feature:picking:delete'), deleteBatch);

export default router;