import { Router } from 'express';

import {
  getPendingInboundForQC,
  getQCList,
  createQCEntry,
  bulkQCUpload,
  multiQCEntry,
  getQCStats,
  getQCBatches,
  deleteQCBatch,
  getQCBrands,
  getQCCategories,
  exportQCData,
  deleteQCEntry,
  getQCTemplate,
  getAllQCWSNs,
  saveQCDraft,
  loadQCDraft,
  clearQCDraft,
  syncQCRows,
} from '../controllers/qc.controller';

import { authMiddleware, hasRole } from '../middleware/auth.middleware';
import {
  requirePermission,
  requireWarehouseAccess,
  injectWarehouseFilter
} from '../middleware/rbac.middleware';
import { upload } from '../middleware/upload.middleware';
import { listTimeout, uploadTimeout } from '../middleware/timeout.middleware';

const router = Router();

// Base authentication for all routes
router.use(authMiddleware);

// View routes - require view permission (extended timeout for large lists)
router.get('/pending-inbound', listTimeout, injectWarehouseFilter, requirePermission('feature:qc:view'), getPendingInboundForQC);
router.get('/wsns/all', listTimeout, injectWarehouseFilter, requirePermission('feature:qc:view'), getAllQCWSNs);
router.get('/list', listTimeout, injectWarehouseFilter, requirePermission('feature:qc:view'), getQCList);
router.get('/stats', injectWarehouseFilter, requirePermission('feature:qc:view'), getQCStats);
router.get('/batches', listTimeout, injectWarehouseFilter, requirePermission('feature:qc:view'), getQCBatches);
router.get('/brands', requirePermission('feature:qc:view'), getQCBrands);
router.get('/categories', requirePermission('feature:qc:view'), getQCCategories);
router.get('/template', requirePermission('feature:qc:view'), getQCTemplate);

// Create routes - require process permission
router.post('/create', requireWarehouseAccess, requirePermission('feature:qc:process'), createQCEntry);
router.post('/multi-entry', requireWarehouseAccess, requirePermission('feature:qc:process'), multiQCEntry);
router.post('/bulk-upload', uploadTimeout, requireWarehouseAccess, requirePermission('feature:qc:process'), upload.single('file'), bulkQCUpload);

// Delete routes - require delete permission
router.delete('/delete/:qcId', injectWarehouseFilter, requirePermission('feature:qc:delete'), deleteQCEntry);
router.delete('/batch/:batchId', injectWarehouseFilter, requirePermission('feature:qc:delete'), deleteQCBatch);

// Multi-entry draft persistence routes (save/load/clear draft from database)
router.get('/draft', requirePermission('feature:qc:view'), loadQCDraft);
router.put('/draft', requirePermission('feature:qc:process'), saveQCDraft);
router.delete('/draft', requirePermission('feature:qc:process'), clearQCDraft);

// Real-time multi-entry row sync across devices (SSE relay, no DB write)
router.post('/sync-rows', requirePermission('feature:qc:process'), syncQCRows);

// Export (extended timeout) - require view permission
router.get('/export', listTimeout, injectWarehouseFilter, requirePermission('feature:qc:view'), exportQCData);

export default router;