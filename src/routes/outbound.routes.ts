// File Path = warehouse-backend/src/routes/outbound.routes.ts
import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { authMiddleware, hasRole, } from '../middleware/auth.middleware';
import {
  requirePermission,
  requireWarehouseAccess,
  injectWarehouseFilter
} from '../middleware/rbac.middleware';
import { listTimeout, bulkUploadTimeout } from '../middleware/timeout.middleware';
import {
  getAllOutboundWSNs,
  getPendingForOutbound,
  getSourceByWSN,
  createSingleEntry,
  multiEntry,
  bulkUpload,
  getList,
  getCustomers,
  getExistingWSNs,
  getBatches,
  deleteBatch,
  exportToExcel,
  getBrands,
  getCategories,
  getSources,
  getAvailableForOutbound,
  saveOutboundDraft,
  loadOutboundDraft,
  clearOutboundDraft,
  syncOutboundRows,
  syncOutboundHeader,
  syncDispatchingWSNs,
  clearDispatchingWSNs,
  getDispatchingWSNs,
} from '../controllers/outbound.controller';

const router = Router();

// Configure multer with disk storage for bulk uploads (100MB)
// Using disk storage prevents memory exhaustion with large files
const uploadDir = path.join(__dirname, '../../uploads');
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, `outbound-${uniqueSuffix}${path.extname(file.originalname)}`);
  }
});

const upload = multer({
  storage,
  limits: { fileSize: 100 * 1024 * 1024 } // 100MB limit
});

// All routes require authentication
router.use(authMiddleware);

// View routes - require view permission (extended timeout for large lists)
router.get('/all-wsns', listTimeout, injectWarehouseFilter, requirePermission('feature:outbound:view'), getAllOutboundWSNs);
router.get('/pending', listTimeout, injectWarehouseFilter, requirePermission('feature:outbound:view'), getPendingForOutbound);
router.get('/source-by-wsn', requirePermission('feature:outbound:view'), getSourceByWSN);
router.get('/list', listTimeout, injectWarehouseFilter, requirePermission('feature:outbound:view'), getList);
router.get('/customers', requirePermission('feature:outbound:view'), getCustomers);
router.get('/existing-wsns', listTimeout, injectWarehouseFilter, requirePermission('feature:outbound:view'), getExistingWSNs);
router.get('/batches', listTimeout, injectWarehouseFilter, requirePermission('feature:outbound:view'), getBatches);
router.get('/brands', requirePermission('feature:outbound:view'), getBrands);
router.get('/categories', requirePermission('feature:outbound:view'), getCategories);
router.get('/sources', requirePermission('feature:outbound:view'), getSources);
router.get('/available-inventory', listTimeout, requirePermission('feature:outbound:view'), getAvailableForOutbound);

// Multi-entry draft persistence routes (save/load/clear draft from database)
router.get('/draft', requirePermission('feature:outbound:view'), loadOutboundDraft);
router.put('/draft', requirePermission('feature:outbound:create'), saveOutboundDraft);
router.delete('/draft', requirePermission('feature:outbound:create'), clearOutboundDraft);

// Real-time multi-entry row sync across devices (SSE relay, no DB write)
router.post('/sync-rows', requirePermission('feature:outbound:create'), syncOutboundRows);

// Real-time header field sync across devices (SSE relay, no DB write)
router.post('/sync-header', requirePermission('feature:outbound:create'), syncOutboundHeader);

// Dispatching WSNs tracking (for "Outbound in Process" status in inbound list)
router.post('/dispatching-wsns/sync', requireWarehouseAccess, requirePermission('feature:outbound:create'), syncDispatchingWSNs);
router.post('/dispatching-wsns/clear', requirePermission('feature:outbound:create'), clearDispatchingWSNs);
router.get('/dispatching-wsns', requirePermission('feature:outbound:view'), getDispatchingWSNs);

// Export routes - require export permission (extended timeout)
router.get('/export', listTimeout, injectWarehouseFilter, requirePermission('feature:outbound:export'), exportToExcel);

// Create routes - require create permission
router.post('/single', requireWarehouseAccess, requirePermission('feature:outbound:create'), createSingleEntry);
router.post('/multi', requireWarehouseAccess, requirePermission('feature:outbound:create'), multiEntry);
// Bulk upload with 30 minute timeout for massive files (500K-5M rows)
router.post('/bulk', bulkUploadTimeout, requireWarehouseAccess, requirePermission('feature:outbound:create'), upload.single('file'), bulkUpload);

// Delete routes - require delete permission
router.delete('/batch/:batchId', injectWarehouseFilter, requirePermission('feature:outbound:delete'), deleteBatch);

export default router;