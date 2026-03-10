// File Path = warehouse-backend/src/routes/inbound.routes.ts
import express, { Router } from 'express';
import multer from 'multer';
import path from 'path';
import { authMiddleware, hasRole } from '../middleware/auth.middleware';
import {
  requirePermission,
  requireWarehouseAccess,
  injectWarehouseFilter
} from '../middleware/rbac.middleware';
import { listTimeout, uploadTimeout } from '../middleware/timeout.middleware';
import * as inboundController from '../controllers/inbound.controller';
import { multiInboundEntry } from "../controllers/inbound.controller";


const router: Router = express.Router();

// Multer configuration
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, 'uploads/');
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + path.extname(file.originalname));
  }
});

const upload = multer({
  storage: storage,
  limits: { fileSize: 200 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowedExts = ['.xlsx', '.xls'];
    const fileExt = path.extname(file.originalname).toLowerCase();
    if (allowedExts.includes(fileExt)) {
      cb(null, true);
    } else {
      cb(new Error('Invalid file format'));
    }
  }
});

// All routes require authentication
router.use(authMiddleware);

// View routes - require view permission and inject warehouse filter
// Extended timeout for large data lists
router.get('/', listTimeout, injectWarehouseFilter, requirePermission('feature:inbound:view'), inboundController.getInboundList);
router.get('/batches', listTimeout, injectWarehouseFilter, requirePermission('feature:inbound:view'), inboundController.getInboundBatches);
router.get('/master-data/:wsn', requirePermission('feature:inbound:view'), inboundController.getMasterDataByWSN);
router.get('/brands', requirePermission('feature:inbound:view'), inboundController.getBrands);
router.get('/categories', requirePermission('feature:inbound:view'), inboundController.getCategories);
router.get('/wsns/all', listTimeout, injectWarehouseFilter, requirePermission('feature:inbound:view'), inboundController.getAllInboundWSNs);
router.get('/racks/:warehouseId', requireWarehouseAccess, requirePermission('feature:inbound:view'), inboundController.getWarehouseRacks);

// Receiving WSNs tracking routes (for multi-entry scanning status)
router.get('/receiving-wsns', requirePermission('feature:inbound:view'), inboundController.getReceivingWSNs);
router.post('/receiving-wsns/sync', requireWarehouseAccess, requirePermission('feature:inbound:create'), inboundController.syncReceivingWSNs);
router.post('/receiving-wsns/clear', requirePermission('feature:inbound:create'), inboundController.clearReceivingWSNs);

// Multi-entry draft persistence routes (save/load/clear draft from database)
router.get('/draft', requirePermission('feature:inbound:view'), inboundController.loadDraft);
router.put('/draft', requirePermission('feature:inbound:create'), inboundController.saveDraft);
router.delete('/draft', requirePermission('feature:inbound:create'), inboundController.clearDraft);

// Real-time multi-entry row sync across devices (SSE relay, no DB write)
router.post('/sync-rows', requirePermission('feature:inbound:create'), inboundController.syncRows);

// Bulk check WSNs (for paste duplicate detection - single query instead of N calls)
router.post('/bulk-check-wsns', requirePermission('feature:inbound:view'), inboundController.bulkCheckInboundWSNs);

// Create routes - require create permission and warehouse access
router.post('/', requireWarehouseAccess, requirePermission('feature:inbound:create'), inboundController.createInboundEntry);
router.post('/multi-entry', requireWarehouseAccess, requirePermission('feature:inbound:create'), inboundController.multiInboundEntry);

// Upload routes - require upload permission (extended timeout)
router.post('/bulk-upload', uploadTimeout, requireWarehouseAccess, requirePermission('feature:inbound:upload'), upload.single('file'), inboundController.bulkInboundUpload);

// Delete routes - require delete permission
router.delete('/batches/:batchId', injectWarehouseFilter, requirePermission('feature:inbound:delete'), inboundController.deleteInboundBatch);

// Print proxy routes - mobile devices send print requests via backend to local Print Agent
router.get('/print-proxy/health', requirePermission('feature:inbound:view'), inboundController.printProxyHealth);
router.post('/print-proxy/print-label', requirePermission('feature:inbound:create'), inboundController.printProxyLabel);
router.get('/print-proxy/printers', requirePermission('feature:inbound:view'), inboundController.printProxyPrinters);

export default router;