// File Path = warehouse-backend/src/routes/master-data.routes.ts
import express, { NextFunction, Router } from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { authMiddleware } from '../middleware/auth.middleware';
import { injectWarehouseFilter, requireWarehouseAccess, requirePermissionOrRole } from '../middleware/rbac.middleware';
import * as ctrl from '../controllers/master-data.controller';

const router = express.Router();

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, 'uploads/'),
  filename: (req, file, cb) => cb(null, `${Date.now()}_${file.originalname}`)
});

const upload = multer({
  storage,
  limits: { fileSize: 2 * 1024 * 1024 * 1024 }, // 2GB
  fileFilter: (req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    if (['.xlsx', '.xls', '.csv'].includes(ext)) cb(null, true);
    else cb(new Error('Invalid file type'));
  }
});

// All routes require authentication
router.use(authMiddleware);

// Permission-based access for CUD operations
const canManageMasterData = requirePermissionOrRole('menu:settings:masterdata', 'admin');

// Cache APIs for frontend IndexedDB sync (read-only — any authenticated user)
router.get('/count', injectWarehouseFilter, ctrl.getMasterDataCount);
router.get('/batch', injectWarehouseFilter, ctrl.getMasterDataBatch);
router.get('/batch-list', injectWarehouseFilter, ctrl.getMasterDataBatchList);  // Get list of all batches
router.get('/by-batch', injectWarehouseFilter, ctrl.getMasterDataByBatchIds);   // Get data for specific batch(es)

// Template download (read-only)
router.get('/download-template', canManageMasterData, ctrl.downloadTemplate);

// View routes (read-only — any authenticated user, warehouse-scoped)
router.get('/', injectWarehouseFilter, ctrl.getMasterData);
router.get('/batches', injectWarehouseFilter, ctrl.getBatches);
router.get('/brands', injectWarehouseFilter, ctrl.getBrands);
router.get('/categories', injectWarehouseFilter, ctrl.getCategories);
router.get('/export', injectWarehouseFilter, ctrl.exportMasterData);

// Create single product
router.post('/', canManageMasterData, ctrl.createMasterData);

// Update product
router.put('/:id', canManageMasterData, ctrl.updateMasterData);

// Upload routes
router.post('/upload', canManageMasterData, upload.single('file'), ctrl.uploadMasterData);
router.get('/upload/progress/:jobId', ctrl.getUploadProgress);
router.get('/upload/active', ctrl.getActiveUploads);
router.get('/upload/history', injectWarehouseFilter, ctrl.getUploadHistory);
router.get('/upload/duplicates/:jobId', ctrl.getUploadDuplicates);
router.delete('/upload/cancel/:jobId', canManageMasterData, ctrl.cancelUpload);
router.delete('/upload/history/:id', canManageMasterData, injectWarehouseFilter, ctrl.deleteUploadLog);

// Delete routes
router.delete('/:id', canManageMasterData, ctrl.deleteMasterData);
router.delete('/batch/:batchId', canManageMasterData, injectWarehouseFilter, ctrl.deleteBatch);

// Phase 5: Advanced features
router.post('/batch/:batchId/restore', canManageMasterData, injectWarehouseFilter, ctrl.restoreBatch);
router.get('/snapshots', injectWarehouseFilter, ctrl.getSnapshots);
router.get('/deleted', injectWarehouseFilter, ctrl.getDeletedRecords);
router.delete('/deleted/purge-all', canManageMasterData, injectWarehouseFilter, ctrl.purgeAllDeletedRecords);
router.delete('/deleted/purge/:id', canManageMasterData, injectWarehouseFilter, ctrl.purgeDeletedRecord);
router.delete('/cleanup/stale', canManageMasterData, injectWarehouseFilter, ctrl.cleanupStaleData);

export default router;