// File Path = warehouse-backend/src/routes/rejection.routes.ts
import express, { Router } from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { authMiddleware } from '../middleware/auth.middleware';
import {
    requirePermission,
    requireWarehouseAccess,
    injectWarehouseFilter
} from '../middleware/rbac.middleware';
import { uploadTimeout } from '../middleware/timeout.middleware';
import * as rejectionController from '../controllers/rejection.controller';

const router: Router = express.Router();

// Ensure upload directory exists
const uploadDir = path.join(__dirname, '../../uploads/rejections');
if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir, { recursive: true });
}

// Multer configuration for rejection Excel uploads
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, uploadDir);
    },
    filename: (req, file, cb) => {
        cb(null, `rejection_${Date.now()}${path.extname(file.originalname)}`);
    }
});

const upload = multer({
    storage: storage,
    limits: { fileSize: 10 * 1024 * 1024 }, // 10MB limit
    fileFilter: (req, file, cb) => {
        const allowedExts = ['.xlsx', '.xls'];
        const fileExt = path.extname(file.originalname).toLowerCase();
        if (allowedExts.includes(fileExt)) {
            cb(null, true);
        } else {
            cb(new Error('Invalid file format. Only .xlsx and .xls files are allowed.'));
        }
    }
});

// All routes require authentication
router.use(authMiddleware);

// Template download - anyone with view permission can download
router.get('/template', requirePermission('feature:rejections:view'), rejectionController.downloadTemplate);

// List and filter rejections
router.get('/', injectWarehouseFilter, requirePermission('feature:rejections:view'), rejectionController.getRejections);

// Get summary for credit note tracking
router.get('/summary', injectWarehouseFilter, requirePermission('feature:rejections:view'), rejectionController.getRejectionSummary);

// Get unique persons for filter dropdown (legacy — reads from rejections table)
router.get('/persons', injectWarehouseFilter, requirePermission('feature:rejections:view'), rejectionController.getRejectedByPersons);

// Managed persons CRUD
router.get('/managed-persons', injectWarehouseFilter, requirePermission('feature:rejections:view'), rejectionController.getManagedPersons);
router.post('/managed-persons', requirePermission('feature:rejections:create'), rejectionController.addManagedPerson);
router.delete('/managed-persons/:id', requirePermission('feature:rejections:delete'), rejectionController.deleteManagedPerson);

// Get upload batches for filter dropdown
router.get('/batches', injectWarehouseFilter, requirePermission('feature:rejections:view'), rejectionController.getRejectionBatches);

// Export to Excel
router.get('/export', injectWarehouseFilter, requirePermission('feature:rejections:export'), rejectionController.exportRejections);

// Upload history
router.get('/upload-history', injectWarehouseFilter, requirePermission('feature:rejections:view'), rejectionController.getUploadHistory);

// Upload rejection Excel - requires create permission
router.post('/upload', uploadTimeout, requireWarehouseAccess, requirePermission('feature:rejections:create'), upload.single('file'), rejectionController.uploadRejections);

// Update credit note info
router.put('/credit-note', requirePermission('feature:rejections:credit'), rejectionController.updateCreditNote);

// Rename batch
router.put('/batch/:batchId/rename', requirePermission('feature:rejections:create'), rejectionController.renameBatch);

// Restore soft-deleted batch
router.put('/batch/:batchId/restore', requirePermission('feature:rejections:create'), rejectionController.restoreBatch);

// Get soft-deleted batches
router.get('/deleted-batches', injectWarehouseFilter, requirePermission('feature:rejections:view'), rejectionController.getDeletedBatches);

// Delete single rejection (soft delete)
router.delete('/:id', requirePermission('feature:rejections:delete'), rejectionController.deleteRejection);

// Delete rejection batch (soft delete)
router.delete('/batch/:batchId', requirePermission('feature:rejections:delete'), rejectionController.deleteRejectionBatch);

// Permanently delete a batch from trash
router.delete('/batch/:batchId/permanent', requirePermission('feature:rejections:delete'), rejectionController.permanentDeleteBatch);

// Delete upload history log entry
router.delete('/upload-history/:id', requirePermission('feature:rejections:delete'), rejectionController.deleteUploadLog);

export default router;
