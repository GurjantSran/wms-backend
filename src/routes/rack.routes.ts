// File Path = warehouse-backend/src/routes/rack.routes.ts
import express, { Router } from 'express';
import multer from 'multer';
import path from 'path';
import { authMiddleware } from '../middleware/auth.middleware';
import { requirePermissionOrRole } from '../middleware/rbac.middleware';
import * as rackController from '../controllers/rack.controller';

const router: Router = express.Router();

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
  limits: { fileSize: 10 * 1024 * 1024 },
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

// Permission-based access for CUD operations
const canManageRacks = requirePermissionOrRole('menu:settings:racks', 'admin');

// GET routes - all authenticated users can view racks
router.get('/', rackController.getRacks);
router.get('/by-warehouse', rackController.getRacksByWarehouse);

// POST routes
router.post('/', canManageRacks, rackController.createRack);
router.post('/bulk-upload', canManageRacks, upload.single('file'), rackController.bulkUploadRacks);

// PUT routes
router.put('/:id', canManageRacks, rackController.updateRack);
router.patch('/:id/toggle', canManageRacks, rackController.toggleRackStatus);

// DELETE routes
router.delete('/:id', canManageRacks, rackController.deleteRack);

export default router;
