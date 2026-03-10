// File Path = warehouse-backend/src/routes/warehouses.routes.ts
import express, { Router } from 'express';
import { authMiddleware } from '../middleware/auth.middleware';
import { requirePermissionOrRole } from '../middleware/rbac.middleware';
import * as warehouseController from '../controllers/warehouses.controller';

const router: Router = express.Router();

// Permission-based access for CUD operations
const canManageWarehouses = requirePermissionOrRole('menu:settings:warehouses', 'admin');

// GET routes — any authenticated user can view warehouses
router.get('/', authMiddleware, warehouseController.getWarehouses);

// CUD routes — permission or admin role required
router.post('/', authMiddleware, canManageWarehouses, warehouseController.createWarehouse);
router.put('/:id', authMiddleware, canManageWarehouses, warehouseController.updateWarehouse);
router.delete('/:id', authMiddleware, canManageWarehouses, warehouseController.deleteWarehouse);
router.patch('/:id/set-active', authMiddleware, canManageWarehouses, warehouseController.setActiveWarehouse);

export default router;
