// File Path = warehouse-backend/src/routes/ui-access.routes.ts
import { Router } from 'express';
import { authMiddleware } from '../middleware/auth.middleware';
import { requirePermissionOrRole } from '../middleware/rbac.middleware';
import * as uiAccessController from '../controllers/ui-access.controller';

const router = Router();

// All routes require authentication
router.use(authMiddleware);

// Permission-based access: allows users with 'menu:settings:permissions' override OR admin role
const canManageAccess = requirePermissionOrRole('menu:settings:permissions', 'admin');

// Get my UI access (for current user — self-service)
router.get('/my-access', uiAccessController.getMyAccess);

// Get all UI elements (read-only — any authenticated user)
router.get('/elements', uiAccessController.getAllElements);

// Roles management
router.get('/roles', uiAccessController.getRoles);
router.post('/roles', canManageAccess, uiAccessController.createRole);
router.delete('/roles/:roleId', canManageAccess, uiAccessController.deleteRole);

// Role access management
router.get('/roles/:roleId/access', uiAccessController.getRoleAccess);
router.put('/roles/:roleId/access', canManageAccess, uiAccessController.updateRoleAccess);

// User overrides management
router.get('/users/:userId/overrides', canManageAccess, uiAccessController.getUserOverrides);
router.put('/users/:userId/overrides', canManageAccess, uiAccessController.updateUserOverrides);

// User warehouses
router.get('/users/:userId/warehouses', canManageAccess, uiAccessController.getUserWarehouses);
router.put('/users/:userId/warehouses', canManageAccess, uiAccessController.updateUserWarehouses);

export default router;
