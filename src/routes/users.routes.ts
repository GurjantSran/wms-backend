// File Path = warehouse-backend/src/routes/users.routes.ts
import express, { Router } from 'express';
import { authMiddleware, adminOnly } from '../middleware/auth.middleware';
import { requirePermission } from '../middleware/rbac.middleware';
import * as usersController from '../controllers/users.controller';

const router: Router = express.Router();

// All routes require authentication
router.use(authMiddleware);

// View routes - require view permission
router.get('/', requirePermission('feature:users:view'), usersController.getUsers);

// Create routes - require create permission
router.post('/', requirePermission('feature:users:create'), usersController.createUser);

// Edit routes - require edit permission
router.put('/:id', requirePermission('feature:users:edit'), usersController.updateUser);

// Delete routes - require delete permission
router.delete('/:id', requirePermission('feature:users:delete'), usersController.deleteUser);

// Password change - require specific permission
router.patch('/:id/change-password', requirePermission('feature:users:change-password'), usersController.changePassword);

// User-warehouses - require assign-warehouses permission
router.get('/:id/warehouses', requirePermission('feature:users:view'), usersController.getUserWarehouses);
router.put('/:id/warehouses', requirePermission('feature:users:assign-warehouses'), usersController.setUserWarehouses);

export default router;
