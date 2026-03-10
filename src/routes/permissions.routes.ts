// File Path = warehouse-backend/src/routes/permissions.routes.ts
import { Router } from 'express';
import { authMiddleware } from '../middleware/auth.middleware';
import { sensitiveWriteRateLimit } from '../middleware/rateLimit.middleware';
import { requirePermissionOrRole } from '../middleware/rbac.middleware';
import * as permissionsController from '../controllers/permissions.controller';
import * as approvalController from '../controllers/permissionApproval.controller';

const router = Router();

// All routes require authentication
router.use(authMiddleware);
// NOTE: sensitiveWriteRateLimit is applied only to CUD operations below,
// NOT to reads. Read endpoints are covered by the global API limiter only.

// Permission-based access: allows users with 'menu:settings:permissions' override OR admin role
const canManagePermissions = requirePermissionOrRole('menu:settings:permissions', 'admin');

// =============================================================
// Current User's Permissions (self-service — any authenticated user)
// =============================================================
router.get('/me', permissionsController.getMyPermissions);
router.get('/check/:code', permissionsController.checkPermission);

// =============================================================
// All Permissions List (read-only — any authenticated user)
// =============================================================
router.get('/', permissionsController.getAllPermissions);

// =============================================================
// Roles Management (admin/super_admin only)
// =============================================================
router.get('/roles', permissionsController.getRoles);
router.get('/roles/:roleId/permissions', permissionsController.getRolePermissions);
router.put('/roles/:roleId/permissions', sensitiveWriteRateLimit, canManagePermissions, permissionsController.updateRolePermissions);

// =============================================================
// User Overrides (admin/super_admin only for modifications)
// =============================================================
router.get('/users/:userId/overrides', canManagePermissions, permissionsController.getUserOverrides);
router.put('/users/:userId/overrides', sensitiveWriteRateLimit, canManagePermissions, permissionsController.updateUserOverrides);

// =============================================================
// User Warehouses (admin/super_admin only for modifications)
// =============================================================
router.get('/users/:userId/warehouses', canManagePermissions, permissionsController.getUserWarehouses);
router.put('/users/:userId/warehouses', sensitiveWriteRateLimit, canManagePermissions, permissionsController.updateUserWarehouses);

// =============================================================
// Permission Approval Workflow (admin/super_admin for review actions)
// =============================================================
router.get('/approval/pending-count', approvalController.getPendingCount);
router.get('/approval/requests', canManagePermissions, approvalController.getApprovalRequests);
router.get('/approval/requests/:id', canManagePermissions, approvalController.getApprovalRequestDetails);
router.get('/approval/my-requests', approvalController.getMyRequests);
router.post('/approval/role-request', sensitiveWriteRateLimit, approvalController.createRolePermissionRequest);
router.post('/approval/user-request', sensitiveWriteRateLimit, approvalController.createUserOverrideRequest);
router.put('/approval/requests/:id/changes', sensitiveWriteRateLimit, canManagePermissions, approvalController.updateChangeApproval);
router.post('/approval/requests/:id/finalize', sensitiveWriteRateLimit, canManagePermissions, approvalController.finalizeRequest);
router.delete('/approval/requests/:id', sensitiveWriteRateLimit, approvalController.cancelRequest);

export default router;
