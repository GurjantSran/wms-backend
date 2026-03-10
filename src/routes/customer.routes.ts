// File Path = warehouse-backend/src/routes/customer.routes.ts
import { Router } from 'express';
import { authMiddleware, hasRole } from '../middleware/auth.middleware';
import {
  requirePermission,
  requireWarehouseAccess,
  injectWarehouseFilter
} from '../middleware/rbac.middleware';
import {
  getCustomers,
  getCustomerById,
  createCustomer,
  updateCustomer,
  deleteCustomer,
  getCustomerNames,
  lookupPincode,
  lookupGSTNumber
} from '../controllers/customer.controller';

const router = Router();

// All routes require authentication
router.use(authMiddleware);

// Pincode lookup (public within authenticated users - no special permission needed)
router.get('/pincode/:pincode', lookupPincode);

// GST Number lookup (public within authenticated users - no special permission needed)
router.get('/gst/:gstin', lookupGSTNumber);

// View routes - require view permission
router.get('/', injectWarehouseFilter, requirePermission('feature:customers:view'), getCustomers);
router.get('/names', injectWarehouseFilter, requirePermission('feature:customers:view'), getCustomerNames);
router.get('/:id', requirePermission('feature:customers:view'), getCustomerById);

// Create routes - require create permission
router.post('/', requireWarehouseAccess, requirePermission('feature:customers:create'), createCustomer);

// Edit routes - require edit permission
router.put('/:id', requirePermission('feature:customers:edit'), updateCustomer);

// Delete routes - require delete permission
router.delete('/:id', requirePermission('feature:customers:delete'), deleteCustomer);

export default router;