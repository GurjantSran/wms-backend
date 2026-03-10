// File Path = warehouse-backend/src/routes/auth.routes.ts
import express, { Router } from 'express';
import * as authController from '../controllers/auth.controller';
import { loginRateLimit } from '../middleware/rateLimit.middleware';
import { authMiddleware } from '../middleware/auth.middleware';
import { requirePermissionOrRole } from '../middleware/rbac.middleware';

const router: Router = express.Router();

// Apply rate limiting to login (5 attempts per 15 minutes)
router.post('/login', loginRateLimit, authController.login);

// Register — requires 'feature:users:create' permission OR admin role
router.post('/register', authMiddleware, requirePermissionOrRole('feature:users:create', 'admin'), authController.register);

export default router;
