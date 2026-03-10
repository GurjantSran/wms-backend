// File Path = warehouse-backend/src/middleware/rbac.middleware.ts
import { Request, Response, NextFunction } from 'express';
import { query } from '../config/database';
import logger from '../utils/logger';

/**
 * Interface for permission check results
 */
interface PermissionResult {
    is_enabled: boolean;
    is_visible: boolean;
    permission_source: 'user' | 'role' | 'legacy';
}

/**
 * Interface for warehouse access
 */
interface WarehouseAccess {
    warehouse_id: number;
    warehouse_name: string;
    is_default: boolean;
}

/**
 * Cache for permissions (in production, use Redis)
 */
const permissionCache = new Map<string, { data: any; expiry: number }>();
const CACHE_TTL = 60000; // 1 minute

// Track if RBAC tables exist
let rbacTablesExist: boolean | null = null;
let warehouseTablesExist: boolean | null = null;

/**
 * Check if RBAC tables exist in the database
 * Performs a real DB check and caches result for performance.
 * If tables exist, full RBAC permission enforcement is active.
 * If tables don't exist, falls back to restrictive role-based access.
 */
async function checkRbacTablesExist(): Promise<boolean> {
    if (rbacTablesExist !== null) {
        return rbacTablesExist;
    }

    try {
        // Check if both required tables/views exist
        await query(`SELECT 1 FROM effective_user_permissions LIMIT 1`);
        rbacTablesExist = true;
        logger.info('RBAC tables detected - full permission enforcement active');
    } catch (error) {
        rbacTablesExist = false;
        logger.warn('RBAC tables not found - using restrictive role-based fallback (admin/super_admin only for protected routes)');
    }
    return rbacTablesExist;
}

/**
 * Check if user_warehouses table exists (separate from full RBAC)
 */
async function checkWarehouseTablesExist(): Promise<boolean> {
    if (warehouseTablesExist !== null) {
        return warehouseTablesExist;
    }

    try {
        await query(`SELECT 1 FROM user_warehouses LIMIT 1`);
        warehouseTablesExist = true;
    } catch (error) {
        logger.debug('user_warehouses table not found - using legacy warehouse access');
        warehouseTablesExist = false;
    }
    return warehouseTablesExist;
}

/**
 * Get cached or fetch fresh permissions
 */
async function getCachedPermissions(userId: number, role: string): Promise<Map<string, PermissionResult>> {
    const cacheKey = `perms_${userId}`;
    const cached = permissionCache.get(cacheKey);

    if (cached && cached.expiry > Date.now()) {
        return cached.data;
    }

    // Check if RBAC tables exist
    const tablesExist = await checkRbacTablesExist();

    if (!tablesExist) {
        // Restrictive fallback: only admin/super_admin get blanket access
        // Non-admin users are denied when RBAC tables are missing
        const permissions = new Map<string, PermissionResult>();
        if (role === 'admin' || role === 'super_admin') {
            permissions.set('__legacy_admin__', {
                is_enabled: true,
                is_visible: true,
                permission_source: 'legacy'
            });
        }
        // Non-admin users get empty permissions (will be denied by requirePermission)
        permissionCache.set(cacheKey, { data: permissions, expiry: Date.now() + CACHE_TTL });
        return permissions;
    }

    const result = await query(`
    SELECT 
      permission_code,
      is_enabled,
      is_visible,
      permission_source
    FROM effective_user_permissions
    WHERE user_id = $1 AND is_enabled = true
  `, [userId]);

    const permissions = new Map<string, PermissionResult>();
    for (const row of result.rows) {
        permissions.set(row.permission_code, {
            is_enabled: row.is_enabled,
            is_visible: row.is_visible,
            permission_source: row.permission_source
        });
    }

    permissionCache.set(cacheKey, { data: permissions, expiry: Date.now() + CACHE_TTL });
    return permissions;
}

/**
 * Get user's accessible warehouses
 * Now checks user_warehouses table separately from RBAC permission system
 */
async function getUserWarehouses(userId: number, legacyWarehouseId?: number): Promise<WarehouseAccess[]> {
    const cacheKey = `warehouses_${userId}`;
    const cached = permissionCache.get(cacheKey);

    if (cached && cached.expiry > Date.now()) {
        return cached.data;
    }

    // Check if user_warehouses table exists (separate from full RBAC system)
    const tablesExist = await checkWarehouseTablesExist();

    if (tablesExist) {
        // First try to get warehouses from user_warehouses table
        try {
            const result = await query(`
                SELECT DISTINCT
                    uw.warehouse_id,
                    w.name as warehouse_name,
                    uw.is_default
                FROM user_warehouses uw
                JOIN warehouses w ON w.id = uw.warehouse_id
                WHERE uw.user_id = $1 AND w.is_active = true
            `, [userId]);

            if (result.rows.length > 0) {
                const warehouses = result.rows;
                permissionCache.set(cacheKey, { data: warehouses, expiry: Date.now() + CACHE_TTL });
                return warehouses;
            }
            // If no entries in user_warehouses, return empty array (means all access)
            permissionCache.set(cacheKey, { data: [], expiry: Date.now() + CACHE_TTL });
            return [];
        } catch (error) {
            logger.debug('Error querying user_warehouses, falling back to legacy mode');
        }
    }

    // Legacy mode: use user's warehouse_id from token
    if (legacyWarehouseId) {
        const whResult = await query('SELECT id, name, code FROM warehouses WHERE id = $1 AND is_active = true', [legacyWarehouseId]);
        if (whResult.rows.length > 0) {
            const warehouses = [{
                warehouse_id: whResult.rows[0].id,
                warehouse_name: whResult.rows[0].name,
                is_default: true
            }];
            permissionCache.set(cacheKey, { data: warehouses, expiry: Date.now() + CACHE_TTL });
            return warehouses;
        }
    }
    return [];
}

/**
 * Clear permission cache for a user
 */
export function clearPermissionCache(userId?: number) {
    if (userId) {
        permissionCache.delete(`perms_${userId}`);
        permissionCache.delete(`warehouses_${userId}`);
    } else {
        permissionCache.clear();
    }
    // Reset table existence checks on full cache clear
    if (!userId) {
        rbacTablesExist = null;
        warehouseTablesExist = null;
    }
}

/**
 * Middleware: Check if user has a specific permission OR has one of the fallback roles.
 * This enables override-granted users to access routes that were previously role-locked,
 * while keeping backwards compatibility for existing admin/super_admin users.
 * Usage: requirePermissionOrRole('menu:settings:backups', 'admin')
 */
export const requirePermissionOrRole = (permissionCode: string, ...fallbackRoles: string[]) => {
    return async (req: Request, res: Response, next: NextFunction) => {
        try {
            const user = (req as any).user;

            if (!user) {
                return res.status(401).json({ error: 'Unauthorized: No user context' });
            }

            // Super admin bypasses all permission checks
            if (user.role === 'super_admin') {
                return next();
            }

            // Check if user has the permission via effective_user_permissions (respects overrides)
            const permissions = await getCachedPermissions(user.userId, user.role);

            // Legacy admin fallback (RBAC tables missing)
            if (permissions.has('__legacy_admin__')) {
                return next();
            }

            const perm = permissions.get(permissionCode);
            if (perm?.is_enabled === true) {
                return next();
            }

            // Fallback: check if user's role is in the allowed roles list
            if (fallbackRoles.includes(user.role)) {
                return next();
            }

            return res.status(403).json({
                error: 'Forbidden: Insufficient permissions',
                required: permissionCode
            });
        } catch (error: any) {
            logger.error('Permission/role check error', error);
            res.status(500).json({ error: 'Permission check failed' });
        }
    };
};

/**
 * Middleware: Check if user has a specific permission
 * Usage: requirePermission('feature:inbound:create')
 */
export const requirePermission = (...permissionCodes: string[]) => {
    return async (req: Request, res: Response, next: NextFunction) => {
        try {
            const user = (req as any).user;

            if (!user) {
                return res.status(401).json({ error: 'Unauthorized: No user context' });
            }

            // Super admin bypasses all permission checks
            if (user.role === 'super_admin') {
                return next();
            }

            const permissions = await getCachedPermissions(user.userId, user.role);

            // Legacy admin fallback - only admin gets through when RBAC tables missing
            if (permissions.has('__legacy_admin__')) {
                return next();
            }

            // Check if user has ANY of the required permissions
            const hasPermission = permissionCodes.some(code => {
                const perm = permissions.get(code);
                return perm?.is_enabled === true;
            });

            if (!hasPermission) {
                return res.status(403).json({
                    error: 'Forbidden: Insufficient permissions',
                    required: permissionCodes
                });
            }

            next();
        } catch (error: any) {
            logger.error('Permission check error', error);
            res.status(500).json({ error: 'Permission check failed' });
        }
    };
};

/**
 * Middleware: Check if user has ALL specified permissions
 * Usage: requireAllPermissions('feature:inbound:view', 'feature:inbound:edit')
 */
export const requireAllPermissions = (...permissionCodes: string[]) => {
    return async (req: Request, res: Response, next: NextFunction) => {
        try {
            const user = (req as any).user;

            if (!user) {
                return res.status(401).json({ error: 'Unauthorized: No user context' });
            }

            if (user.role === 'super_admin') {
                return next();
            }

            const permissions = await getCachedPermissions(user.userId, user.role);

            // Legacy admin fallback - only admin gets through when RBAC tables missing
            if (permissions.has('__legacy_admin__')) {
                return next();
            }

            const missingPermissions = permissionCodes.filter(code => {
                const perm = permissions.get(code);
                return !perm?.is_enabled;
            });

            if (missingPermissions.length > 0) {
                return res.status(403).json({
                    error: 'Forbidden: Missing required permissions',
                    missing: missingPermissions
                });
            }

            next();
        } catch (error: any) {
            logger.error('Permission check error', error);
            res.status(500).json({ error: 'Permission check failed' });
        }
    };
};

/**
 * Middleware: Ensure user can access the warehouse in the request
 * Checks warehouse_id from body, params, or query
 */
export const requireWarehouseAccess = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const user = (req as any).user;

        if (!user) {
            return res.status(401).json({ error: 'Unauthorized: No user context' });
        }

        // Super admin can access all warehouses
        if (user.role === 'super_admin') {
            return next();
        }

        // Admin can access all warehouses
        if (user.role === 'admin') {
            return next();
        }

        // Get warehouse_id from request (body, params, or query)
        // Use optional chaining on req.body since GET requests may not have a body
        const warehouseId = parseInt(
            req.user?.warehouseId ||
            req.body?.warehouse_id ||
            req.params.warehouse_id ||
            req.params.warehouseId ||
            req.query.warehouse_id as string ||
            req.query.warehouseId as string ||
            '0'
        );

        if (!warehouseId) {
            // No warehouse specified - will be filtered later
            return next();
        }

        // Check if user can access this warehouse
        const warehouses = await getUserWarehouses(user.userId, user.warehouseId);

        // No warehouses assigned = unrestricted access (all warehouses)
        if (warehouses.length === 0) {
            return next();
        }

        const canAccess = warehouses.some(w => w?.warehouse_id === warehouseId);

        if (!canAccess) {
            return res.status(403).json({
                error: 'Forbidden: You do not have access to this warehouse',
                warehouseId
            });
        }

        next();
    } catch (error: any) {
        logger.error('Warehouse access check error', error);
        res.status(500).json({ error: 'Warehouse access check failed' });
    }
};

/**
 * Middleware: Inject user's accessible warehouse IDs into request
 * Use this to filter queries to only show data from accessible warehouses
 */
export const injectWarehouseFilter = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const user = (req as any).user;

        if (!user) {
            return res.status(401).json({ error: 'Unauthorized: No user context for warehouse filter' });
        }

        // Super admin sees all warehouses
        if (user.role === 'super_admin') {
            (req as any).accessibleWarehouses = null; // null = all warehouses
            (req as any).warehouseFilter = ''; // Empty filter = no restriction
            return next();
        }

        // Admin users can see all warehouses (unless restricted in user_warehouses)
        const warehouses = await getUserWarehouses(user.userId, user.warehouseId);

        // If admin and no warehouse restrictions, allow all
        if (user.role === 'admin' && warehouses.length === 0) {
            (req as any).accessibleWarehouses = null;
            (req as any).warehouseFilter = '';
            return next();
        }

        // Check if user_warehouses table exists
        const warehouseTablesOk = await checkWarehouseTablesExist();

        // If warehouse tables don't exist, use legacy warehouseId from token
        if (!warehouseTablesOk) {
            if (user.warehouseId) {
                (req as any).accessibleWarehouses = [user.warehouseId];
                (req as any).defaultWarehouseId = user.warehouseId;
                (req as any).warehouseFilter = `warehouse_id = $WAREHOUSE_PARAM`;
                (req as any).warehouseFilterValues = [user.warehouseId];
            } else if (user.role === 'admin') {
                // Admin without warehouseId in token gets full access
                (req as any).accessibleWarehouses = null;
                (req as any).warehouseFilter = '';
            } else {
                // Non-admin without warehouse assignment: deny access
                return res.status(403).json({ error: 'Forbidden: No warehouse assigned to your account' });
            }
            return next();
        }
        const warehouseIds = warehouses.map((w: WarehouseAccess) => w.warehouse_id).filter(Boolean);

        // If no warehouses assigned: user has not been restricted — allow all access
        // This matches the design: empty user_warehouses = unrestricted (all warehouses)
        // Admins can later restrict by adding specific warehouse entries
        if (warehouseIds.length === 0) {
            (req as any).accessibleWarehouses = null;
            (req as any).warehouseFilter = '';
            return next();
        }

        (req as any).accessibleWarehouses = warehouseIds;
        (req as any).defaultWarehouseId = warehouses.find((w: WarehouseAccess) => w.is_default)?.warehouse_id || warehouseIds[0];

        // Generate SQL filter clause using parameterized values
        const placeholders = warehouseIds.map((_: number, i: number) => `$WAREHOUSE_PARAM_${i}`).join(',');
        (req as any).warehouseFilter = `warehouse_id IN (${warehouseIds.join(',')})`;
        (req as any).warehouseFilterValues = warehouseIds;

        next();
    } catch (error: any) {
        logger.error('Warehouse filter injection error', error);
        // Security: fail closed — do not silently continue without filter
        res.status(500).json({ error: 'Internal error: Unable to determine warehouse access' });
    }
};

/**
 * Helper: Check single permission for a user
 */
export async function checkPermission(userId: number, permissionCode: string, role: string = ''): Promise<boolean> {
    try {
        const permissions = await getCachedPermissions(userId, role);
        // Legacy admin fallback
        if (permissions.has('__legacy_admin__')) return true;
        const perm = permissions.get(permissionCode);
        return perm?.is_enabled === true;
    } catch {
        return false;
    }
}

/**
 * Helper: Check if permission is visible for a user
 */
export async function isPermissionVisible(userId: number, permissionCode: string, role: string = ''): Promise<boolean> {
    try {
        const permissions = await getCachedPermissions(userId, role);
        // Legacy admin fallback
        if (permissions.has('__legacy_admin__')) return true;
        const perm = permissions.get(permissionCode);
        return perm?.is_visible === true;
    } catch {
        return false;
    }
}

/**
 * Helper: Get all permissions for a user
 */
export async function getAllUserPermissions(userId: number, role: string = ''): Promise<PermissionResult[]> {
    const permissions = await getCachedPermissions(userId, role);
    return Array.from(permissions.entries()).map(([code, perm]) => ({
        permission_code: code,
        ...perm
    })) as any;
}

/**
 * Helper: Get all accessible warehouses for a user
 */
export async function getAccessibleWarehouses(userId: number): Promise<WarehouseAccess[]> {
    return getUserWarehouses(userId);
}

/**
 * Middleware: Check page access
 * Used for protecting entire route groups
 */
export const requirePageAccess = (pageName: string) => {
    return requirePermission(`page:${pageName}`);
};

/**
 * Middleware: Check feature access
 */
export const requireFeatureAccess = (resource: string, action: string) => {
    return requirePermission(`feature:${resource}:${action}`);
};

/**
 * Middleware: Check action access
 */
export const requireActionAccess = (actionCode: string) => {
    return requirePermission(`action:${actionCode}`);
};
