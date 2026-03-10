// File Path = warehouse-backend/src/controllers/permissions.controller.ts
// Final Permissions Controller - Enable/Disable + Show/Hide

import { Request, Response } from 'express';
import { query, withTransaction } from '../config/database';
import { safeError } from '../utils/sanitizeError';
import { logAudit, getClientIp } from '../utils/auditLogger';
import { clearPermissionCache } from '../middleware/rbac.middleware';

// =============================================================
// GET ALL PERMISSIONS (grouped by page)
// =============================================================
export const getAllPermissions = async (req: Request, res: Response) => {
    try {
        const result = await query(`
            SELECT code, name, category, page, parent_code, sort_order
            FROM permissions
            ORDER BY sort_order
        `);

        // Group by page
        const grouped: Record<string, any[]> = {};
        result.rows.forEach(row => {
            if (!grouped[row.page]) {
                grouped[row.page] = [];
            }
            grouped[row.page].push(row);
        });

        res.json({
            permissions: result.rows,
            grouped
        });
    } catch (error: any) {
        console.error('Get all permissions error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// GET MY PERMISSIONS (for current logged-in user)
// =============================================================
export const getMyPermissions = async (req: Request, res: Response) => {
    try {
        const user = (req as any).user;

        // ONLY Super admin gets everything enabled and visible automatically
        // Admin role should respect their role-based permission settings
        if (user.role === 'super_admin') {
            const perms = await query(`
                SELECT code, name, category, page
                FROM permissions
                ORDER BY sort_order
            `);

            const permissions: Record<string, any> = {};
            perms.rows.forEach(p => {
                permissions[p.code] = {
                    name: p.name,
                    category: p.category,
                    page: p.page,
                    is_enabled: true,
                    is_visible: true,
                    source: 'super_admin'
                };
            });

            return res.json({
                permissions,
                role: user.role,
                isAdmin: true  // Only super_admin is true admin with full bypass
            });
        }

        // Regular users - get from effective_user_permissions view
        const result = await query(`
            SELECT 
                permission_code as code,
                permission_name as name,
                category,
                page,
                is_enabled,
                is_visible,
                permission_source as source
            FROM effective_user_permissions
            WHERE user_id = $1
            ORDER BY sort_order
        `, [user.userId]);

        const permissions: Record<string, any> = {};
        result.rows.forEach(p => {
            permissions[p.code] = {
                name: p.name,
                category: p.category,
                page: p.page,
                is_enabled: p.is_enabled,
                is_visible: p.is_visible,
                source: p.source
            };
        });

        res.json({
            permissions,
            role: user.role,
            isAdmin: false
        });
    } catch (error: any) {
        console.error('Get my permissions error:', error);
        // Fallback for super_admin only
        const user = (req as any).user;
        if (user.role === 'super_admin') {
            return res.json({ permissions: {}, role: user.role, isAdmin: true, legacy: true });
        }
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// GET ALL ROLES
// =============================================================
export const getRoles = async (req: Request, res: Response) => {
    try {
        const result = await query(`
            SELECT 
                r.id, r.name, r.description, r.is_system_role, r.is_active, r.priority,
                COUNT(DISTINCT u.id) as user_count
            FROM roles r
            LEFT JOIN users u ON u.role = r.name AND u.is_active = true
            GROUP BY r.id
            ORDER BY r.priority DESC
        `);

        res.json(result.rows);
    } catch (error: any) {
        console.error('Get roles error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// GET ROLE PERMISSIONS
// =============================================================
export const getRolePermissions = async (req: Request, res: Response) => {
    try {
        const { roleId } = req.params;

        const result = await query(`
            SELECT 
                p.code,
                p.name,
                p.category,
                p.page,
                p.parent_code,
                p.sort_order,
                COALESCE(rp.is_enabled, false) as is_enabled,
                COALESCE(rp.is_visible, false) as is_visible
            FROM permissions p
            LEFT JOIN role_permissions rp ON rp.permission_code = p.code AND rp.role_id = $1
            ORDER BY p.sort_order
        `, [roleId]);

        res.json(result.rows);
    } catch (error: any) {
        console.error('Get role permissions error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// UPDATE ROLE PERMISSIONS
// =============================================================
export const updateRolePermissions = async (req: Request, res: Response) => {
    try {
        const { roleId } = req.params;
        const { permissions } = req.body; // Array of { code, is_enabled, is_visible }

        console.log(`[updateRolePermissions] roleId: ${roleId}, permissions count: ${Array.isArray(permissions) ? permissions.length : 'not array'}`);
        if (Array.isArray(permissions) && permissions.length > 0) {
            console.log('[updateRolePermissions] First 3 permission codes:', permissions.slice(0, 3).map((p: any) => p.code));
        }

        if (!Array.isArray(permissions)) {
            return res.status(400).json({ error: 'Permissions array required' });
        }

        // Build batch upsert query for better performance
        // Instead of 76 individual queries, do one batch query
        const values: any[] = [];
        const placeholders: string[] = [];

        permissions.forEach((perm, idx) => {
            const offset = idx * 4;
            placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4})`);
            values.push(roleId, perm.code, perm.is_enabled, perm.is_visible);
        });

        if (placeholders.length === 0) {
            return res.json({ success: true, message: 'No permissions to update' });
        }

        await withTransaction(async (client) => {
            // Batch upsert all permissions in one query
            await client.query(`
                INSERT INTO role_permissions (role_id, permission_code, is_enabled, is_visible)
                VALUES ${placeholders.join(', ')}
                ON CONFLICT (role_id, permission_code)
                DO UPDATE SET 
                    is_enabled = EXCLUDED.is_enabled, 
                    is_visible = EXCLUDED.is_visible,
                    updated_at = NOW()
            `, values);
        });

        // Clear permission cache for all users since role change affects everyone with this role
        clearPermissionCache();

        console.log(`[updateRolePermissions] SUCCESS - roleId: ${roleId}`);
        logAudit({ action: 'PERMISSION_CHANGE', performedBy: (req as any).user?.username || 'unknown', details: `Updated permissions for role ${roleId}`, ipAddress: getClientIp(req) });
        res.json({ success: true, message: 'Role permissions updated' });
    } catch (error: any) {
        console.error('[updateRolePermissions] ERROR:', error.message);
        console.error('[updateRolePermissions] Full error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// GET USER OVERRIDES
// =============================================================
export const getUserOverrides = async (req: Request, res: Response) => {
    try {
        const { userId } = req.params;

        // Get user's role first
        const userResult = await query('SELECT role FROM users WHERE id = $1', [userId]);
        if (userResult.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }
        const userRole = userResult.rows[0].role;

        // Get role's default permissions with user overrides
        const result = await query(`
            SELECT 
                p.code,
                p.name,
                p.category,
                p.page,
                p.parent_code,
                p.sort_order,
                COALESCE(rp.is_enabled, false) as role_enabled,
                COALESCE(rp.is_visible, false) as role_visible,
                upo.is_enabled as override_enabled,
                upo.is_visible as override_visible,
                COALESCE(upo.is_enabled, rp.is_enabled, false) as effective_enabled,
                COALESCE(upo.is_visible, rp.is_visible, false) as effective_visible
            FROM permissions p
            LEFT JOIN roles r ON r.name = $2
            LEFT JOIN role_permissions rp ON rp.permission_code = p.code AND rp.role_id = r.id
            LEFT JOIN user_permission_overrides upo ON upo.permission_code = p.code AND upo.user_id = $1
            ORDER BY p.sort_order
        `, [userId, userRole]);

        res.json({
            userRole,
            permissions: result.rows
        });
    } catch (error: any) {
        console.error('Get user overrides error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// UPDATE USER OVERRIDES (super_admin only - admin must use approval)
// =============================================================
export const updateUserOverrides = async (req: Request, res: Response) => {
    try {
        const user = (req as any).user;
        const { userId } = req.params;
        const { overrides } = req.body; // Array of { code, is_enabled, is_visible } - null values mean use role default

        // Only super_admin can directly update overrides; admin must use the approval workflow
        if (user.role !== 'super_admin') {
            return res.status(403).json({ error: 'Only super admin can directly update user overrides. Please use the approval workflow.' });
        }

        if (!Array.isArray(overrides)) {
            return res.status(400).json({ error: 'Overrides array required' });
        }

        await withTransaction(async (client) => {
            // First, delete all existing overrides for this user
            await client.query('DELETE FROM user_permission_overrides WHERE user_id = $1', [userId]);

            // Filter and batch insert overrides (only where is_enabled or is_visible is not null)
            const validOverrides = overrides.filter(ovr => ovr.is_enabled !== null || ovr.is_visible !== null);

            if (validOverrides.length > 0) {
                const values: any[] = [];
                const placeholders: string[] = [];

                validOverrides.forEach((ovr, idx) => {
                    const offset = idx * 4;
                    placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4})`);
                    values.push(userId, ovr.code, ovr.is_enabled, ovr.is_visible);
                });

                await client.query(`
                    INSERT INTO user_permission_overrides (user_id, permission_code, is_enabled, is_visible)
                    VALUES ${placeholders.join(', ')}
                `, values);
            }
        });

        // Clear permission cache for the specific user whose overrides changed
        clearPermissionCache(parseInt(userId as string));

        logAudit({ action: 'USER_OVERRIDE_CHANGE', performedBy: user?.username || 'unknown', details: `Updated overrides for user ${userId} (${overrides.length} permissions)`, ipAddress: getClientIp(req) });

        res.json({ success: true, message: 'User overrides updated' });
    } catch (error: any) {
        console.error('Update user overrides error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// GET USER WAREHOUSES
// =============================================================
export const getUserWarehouses = async (req: Request, res: Response) => {
    try {
        const { userId } = req.params;

        const result = await query(`
            SELECT 
                uw.warehouse_id,
                w.name as warehouse_name,
                w.code as warehouse_code,
                uw.is_default
            FROM user_warehouses uw
            JOIN warehouses w ON w.id = uw.warehouse_id
            WHERE uw.user_id = $1 AND w.is_active = true
            ORDER BY uw.is_default DESC, w.name
        `, [userId]);

        res.json(result.rows);
    } catch (error: any) {
        console.error('Get user warehouses error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// UPDATE USER WAREHOUSES (super_admin only)
// =============================================================
export const updateUserWarehouses = async (req: Request, res: Response) => {
    try {
        const user = (req as any).user;
        const { userId } = req.params;
        const { warehouse_ids, default_warehouse_id } = req.body;

        // Only super_admin can directly update warehouse access
        if (user.role !== 'super_admin') {
            return res.status(403).json({ error: 'Only super admin can update warehouse access. Please contact your super admin.' });
        }

        if (!Array.isArray(warehouse_ids)) {
            return res.status(400).json({ error: 'warehouse_ids array required' });
        }

        await withTransaction(async (client) => {
            // Delete existing warehouse assignments
            await client.query('DELETE FROM user_warehouses WHERE user_id = $1', [userId]);

            // Batch insert new assignments
            if (warehouse_ids.length > 0) {
                const values: any[] = [];
                const placeholders: string[] = [];

                warehouse_ids.forEach((whId: any, idx: number) => {
                    const offset = idx * 3;
                    placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3})`);
                    values.push(userId, whId, whId === default_warehouse_id);
                });

                await client.query(`
                    INSERT INTO user_warehouses (user_id, warehouse_id, is_default)
                    VALUES ${placeholders.join(', ')}
                `, values);
            }
        });

        logAudit({ action: 'WAREHOUSE_ACCESS_CHANGE', performedBy: (req as any).user?.username || 'unknown', details: `Updated warehouse access for user ${userId} (${warehouse_ids.length} warehouses)`, ipAddress: getClientIp(req) });

        res.json({ success: true, message: 'User warehouses updated' });
    } catch (error: any) {
        console.error('Update user warehouses error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// CHECK PERMISSION (API endpoint for frontend)
// =============================================================
export const checkPermission = async (req: Request, res: Response) => {
    try {
        const user = (req as any).user;
        const { code } = req.params;

        // Only super_admin has automatic full access (admin should respect role permissions)
        if (user.role === 'super_admin') {
            return res.json({
                code,
                is_enabled: true,
                is_visible: true
            });
        }

        const result = await query(`
            SELECT is_enabled, is_visible
            FROM effective_user_permissions
            WHERE user_id = $1 AND permission_code = $2
        `, [user.userId, code]);

        if (result.rows.length === 0) {
            return res.json({
                code,
                is_enabled: false,
                is_visible: false
            });
        }

        res.json({
            code,
            is_enabled: result.rows[0].is_enabled,
            is_visible: result.rows[0].is_visible
        });
    } catch (error: any) {
        console.error('Check permission error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};
