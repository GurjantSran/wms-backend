// File Path = warehouse-backend/src/controllers/ui-access.controller.ts
import { Request, Response } from 'express';
import { query, withTransaction } from '../config/database';
import { safeError } from '../utils/sanitizeError';

/**
 * Get all UI elements grouped by type
 */
export const getAllElements = async (req: Request, res: Response) => {
    try {
        const result = await query(`
            SELECT code, name, element_type, parent_menu, sort_order, is_active
            FROM ui_elements
            ORDER BY sort_order
        `);

        // Group by type
        const grouped = {
            menus: result.rows.filter(r => r.element_type === 'menu'),
            tabs: result.rows.filter(r => r.element_type === 'tab'),
            buttons: result.rows.filter(r => r.element_type === 'button'),
        };

        res.json(grouped);
    } catch (error: any) {
        console.error('Get UI elements error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Get current user's UI access
 */
export const getMyAccess = async (req: Request, res: Response) => {
    try {
        const user = (req as any).user;

        // Super admin gets everything
        if (user.role === 'super_admin' || user.role === 'admin') {
            const elements = await query(`
                SELECT code, name, element_type, parent_menu
                FROM ui_elements
                WHERE is_active = true
                ORDER BY sort_order
            `);

            const access: Record<string, boolean> = {};
            elements.rows.forEach(e => {
                access[e.code] = true;
            });

            return res.json({
                access,
                elements: elements.rows,
                role: user.role
            });
        }

        // Regular users - check role_ui_access and overrides
        const result = await query(`
            SELECT 
                e.code,
                e.name,
                e.element_type,
                e.parent_menu,
                COALESCE(uuo.is_visible, rua.is_visible, false) as is_visible
            FROM ui_elements e
            LEFT JOIN roles r ON r.name = $2
            LEFT JOIN role_ui_access rua ON rua.role_id = r.id AND rua.element_code = e.code
            LEFT JOIN user_ui_overrides uuo ON uuo.user_id = $1 AND uuo.element_code = e.code
            WHERE e.is_active = true
            ORDER BY e.sort_order
        `, [user.userId, user.role]);

        const access: Record<string, boolean> = {};
        result.rows.forEach(e => {
            access[e.code] = e.is_visible === true;
        });

        res.json({
            access,
            elements: result.rows,
            role: user.role
        });
    } catch (error: any) {
        console.error('Get my access error:', error);
        // Fallback - return all access for admin roles
        const user = (req as any).user;
        if (user.role === 'super_admin' || user.role === 'admin') {
            return res.json({ access: {}, elements: [], role: user.role, legacy: true });
        }
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Get all roles
 */
export const getRoles = async (req: Request, res: Response) => {
    try {
        const result = await query(`
            SELECT 
                r.id, r.name, r.description, r.is_system_role, r.is_active, r.priority,
                COUNT(DISTINCT rua.id) as element_count,
                COUNT(DISTINCT u.id) as user_count
            FROM roles r
            LEFT JOIN role_ui_access rua ON rua.role_id = r.id AND rua.is_visible = true
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

/**
 * Get UI access for a specific role
 */
export const getRoleAccess = async (req: Request, res: Response) => {
    try {
        const { roleId } = req.params;

        const result = await query(`
            SELECT 
                e.code,
                e.name,
                e.element_type,
                e.parent_menu,
                COALESCE(rua.is_visible, false) as is_visible
            FROM ui_elements e
            LEFT JOIN role_ui_access rua ON rua.role_id = $1 AND rua.element_code = e.code
            WHERE e.is_active = true
            ORDER BY e.element_type, e.sort_order
        `, [roleId]);

        res.json(result.rows);
    } catch (error: any) {
        console.error('Get role access error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Update role UI access
 */
export const updateRoleAccess = async (req: Request, res: Response) => {
    try {
        const { roleId } = req.params;
        const { elements } = req.body; // Array of { code, is_visible }

        if (!Array.isArray(elements)) {
            return res.status(400).json({ error: 'Elements array required' });
        }

        await withTransaction(async (client) => {
            // Batch upsert all elements in one query
            if (elements.length > 0) {
                const values: any[] = [];
                const placeholders: string[] = [];

                elements.forEach((el: any, idx: number) => {
                    const offset = idx * 3;
                    placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3})`);
                    values.push(roleId, el.code, el.is_visible);
                });

                await client.query(`
                    INSERT INTO role_ui_access (role_id, element_code, is_visible)
                    VALUES ${placeholders.join(', ')}
                    ON CONFLICT (role_id, element_code) 
                    DO UPDATE SET is_visible = EXCLUDED.is_visible, updated_at = NOW()
                `, values);
            }
        });

        res.json({ success: true, message: 'Role access updated' });
    } catch (error: any) {
        console.error('Update role access error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Get UI access overrides for a specific user
 */
export const getUserOverrides = async (req: Request, res: Response) => {
    try {
        const { userId } = req.params;

        // Get user's role first
        const userResult = await query('SELECT role FROM users WHERE id = $1', [userId]);
        if (userResult.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }
        const userRole = userResult.rows[0].role;

        // Get role ID
        const roleResult = await query('SELECT id FROM roles WHERE name = $1', [userRole]);
        const roleId = roleResult.rows[0]?.id;

        const result = await query(`
            SELECT 
                e.code,
                e.name,
                e.element_type,
                e.parent_menu,
                COALESCE(rua.is_visible, false) as role_default,
                uuo.is_visible as user_override,
                COALESCE(uuo.is_visible, rua.is_visible, false) as effective
            FROM ui_elements e
            LEFT JOIN role_ui_access rua ON rua.role_id = $2 AND rua.element_code = e.code
            LEFT JOIN user_ui_overrides uuo ON uuo.user_id = $1 AND uuo.element_code = e.code
            WHERE e.is_active = true
            ORDER BY e.element_type, e.sort_order
        `, [userId, roleId]);

        res.json({
            userRole,
            elements: result.rows
        });
    } catch (error: any) {
        console.error('Get user overrides error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Update user UI overrides
 */
export const updateUserOverrides = async (req: Request, res: Response) => {
    try {
        const { userId } = req.params;
        const { overrides } = req.body; // Array of { code, is_visible } - only elements that differ from role

        if (!Array.isArray(overrides)) {
            return res.status(400).json({ error: 'Overrides array required' });
        }

        await withTransaction(async (client) => {
            // Clear existing overrides
            await client.query('DELETE FROM user_ui_overrides WHERE user_id = $1', [userId]);

            // Batch insert new overrides
            const validOverrides = overrides.filter((o: any) => o.is_visible !== null && o.is_visible !== undefined);
            if (validOverrides.length > 0) {
                const values: any[] = [];
                const placeholders: string[] = [];

                validOverrides.forEach((override: any, idx: number) => {
                    const offset = idx * 3;
                    placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3})`);
                    values.push(userId, override.code, override.is_visible);
                });

                await client.query(`
                    INSERT INTO user_ui_overrides (user_id, element_code, is_visible)
                    VALUES ${placeholders.join(', ')}
                `, values);
            }
        });

        res.json({ success: true, message: 'User overrides updated' });
    } catch (error: any) {
        console.error('Update user overrides error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Get user warehouses
 */
export const getUserWarehouses = async (req: Request, res: Response) => {
    try {
        const { userId } = req.params;

        const result = await query(`
            SELECT 
                w.id as warehouse_id,
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

/**
 * Update user warehouses
 */
export const updateUserWarehouses = async (req: Request, res: Response) => {
    try {
        const { userId } = req.params;
        const { warehouse_ids, default_warehouse_id } = req.body;

        if (!Array.isArray(warehouse_ids)) {
            return res.status(400).json({ error: 'warehouse_ids array required' });
        }

        await withTransaction(async (client) => {
            // Clear existing
            await client.query('DELETE FROM user_warehouses WHERE user_id = $1', [userId]);

            // Batch insert new
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

        res.json({ success: true, message: 'User warehouses updated' });
    } catch (error: any) {
        console.error('Update user warehouses error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Create a new role with default view-only permissions
 * New roles get: all permissions visible, only menu permissions enabled
 */
export const createRole = async (req: Request, res: Response) => {
    try {
        const { name, description } = req.body;

        if (!name) {
            return res.status(400).json({ error: 'Role name required' });
        }

        const newRole = await withTransaction(async (client) => {
            // Create the role
            const result = await client.query(`
                INSERT INTO roles (name, description, is_system_role, is_active, priority)
                VALUES ($1, $2, false, true, 20)
                RETURNING *
            `, [name.toLowerCase().replace(/\s+/g, '_'), description || '']);

            const newRoleId = result.rows[0].id;

            // Set default view-only permissions for the new role
            // All permissions are visible, but only menu permissions are enabled
            await client.query(`
                INSERT INTO role_permissions (role_id, permission_code, is_enabled, is_visible)
                SELECT $1, code, 
                       CASE WHEN code LIKE 'menu:%' THEN true ELSE false END as is_enabled,
                       true as is_visible
                FROM permissions
                ON CONFLICT (role_id, permission_code) DO NOTHING
            `, [newRoleId]);

            return result.rows[0];
        });

        res.json(newRole);
    } catch (error: any) {
        console.error('Create role error:', error);
        if (error.code === '23505') {
            return res.status(400).json({ error: 'Role already exists' });
        }
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Delete a role
 */
export const deleteRole = async (req: Request, res: Response) => {
    try {
        const { roleId } = req.params;

        // Check if system role
        const check = await query('SELECT is_system_role FROM roles WHERE id = $1', [roleId]);
        if (check.rows[0]?.is_system_role) {
            return res.status(400).json({ error: 'Cannot delete system role' });
        }

        await query('DELETE FROM roles WHERE id = $1', [roleId]);

        res.json({ success: true, message: 'Role deleted' });
    } catch (error: any) {
        console.error('Delete role error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};
