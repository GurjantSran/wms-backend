// File Path = warehouse-backend/src/controllers/permissionApproval.controller.ts
// Permission Approval Controller - Handles approval workflow for permission changes

import { Request, Response } from 'express';
import { query, withTransaction } from '../config/database';
import { safeError } from '../utils/sanitizeError';

// =============================================================
// GET PENDING APPROVAL COUNT (for badge)
// =============================================================
export const getPendingCount = async (req: Request, res: Response) => {
    try {
        const result = await query(`
            SELECT COUNT(*) as count
            FROM permission_change_requests
            WHERE status = 'pending'
        `);

        res.json({ count: parseInt(result.rows[0].count) || 0 });
    } catch (error: any) {
        console.error('Get pending count error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// GET ALL APPROVAL REQUESTS (for super_admin)
// =============================================================
export const getApprovalRequests = async (req: Request, res: Response) => {
    try {
        const user = (req as any).user;
        const { status = 'all' } = req.query;

        // super_admin and admin can view all requests
        if (user.role !== 'super_admin' && user.role !== 'admin') {
            return res.status(403).json({ error: 'Only admin and super admin can view approval requests' });
        }

        let statusFilter = '';
        const params: any[] = [];

        if (status !== 'all') {
            statusFilter = 'WHERE pcr.status = $1';
            params.push(status);
        }

        const result = await query(`
            SELECT 
                pcr.id,
                pcr.request_type,
                pcr.role_id,
                pcr.target_user_id,
                pcr.requested_by,
                pcr.status,
                pcr.reviewer_id,
                pcr.reviewed_at,
                pcr.review_note,
                pcr.created_at,
                pcr.updated_at,
                r.name as role_name,
                tu.username as target_username,
                tu.full_name as target_full_name,
                ru.username as requester_username,
                ru.full_name as requester_full_name,
                rev.username as reviewer_username,
                rev.full_name as reviewer_full_name,
                (SELECT COUNT(*) FROM permission_change_details WHERE request_id = pcr.id) as total_changes,
                (SELECT COUNT(*) FROM permission_change_details WHERE request_id = pcr.id AND is_approved = true) as approved_changes,
                (SELECT COUNT(*) FROM permission_change_details WHERE request_id = pcr.id AND is_approved = false) as rejected_changes,
                (SELECT COUNT(*) FROM permission_change_details WHERE request_id = pcr.id AND is_approved IS NULL) as pending_changes
            FROM permission_change_requests pcr
            LEFT JOIN roles r ON r.id = pcr.role_id
            LEFT JOIN users tu ON tu.id = pcr.target_user_id
            LEFT JOIN users ru ON ru.id = pcr.requested_by
            LEFT JOIN users rev ON rev.id = pcr.reviewer_id
            ${statusFilter}
            ORDER BY 
                CASE WHEN pcr.status = 'pending' THEN 0 ELSE 1 END,
                pcr.created_at DESC
        `, params);

        res.json(result.rows);
    } catch (error: any) {
        console.error('Get approval requests error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// GET SINGLE APPROVAL REQUEST WITH DETAILS
// =============================================================
export const getApprovalRequestDetails = async (req: Request, res: Response) => {
    try {
        const user = (req as any).user;
        const { id } = req.params;

        // Get request info
        const requestResult = await query(`
            SELECT 
                pcr.*,
                r.name as role_name,
                tu.username as target_username,
                tu.full_name as target_full_name,
                ru.username as requester_username,
                ru.full_name as requester_full_name,
                rev.username as reviewer_username,
                rev.full_name as reviewer_full_name
            FROM permission_change_requests pcr
            LEFT JOIN roles r ON r.id = pcr.role_id
            LEFT JOIN users tu ON tu.id = pcr.target_user_id
            LEFT JOIN users ru ON ru.id = pcr.requested_by
            LEFT JOIN users rev ON rev.id = pcr.reviewer_id
            WHERE pcr.id = $1
        `, [id]);

        if (requestResult.rows.length === 0) {
            return res.status(404).json({ error: 'Request not found' });
        }

        const request = requestResult.rows[0];

        // Check permissions - super_admin can see all, others can only see their own
        if (user.role !== 'super_admin' && request.requested_by !== user.userId) {
            return res.status(403).json({ error: 'Access denied' });
        }

        // Get change details
        const detailsResult = await query(`
            SELECT 
                pcd.*,
                p.name as permission_name,
                p.category,
                p.page
            FROM permission_change_details pcd
            JOIN permissions p ON p.code = pcd.permission_code
            WHERE pcd.request_id = $1
            ORDER BY p.sort_order
        `, [id]);

        res.json({
            request,
            details: detailsResult.rows
        });
    } catch (error: any) {
        console.error('Get approval request details error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// GET MY REQUESTS (for the requester to see their own requests)
// =============================================================
export const getMyRequests = async (req: Request, res: Response) => {
    try {
        const user = (req as any).user;

        const result = await query(`
            SELECT 
                pcr.id,
                pcr.request_type,
                pcr.role_id,
                pcr.target_user_id,
                pcr.status,
                pcr.review_note,
                pcr.created_at,
                pcr.reviewed_at,
                r.name as role_name,
                tu.username as target_username,
                tu.full_name as target_full_name,
                rev.username as reviewer_username,
                (SELECT COUNT(*) FROM permission_change_details WHERE request_id = pcr.id) as total_changes
            FROM permission_change_requests pcr
            LEFT JOIN roles r ON r.id = pcr.role_id
            LEFT JOIN users tu ON tu.id = pcr.target_user_id
            LEFT JOIN users rev ON rev.id = pcr.reviewer_id
            WHERE pcr.requested_by = $1
            ORDER BY pcr.created_at DESC
            LIMIT 50
        `, [user.userId]);

        res.json(result.rows);
    } catch (error: any) {
        console.error('Get my requests error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// CREATE APPROVAL REQUEST (Role Permissions)
// =============================================================
export const createRolePermissionRequest = async (req: Request, res: Response) => {
    try {
        const user = (req as any).user;
        const { roleId, permissions } = req.body;

        // Validate input
        if (!roleId || !Array.isArray(permissions) || permissions.length === 0) {
            return res.status(400).json({ error: 'roleId and permissions array required' });
        }

        // Super admin can directly update (no approval needed)
        if (user.role === 'super_admin') {
            return res.status(400).json({
                error: 'Super admin should use direct update, not approval request',
                hint: 'Use PUT /api/permissions/roles/:roleId/permissions instead'
            });
        }

        // Get current role permissions for comparison
        const currentPerms = await query(`
            SELECT 
                p.code,
                COALESCE(rp.is_enabled, false) as is_enabled,
                COALESCE(rp.is_visible, false) as is_visible
            FROM permissions p
            LEFT JOIN role_permissions rp ON rp.permission_code = p.code AND rp.role_id = $1
        `, [roleId]);

        const currentPermsMap = new Map(
            currentPerms.rows.map(p => [p.code, { is_enabled: p.is_enabled, is_visible: p.is_visible }])
        );

        // Filter only changed permissions
        const changedPermissions = permissions.filter(perm => {
            const current = currentPermsMap.get(perm.code);
            if (!current) return true;
            return current.is_enabled !== perm.is_enabled || current.is_visible !== perm.is_visible;
        });

        if (changedPermissions.length === 0) {
            return res.status(400).json({ error: 'No changes detected' });
        }

        const requestId = await withTransaction(async (client) => {
            // Create request
            const requestResult = await client.query(`
                INSERT INTO permission_change_requests (request_type, role_id, requested_by, status)
                VALUES ('role', $1, $2, 'pending')
                RETURNING id
            `, [roleId, user.userId]);

            const reqId = requestResult.rows[0].id;

            // Insert change details
            const values: any[] = [];
            const placeholders: string[] = [];

            changedPermissions.forEach((perm, idx) => {
                const current = currentPermsMap.get(perm.code) || { is_enabled: false, is_visible: false };
                const offset = idx * 6;
                placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6})`);
                values.push(
                    reqId,
                    perm.code,
                    current.is_enabled,
                    perm.is_enabled,
                    current.is_visible,
                    perm.is_visible
                );
            });

            await client.query(`
                INSERT INTO permission_change_details 
                (request_id, permission_code, old_is_enabled, new_is_enabled, old_is_visible, new_is_visible)
                VALUES ${placeholders.join(', ')}
            `, values);

            return reqId;
        });

        res.status(201).json({
            success: true,
            requestId,
            message: `Permission change request created with ${changedPermissions.length} changes. Awaiting super admin approval.`
        });

    } catch (error: any) {
        console.error('Create role permission request error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// CREATE APPROVAL REQUEST (User Overrides)
// =============================================================
export const createUserOverrideRequest = async (req: Request, res: Response) => {
    try {
        const user = (req as any).user;
        const { targetUserId, overrides } = req.body;

        // Validate input
        if (!targetUserId || !Array.isArray(overrides)) {
            return res.status(400).json({ error: 'targetUserId and overrides array required' });
        }

        // Super admin can directly update
        if (user.role === 'super_admin') {
            return res.status(400).json({
                error: 'Super admin should use direct update, not approval request'
            });
        }

        // Get current user overrides for comparison
        const currentOverrides = await query(`
            SELECT permission_code, is_enabled, is_visible
            FROM user_permission_overrides
            WHERE user_id = $1
        `, [targetUserId]);

        const currentMap = new Map(
            currentOverrides.rows.map(o => [o.permission_code, { is_enabled: o.is_enabled, is_visible: o.is_visible }])
        );

        // Filter changed overrides
        const changedOverrides = overrides.filter(ovr => {
            const current = currentMap.get(ovr.code);
            if (!current && (ovr.is_enabled !== null || ovr.is_visible !== null)) return true;
            if (!current) return false;
            return current.is_enabled !== ovr.is_enabled || current.is_visible !== ovr.is_visible;
        });

        if (changedOverrides.length === 0) {
            return res.status(400).json({ error: 'No changes detected' });
        }

        const requestId = await withTransaction(async (client) => {
            // Create request
            const requestResult = await client.query(`
                INSERT INTO permission_change_requests (request_type, target_user_id, requested_by, status)
                VALUES ('user_override', $1, $2, 'pending')
                RETURNING id
            `, [targetUserId, user.userId]);

            const reqId = requestResult.rows[0].id;

            // Insert change details
            const values: any[] = [];
            const placeholders: string[] = [];

            changedOverrides.forEach((ovr, idx) => {
                const current = currentMap.get(ovr.code) || { is_enabled: null, is_visible: null };
                const offset = idx * 6;
                placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6})`);
                values.push(
                    reqId,
                    ovr.code,
                    current.is_enabled,
                    ovr.is_enabled,
                    current.is_visible,
                    ovr.is_visible
                );
            });

            await client.query(`
                INSERT INTO permission_change_details 
                (request_id, permission_code, old_is_enabled, new_is_enabled, old_is_visible, new_is_visible)
                VALUES ${placeholders.join(', ')}
            `, values);

            return reqId;
        });

        res.status(201).json({
            success: true,
            requestId,
            message: `User override request created with ${changedOverrides.length} changes. Awaiting super admin approval.`
        });

    } catch (error: any) {
        console.error('Create user override request error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// APPROVE/REJECT INDIVIDUAL CHANGES (and auto-apply)
// =============================================================
export const updateChangeApproval = async (req: Request, res: Response) => {
    try {
        const user = (req as any).user;
        const { id } = req.params; // request id
        const { changes } = req.body; // Array of { detailId, is_approved }

        // Only super_admin can approve/reject
        if (user.role !== 'super_admin') {
            return res.status(403).json({ error: 'Only super admin can approve/reject changes' });
        }

        if (!Array.isArray(changes) || changes.length === 0) {
            return res.status(400).json({ error: 'changes array required' });
        }

        // Verify request exists and is pending
        const requestResult = await query(`
            SELECT * FROM permission_change_requests WHERE id = $1
        `, [id]);

        if (requestResult.rows.length === 0) {
            return res.status(404).json({ error: 'Request not found' });
        }

        const request = requestResult.rows[0];

        if (request.status !== 'pending') {
            return res.status(400).json({ error: 'Request is not pending' });
        }

        const txResult = await withTransaction(async (client) => {
            // Update each change's approval status
            for (const change of changes) {
                await client.query(`
                    UPDATE permission_change_details 
                    SET is_approved = $1
                    WHERE id = $2 AND request_id = $3
                `, [change.is_approved, change.detailId, id]);
            }

            // AUTO-APPLY: Immediately apply approved changes to role_permissions/user_permission_overrides
            const approvedChanges = await client.query(`
                SELECT * FROM permission_change_details
                WHERE request_id = $1 AND is_approved = true
            `, [id]);

            if (approvedChanges.rows.length > 0) {
                if (request.request_type === 'role') {
                    // Apply to role_permissions
                    for (const change of approvedChanges.rows) {
                        await client.query(`
                            INSERT INTO role_permissions (role_id, permission_code, is_enabled, is_visible)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (role_id, permission_code)
                            DO UPDATE SET 
                                is_enabled = $3,
                                is_visible = $4,
                                updated_at = NOW()
                        `, [request.role_id, change.permission_code, change.new_is_enabled, change.new_is_visible]);
                    }
                } else if (request.request_type === 'user_override') {
                    // Apply to user_permission_overrides
                    for (const change of approvedChanges.rows) {
                        if (change.new_is_enabled === null && change.new_is_visible === null) {
                            await client.query(`
                                DELETE FROM user_permission_overrides
                                WHERE user_id = $1 AND permission_code = $2
                            `, [request.target_user_id, change.permission_code]);
                        } else {
                            await client.query(`
                                INSERT INTO user_permission_overrides (user_id, permission_code, is_enabled, is_visible)
                                VALUES ($1, $2, $3, $4)
                                ON CONFLICT (user_id, permission_code)
                                DO UPDATE SET 
                                    is_enabled = $3,
                                    is_visible = $4,
                                    updated_at = NOW()
                            `, [request.target_user_id, change.permission_code, change.new_is_enabled, change.new_is_visible]);
                        }
                    }
                }
            }

            // Check if all changes have been reviewed (no pending ones left)
            const pendingCheck = await client.query(`
                SELECT COUNT(*) as pending_count FROM permission_change_details
                WHERE request_id = $1 AND is_approved IS NULL
            `, [id]);

            const pendingCount = parseInt(pendingCheck.rows[0].pending_count);

            // If no pending changes left, auto-finalize the request
            if (pendingCount === 0) {
                const finalCounts = await client.query(`
                    SELECT 
                        COUNT(*) FILTER (WHERE is_approved = true) as approved,
                        COUNT(*) FILTER (WHERE is_approved = false) as rejected,
                        COUNT(*) as total
                    FROM permission_change_details
                    WHERE request_id = $1
                `, [id]);

                const counts = finalCounts.rows[0];
                let finalStatus = 'approved';

                if (parseInt(counts.approved) === 0) {
                    finalStatus = 'rejected';
                } else if (parseInt(counts.rejected) > 0) {
                    finalStatus = 'partially_approved';
                }

                await client.query(`
                    UPDATE permission_change_requests
                    SET 
                        status = $1,
                        reviewer_id = $2,
                        reviewed_at = NOW()
                    WHERE id = $3
                `, [finalStatus, user.userId, id]);
            }

            return { appliedCount: approvedChanges.rows.length, autoFinalized: pendingCount === 0 };
        });

        res.json({
            success: true,
            message: 'Changes approved and applied immediately',
            appliedCount: txResult.appliedCount,
            autoFinalized: txResult.autoFinalized
        });

    } catch (error: any) {
        console.error('Update change approval error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// FINALIZE REQUEST (Apply approved changes)
// =============================================================
export const finalizeRequest = async (req: Request, res: Response) => {
    try {
        const user = (req as any).user;
        const { id } = req.params;
        const { action, note, approveAll } = req.body; // action: 'approve' | 'reject' | 'partial'

        // Only super_admin can finalize
        if (user.role !== 'super_admin') {
            return res.status(403).json({ error: 'Only super admin can finalize requests' });
        }

        if (!['approve', 'reject', 'partial'].includes(action)) {
            return res.status(400).json({ error: 'Invalid action. Use: approve, reject, or partial' });
        }

        // Get request
        const requestResult = await query(`
            SELECT * FROM permission_change_requests WHERE id = $1
        `, [id]);

        if (requestResult.rows.length === 0) {
            return res.status(404).json({ error: 'Request not found' });
        }

        const request = requestResult.rows[0];

        if (request.status !== 'pending') {
            return res.status(400).json({ error: 'Request is not pending' });
        }

        const txResult = await withTransaction(async (client) => {
            // If approveAll is true, approve all pending changes
            if (approveAll && action === 'approve') {
                await client.query(`
                    UPDATE permission_change_details 
                    SET is_approved = true
                    WHERE request_id = $1 AND is_approved IS NULL
                `, [id]);
            }

            // If reject all
            if (action === 'reject') {
                await client.query(`
                    UPDATE permission_change_details 
                    SET is_approved = false
                    WHERE request_id = $1 AND is_approved IS NULL
                `, [id]);
            }

            // Get approved changes to apply
            const approvedChanges = await client.query(`
                SELECT * FROM permission_change_details
                WHERE request_id = $1 AND is_approved = true
            `, [id]);

            // Apply approved changes based on request type
            if (approvedChanges.rows.length > 0) {
                if (request.request_type === 'role') {
                    // Apply to role_permissions
                    for (const change of approvedChanges.rows) {
                        await client.query(`
                            INSERT INTO role_permissions (role_id, permission_code, is_enabled, is_visible)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (role_id, permission_code)
                            DO UPDATE SET 
                                is_enabled = $3,
                                is_visible = $4,
                                updated_at = NOW()
                        `, [request.role_id, change.permission_code, change.new_is_enabled, change.new_is_visible]);
                    }
                } else if (request.request_type === 'user_override') {
                    // Apply to user_permission_overrides
                    for (const change of approvedChanges.rows) {
                        if (change.new_is_enabled === null && change.new_is_visible === null) {
                            // Remove override
                            await client.query(`
                                DELETE FROM user_permission_overrides
                                WHERE user_id = $1 AND permission_code = $2
                            `, [request.target_user_id, change.permission_code]);
                        } else {
                            await client.query(`
                                INSERT INTO user_permission_overrides (user_id, permission_code, is_enabled, is_visible)
                                VALUES ($1, $2, $3, $4)
                                ON CONFLICT (user_id, permission_code)
                                DO UPDATE SET 
                                    is_enabled = $3,
                                    is_visible = $4,
                                    updated_at = NOW()
                            `, [request.target_user_id, change.permission_code, change.new_is_enabled, change.new_is_visible]);
                        }
                    }
                }
            }

            // Determine final status
            const allChanges = await client.query(`
                SELECT 
                    COUNT(*) FILTER (WHERE is_approved = true) as approved,
                    COUNT(*) FILTER (WHERE is_approved = false) as rejected,
                    COUNT(*) as total
                FROM permission_change_details
                WHERE request_id = $1
            `, [id]);

            const counts = allChanges.rows[0];
            let finalStatus = 'approved';

            if (parseInt(counts.approved) === 0) {
                finalStatus = 'rejected';
            } else if (parseInt(counts.rejected) > 0) {
                finalStatus = 'partially_approved';
            }

            // Update request status
            await client.query(`
                UPDATE permission_change_requests
                SET 
                    status = $1,
                    reviewer_id = $2,
                    reviewed_at = NOW(),
                    review_note = $3
                WHERE id = $4
            `, [finalStatus, user.userId, note || null, id]);

            return { finalStatus, appliedCount: approvedChanges.rows.length };
        });

        res.json({
            success: true,
            status: txResult.finalStatus,
            appliedChanges: txResult.appliedCount,
            message: `Request ${txResult.finalStatus}. ${txResult.appliedCount} changes applied.`
        });

    } catch (error: any) {
        console.error('Finalize request error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =============================================================
// CANCEL REQUEST (by requester only, if still pending)
// =============================================================
export const cancelRequest = async (req: Request, res: Response) => {
    try {
        const user = (req as any).user;
        const { id } = req.params;

        // Get request
        const requestResult = await query(`
            SELECT * FROM permission_change_requests WHERE id = $1
        `, [id]);

        if (requestResult.rows.length === 0) {
            return res.status(404).json({ error: 'Request not found' });
        }

        const request = requestResult.rows[0];

        // Only requester or super_admin can cancel
        if (request.requested_by !== user.userId && user.role !== 'super_admin') {
            return res.status(403).json({ error: 'Only the requester or super admin can cancel this request' });
        }

        if (request.status !== 'pending') {
            return res.status(400).json({ error: 'Only pending requests can be cancelled' });
        }

        // Delete the request (cascade will delete details)
        await query('DELETE FROM permission_change_requests WHERE id = $1', [id]);

        res.json({ success: true, message: 'Request cancelled' });

    } catch (error: any) {
        console.error('Cancel request error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};
