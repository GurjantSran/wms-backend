// File Path = warehouse-backend/src/routes/sessions.routes.ts
import { Router, Request, Response } from 'express';
import { query } from '../config/database';
import { authMiddleware, hasRole } from '../middleware/auth.middleware';
import { clearPermissionCache, requirePermissionOrRole } from '../middleware/rbac.middleware';
import crypto from 'crypto';
import logger from '../utils/logger';

const router = Router();

// Permission-based access for session management routes
const canViewSessions = requirePermissionOrRole('menu:settings:users', 'admin');
const canLogoutUser = requirePermissionOrRole('btn:users:force_logout', 'admin');

// Helper to hash token
const hashToken = (token: string): string => {
    return crypto.createHash('sha256').update(token).digest('hex');
};

// Parse user agent to extract device info
const parseUserAgent = (userAgent: string): { deviceType: string; browser: string; os: string } => {
    let deviceType = 'desktop';
    let browser = 'unknown';
    let os = 'unknown';

    if (!userAgent) return { deviceType, browser, os };

    const ua = userAgent.toLowerCase();

    // Detect device type
    if (/mobile|android|iphone|ipad|ipod|blackberry|iemobile|opera mini/i.test(ua)) {
        deviceType = /ipad|tablet/i.test(ua) ? 'tablet' : 'mobile';
    }

    // Detect browser
    if (ua.includes('edg/')) browser = 'Edge';
    else if (ua.includes('chrome')) browser = 'Chrome';
    else if (ua.includes('firefox')) browser = 'Firefox';
    else if (ua.includes('safari')) browser = 'Safari';
    else if (ua.includes('opera') || ua.includes('opr')) browser = 'Opera';
    else if (ua.includes('msie') || ua.includes('trident')) browser = 'IE';

    // Detect OS
    if (ua.includes('windows nt 10')) os = 'Windows 10/11';
    else if (ua.includes('windows')) os = 'Windows';
    else if (ua.includes('mac os x')) os = 'macOS';
    else if (ua.includes('android')) os = 'Android';
    else if (ua.includes('iphone') || ua.includes('ipad')) os = 'iOS';
    else if (ua.includes('linux')) os = 'Linux';

    return { deviceType, browser, os };
};

// ==================== HEARTBEAT - Real-time online status ====================
router.post('/heartbeat', authMiddleware, async (req: Request, res: Response) => {
    try {
        const userId = req.user!.userId;
        const token = req.headers.authorization?.replace('Bearer ', '');

        if (!token) {
            return res.status(401).json({ error: 'No token provided' });
        }

        const tokenHash = hashToken(token);
        const userAgent = req.headers['user-agent'] || '';
        const ipAddress = req.ip || req.socket.remoteAddress || '';
        const { deviceType, browser, os } = parseUserAgent(userAgent);

        // Update last_activity for this session
        const result = await query(`
            UPDATE active_sessions 
            SET 
                last_activity = NOW(),
                device_type = $3,
                browser = $4,
                os = $5,
                ip_address = COALESCE($6, ip_address)
            WHERE user_id = $1 AND token_hash = $2 AND is_active = true
            RETURNING id
        `, [userId, tokenHash, deviceType, browser, os, ipAddress]);

        if (result.rowCount === 0) {
            return res.status(401).json({ error: 'Session not found or expired' });
        }

        // Also update user's last_seen
        await query('UPDATE users SET last_seen = NOW() WHERE id = $1', [userId]);

        res.json({ success: true, timestamp: new Date().toISOString() });
    } catch (error: any) {
        logger.error('Heartbeat error', error);
        res.status(500).json({ error: 'Heartbeat failed' });
    }
});

// Get all active sessions with enhanced info
router.get('/', authMiddleware, canViewSessions, async (req: Request, res: Response) => {
    try {
        const result = await query(`
            SELECT 
                s.id,
                s.user_id,
                u.username,
                u.full_name,
                u.role,
                s.ip_address,
                s.user_agent,
                s.device_type,
                s.browser,
                s.os,
                s.created_at as logged_in_at,
                s.last_activity,
                s.expires_at,
                CASE 
                    WHEN s.last_activity > NOW() - INTERVAL '2 minutes' THEN 'online'
                    WHEN s.last_activity > NOW() - INTERVAL '5 minutes' THEN 'away'
                    ELSE 'offline'
                END as status
            FROM active_sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.is_active = true AND s.expires_at > NOW()
            ORDER BY s.last_activity DESC NULLS LAST
        `);

        res.json({ sessions: result.rows });
    } catch (error: any) {
        logger.error('Error fetching sessions', error);
        res.status(500).json({ error: 'Failed to fetch active sessions' });
    }
});

// Get online users count (based on heartbeat - last 2 minutes)
router.get('/online-count', authMiddleware, canViewSessions, async (req: Request, res: Response) => {
    try {
        const result = await query(`
            SELECT COUNT(DISTINCT user_id) as count 
            FROM active_sessions 
            WHERE is_active = true 
            AND expires_at > NOW()
            AND (last_activity > NOW() - INTERVAL '2 minutes' OR last_activity IS NULL)
        `);

        res.json({ count: parseInt(result.rows[0].count) || 0 });
    } catch (error: any) {
        logger.error('Error fetching online count', error);
        res.status(500).json({ error: 'Failed to fetch online count' });
    }
});

// Get online user IDs with status (for frontend to show real-time status)
router.get('/online-users', authMiddleware, canViewSessions, async (req: Request, res: Response) => {
    try {
        const result = await query(`
            SELECT 
                user_id,
                MAX(last_activity) as last_activity,
                CASE 
                    WHEN MAX(last_activity) > NOW() - INTERVAL '2 minutes' THEN 'online'
                    WHEN MAX(last_activity) > NOW() - INTERVAL '5 minutes' THEN 'away'
                    ELSE 'offline'
                END as status
            FROM active_sessions 
            WHERE is_active = true AND expires_at > NOW()
            GROUP BY user_id
        `);

        // Return both userIds array and detailed status map
        const userIds = result.rows
            .filter((r: any) => r.status === 'online')
            .map((r: any) => r.user_id);

        const userStatus = result.rows.reduce((acc: any, r: any) => {
            acc[r.user_id] = {
                status: r.status,
                lastActivity: r.last_activity
            };
            return acc;
        }, {});

        res.json({ userIds, userStatus });
    } catch (error: any) {
        logger.error('Error fetching online users', error);
        res.status(500).json({ error: 'Failed to fetch online users' });
    }
});

// ==================== USER ACTIVITY & LOGIN HISTORY ====================

// Get user's login history
router.get('/login-history/:userId', authMiddleware, canViewSessions, async (req: Request, res: Response) => {
    try {
        const targetUserId = parseInt(req.params.userId);
        const { limit = 50, page = 1 } = req.query;
        const offset = (Number(page) - 1) * Number(limit);

        const result = await query(`
            SELECT 
                id,
                login_time,
                logout_time,
                duration_minutes,
                ip_address,
                device_type,
                browser,
                os,
                logout_reason
            FROM login_history
            WHERE user_id = $1
            ORDER BY login_time DESC
            LIMIT $2 OFFSET $3
        `, [targetUserId, Number(limit), offset]);

        const countResult = await query(
            'SELECT COUNT(*) as total FROM login_history WHERE user_id = $1',
            [targetUserId]
        );

        res.json({
            history: result.rows,
            total: parseInt(countResult.rows[0].total) || 0,
            page: Number(page),
            limit: Number(limit)
        });
    } catch (error: any) {
        logger.error('Error fetching login history', error);
        res.status(500).json({ error: 'Failed to fetch login history' });
    }
});

// Get user's current session details
router.get('/user-session/:userId', authMiddleware, canViewSessions, async (req: Request, res: Response) => {
    try {
        const targetUserId = parseInt(req.params.userId);

        const result = await query(`
            SELECT 
                s.id,
                s.ip_address,
                s.user_agent,
                s.device_type,
                s.browser,
                s.os,
                s.created_at as session_started,
                s.last_activity,
                s.expires_at,
                CASE 
                    WHEN s.last_activity > NOW() - INTERVAL '2 minutes' THEN 'online'
                    WHEN s.last_activity > NOW() - INTERVAL '5 minutes' THEN 'away'
                    ELSE 'offline'
                END as status,
                EXTRACT(EPOCH FROM (NOW() - s.created_at)) / 60 as session_duration_minutes
            FROM active_sessions s
            WHERE s.user_id = $1 AND s.is_active = true AND s.expires_at > NOW()
            ORDER BY s.last_activity DESC NULLS LAST
            LIMIT 1
        `, [targetUserId]);

        if (result.rows.length === 0) {
            return res.json({ session: null, isOnline: false });
        }

        res.json({
            session: result.rows[0],
            isOnline: result.rows[0].status === 'online'
        });
    } catch (error: any) {
        logger.error('Error fetching user session', error);
        res.status(500).json({ error: 'Failed to fetch user session' });
    }
});

// Get user activity logs
router.get('/activity/:userId', authMiddleware, canViewSessions, async (req: Request, res: Response) => {
    try {
        const targetUserId = parseInt(req.params.userId);
        const { limit = 50, page = 1, module } = req.query;
        const offset = (Number(page) - 1) * Number(limit);

        let whereClause = 'WHERE user_id = $1';
        const params: any[] = [targetUserId];
        let paramIndex = 2;

        if (module) {
            whereClause += ` AND module = $${paramIndex}`;
            params.push(module);
            paramIndex++;
        }

        params.push(Number(limit), offset);

        const result = await query(`
            SELECT 
                id,
                activity_type,
                module,
                action,
                details,
                ip_address,
                warehouse_id,
                timestamp
            FROM user_activity_logs
            ${whereClause}
            ORDER BY timestamp DESC
            LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
        `, params);

        const countResult = await query(
            `SELECT COUNT(*) as total FROM user_activity_logs ${whereClause}`,
            params.slice(0, -2)
        );

        res.json({
            activities: result.rows,
            total: parseInt(countResult.rows[0].total) || 0,
            page: Number(page),
            limit: Number(limit)
        });
    } catch (error: any) {
        logger.error('Error fetching user activity', error);
        res.status(500).json({ error: 'Failed to fetch user activity' });
    }
});

// Log user activity (called from other modules)
router.post('/log-activity', authMiddleware, async (req: Request, res: Response) => {
    try {
        const userId = req.user!.userId;
        const { activityType, module, action, details, warehouseId } = req.body;
        const ipAddress = req.ip || req.socket.remoteAddress || '';
        const userAgent = req.headers['user-agent'] || '';

        await query(`
            INSERT INTO user_activity_logs 
            (user_id, activity_type, module, action, details, ip_address, user_agent, warehouse_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `, [userId, activityType, module, action, JSON.stringify(details || {}), ipAddress, userAgent, warehouseId]);

        res.json({ success: true });
    } catch (error: any) {
        logger.error('Error logging activity', error);
        res.status(500).json({ error: 'Failed to log activity' });
    }
});

// Get user summary with last seen info
router.get('/user-summary/:userId', authMiddleware, canViewSessions, async (req: Request, res: Response) => {
    try {
        const targetUserId = parseInt(req.params.userId);

        // Get user info with last seen
        const userResult = await query(`
            SELECT 
                id, username, full_name, role, email, phone,
                last_seen, last_login, last_login_ip, last_login_device,
                created_at
            FROM users
            WHERE id = $1
        `, [targetUserId]);

        if (userResult.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }

        const user = userResult.rows[0];

        // Get current session if any
        const sessionResult = await query(`
            SELECT 
                device_type, browser, os, ip_address,
                created_at as session_started, last_activity,
                CASE 
                    WHEN last_activity > NOW() - INTERVAL '2 minutes' THEN 'online'
                    WHEN last_activity > NOW() - INTERVAL '5 minutes' THEN 'away'
                    ELSE 'offline'
                END as status
            FROM active_sessions
            WHERE user_id = $1 AND is_active = true AND expires_at > NOW()
            ORDER BY last_activity DESC NULLS LAST
            LIMIT 1
        `, [targetUserId]);

        // Get login stats
        const statsResult = await query(`
            SELECT 
                COUNT(*) as total_logins,
                COUNT(*) FILTER (WHERE login_time > NOW() - INTERVAL '30 days') as logins_last_30_days,
                AVG(duration_minutes) FILTER (WHERE duration_minutes IS NOT NULL) as avg_session_duration
            FROM login_history
            WHERE user_id = $1
        `, [targetUserId]);

        res.json({
            user,
            currentSession: sessionResult.rows[0] || null,
            isOnline: sessionResult.rows[0]?.status === 'online',
            stats: {
                totalLogins: parseInt(statsResult.rows[0].total_logins) || 0,
                loginsLast30Days: parseInt(statsResult.rows[0].logins_last_30_days) || 0,
                avgSessionDuration: Math.round(parseFloat(statsResult.rows[0].avg_session_duration) || 0)
            }
        });
    } catch (error: any) {
        logger.error('Error fetching user summary', error);
        res.status(500).json({ error: 'Failed to fetch user summary' });
    }
});

// Logout specific user (invalidate all their sessions)
router.post('/logout-user/:userId', authMiddleware, canLogoutUser, async (req: Request, res: Response) => {
    try {
        const targetUserId = parseInt(req.params.userId);
        const currentUser = req.user!;

        // Get target user info
        const targetResult = await query('SELECT id, role, username FROM users WHERE id = $1', [targetUserId]);
        if (targetResult.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }

        const targetUser = targetResult.rows[0];

        // Can't logout yourself
        if (targetUserId === currentUser.userId) {
            return res.status(400).json({ error: 'Cannot logout yourself from here. Use normal logout.' });
        }

        // Admin can only logout regular users (not other admins or super_admin)
        if (currentUser.role === 'admin') {
            if (targetUser.role === 'admin' || targetUser.role === 'super_admin') {
                return res.status(403).json({ error: 'Admins can only logout regular users, not other admins' });
            }
        }

        // Super admin can logout anyone except themselves (already checked above)

        // Invalidate all sessions for target user
        const result = await query(`
      UPDATE active_sessions 
      SET is_active = false 
      WHERE user_id = $1 AND is_active = true
      RETURNING id
    `, [targetUserId]);

        // Clear permission cache for logged out user
        clearPermissionCache(targetUserId);

        logger.info(`User ${currentUser.username} logged out user ${targetUser.username} (${result.rowCount} sessions)`);

        res.json({
            message: `User ${targetUser.username} has been logged out`,
            sessionsInvalidated: result.rowCount
        });
    } catch (error: any) {
        logger.error('Error logging out user', error);
        res.status(500).json({ error: 'Failed to logout user' });
    }
});

// Logout all users (super_admin only)
router.post('/logout-all', authMiddleware, hasRole('super_admin'), async (req: Request, res: Response) => {
    try {
        const currentUser = req.user!;
        const { excludeSelf } = req.body;

        let result;
        if (excludeSelf) {
            // Logout all except current user
            result = await query(`
        UPDATE active_sessions 
        SET is_active = false 
        WHERE is_active = true AND user_id != $1
        RETURNING id
      `, [currentUser.userId]);
        } else {
            // Logout everyone including self
            result = await query(`
        UPDATE active_sessions 
        SET is_active = false 
        WHERE is_active = true
        RETURNING id
      `);
        }

        // Clear all permission caches
        clearPermissionCache();

        logger.info(`Super admin ${currentUser.username} logged out all users (${result.rowCount} sessions)`);

        res.json({
            message: `All users have been logged out`,
            sessionsInvalidated: result.rowCount
        });
    } catch (error: any) {
        logger.error('Error logging out all users', error);
        res.status(500).json({ error: 'Failed to logout all users' });
    }
});

// Cleanup expired sessions (can be called periodically)
router.delete('/cleanup', authMiddleware, hasRole('super_admin'), async (req: Request, res: Response) => {
    try {
        const result = await query(`
      DELETE FROM active_sessions 
      WHERE expires_at < NOW() OR is_active = false
      RETURNING id
    `);

        res.json({
            message: 'Expired sessions cleaned up',
            deletedCount: result.rowCount
        });
    } catch (error: any) {
        logger.error('Error cleaning up sessions', error);
        res.status(500).json({ error: 'Failed to cleanup sessions' });
    }
});

export default router;
export { hashToken };
