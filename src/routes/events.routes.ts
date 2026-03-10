// File Path = warehouse-backend/src/routes/events.routes.ts
// SSE (Server-Sent Events) Route for real-time multi-device sync
import { Router, Request, Response } from 'express';
import { authMiddleware } from '../middleware/auth.middleware';
import { sseManager } from '../services/sseManager';
import { verifyToken } from '../config/auth';

const router = Router();

/**
 * GET /api/events/subscribe
 * Establish SSE connection for real-time updates
 * 
 * NOTE: EventSource API doesn't support custom headers, so auth token
 * is passed as query param instead of Authorization header.
 * 
 * Query params:
 *   - warehouseId: number (required)
 *   - page: string (required) — 'inbound' | 'qc' | 'picking' | 'outbound'
 *   - deviceId: string (required) — unique per browser tab
 *   - token: string (required) — JWT auth token
 */
router.get('/subscribe', (req: Request, res: Response) => {
    // Manual auth: EventSource can't set Authorization header
    const token = req.query.token as string;
    if (!token) {
        return res.status(401).json({ error: 'Token required' });
    }

    let user: any;
    try {
        user = verifyToken(token);
    } catch {
        return res.status(401).json({ error: 'Invalid token' });
    }

    const userId = user?.userId || user?.id;
    const warehouseId = parseInt(req.query.warehouseId as string);
    const page = req.query.page as string;
    const deviceId = req.query.deviceId as string;

    if (!userId || !warehouseId || !page || !deviceId) {
        return res.status(400).json({ error: 'Missing required params: warehouseId, page, deviceId' });
    }

    const validPages = ['inbound', 'qc', 'picking', 'outbound'];
    if (!validPages.includes(page)) {
        return res.status(400).json({ error: `Invalid page. Must be one of: ${validPages.join(', ')}` });
    }

    // Set SSE headers
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-transform',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no', // Disable Nginx/Render proxy buffering
    });

    // Force headers to be sent immediately (prevents proxy buffering)
    res.flushHeaders();

    // SSE comment padding — forces proxy to start streaming mode
    res.write(':ok\n\n');

    // Send initial connection event
    res.write(`event: connected\ndata: ${JSON.stringify({ message: 'Connected', userId, warehouseId, page, deviceId })}\n\n`);
    // Flush to ensure event reaches client through proxy
    if (typeof (res as any).flush === 'function') (res as any).flush();

    // Register connection
    sseManager.addConnection(res, userId, warehouseId, page, deviceId);
});

/**
 * GET /api/events/stats
 * Get SSE connection stats (for debugging/monitoring)
 */
router.get('/stats', authMiddleware, (req: Request, res: Response) => {
    res.json(sseManager.getStats());
});

export default router;
