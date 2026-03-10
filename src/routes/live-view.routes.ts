// Live View Routes - Real-time entry viewing for Multi Entry grids
import express, { Request, Response, NextFunction } from 'express';
import { getSupabase, supabaseRetry, isSupabaseCircuitOpen } from '../config/supabase';
import { authMiddleware, hasRole } from '../middleware/auth.middleware';

const router = express.Router();

// ===== In-memory cache for active sessions (prevents redundant Supabase calls) =====
interface CacheEntry {
    data: any;
    timestamp: number;
}
const sessionsCache = new Map<string, CacheEntry>();
const SESSION_CACHE_TTL_MS = 5000; // 5 seconds TTL

function getCachedSessions(key: string): any | null {
    const entry = sessionsCache.get(key);
    if (entry && Date.now() - entry.timestamp < SESSION_CACHE_TTL_MS) {
        return entry.data;
    }
    sessionsCache.delete(key);
    return null;
}

function setCachedSessions(key: string, data: any): void {
    sessionsCache.set(key, { data, timestamp: Date.now() });
    // Evict old entries periodically (keep map bounded)
    if (sessionsCache.size > 100) {
        const now = Date.now();
        for (const [k, v] of sessionsCache) {
            if (now - v.timestamp > SESSION_CACHE_TTL_MS) sessionsCache.delete(k);
        }
    }
}

// Types
interface LiveSession {
    session_id: string;
    user_id: number;
    warehouse_id: number;
    page_type: string;
    started_at: string;
    last_activity_at: string;
    is_active: boolean;
    total_entries: number;
    user_name?: string;
}

interface LiveEntry {
    wsn: string;
    product_title?: string;
    brand?: string;
    mrp?: number;
    fsp?: number;
    cms_vertical?: string;
    fkqc_remarks?: string;
    p_type?: string;
    p_size?: string;
    source?: string;
    wid?: string;
    fsn?: string;
    order_id?: string;
    fk_grade?: string;
    hsn_sac?: string;
    igst_rate?: number;
    vrp?: number;
    yield_value?: number;
    invoice_date?: string;
    fkt_link?: string;
    wh_location?: string;
    quantity?: number;
    dispatch_remarks?: string;
    other_remarks?: string;
    rack_no?: string;
    qc_grade?: string;
    qc_remarks?: string;
    product_serial_number?: string;
    row_index: number;
    created_at?: string;
}

// ===== START SESSION =====
// POST /api/live-view/start
router.post('/start', authMiddleware, async (req: Request, res: Response, next: NextFunction) => {
    try {
        const { warehouse_id, page_type } = req.body;
        const user_id = (req as any).user?.userId;

        // Validate warehouse_id - must be a valid number
        if (!warehouse_id || warehouse_id === 'undefined' || warehouse_id === undefined) {
            return res.json({ success: false, sessionId: null, message: 'No warehouse selected' });
        }

        const warehouseIdNum = typeof warehouse_id === 'number' ? warehouse_id : parseInt(warehouse_id, 10);
        if (isNaN(warehouseIdNum)) {
            return res.json({ success: false, sessionId: null, message: 'Invalid warehouse_id' });
        }

        if (!page_type || !['inbound', 'qc', 'picking', 'outbound'].includes(page_type)) {
            return res.status(400).json({ error: 'Invalid page_type' });
        }

        if (!user_id) {
            return res.status(401).json({ error: 'User not authenticated' });
        }

        const supabase = getSupabase();

        // Upsert session (reactivate if exists, create if not)
        const { data, error } = await supabaseRetry(
            () => supabase
                .from('live_entry_sessions')
                .upsert({
                    user_id,
                    warehouse_id: warehouseIdNum,
                    page_type,
                    is_active: true,
                    started_at: new Date().toISOString(),
                    last_activity_at: new Date().toISOString(),
                    total_entries: 0
                }, {
                    onConflict: 'user_id,warehouse_id,page_type'
                })
                .select('session_id')
                .single(),
            'startLiveSession'
        );

        if (error) {
            // Circuit breaker errors → graceful response (not a crash)
            if (error.isCircuitBreaker) {
                return res.json({ success: false, sessionId: null, message: 'Supabase temporarily unavailable, please try again in a minute' });
            }
            throw error;
        }

        res.json({
            success: true,
            sessionId: data?.session_id,
            message: 'Live session started'
        });
    } catch (error: any) {
        // Connectivity errors → graceful response instead of 500
        if (error?.message?.includes('fetch failed') || error?.message?.includes('abort') || error?.message?.includes('timeout')) {
            console.warn('[LiveView] Start session failed (connectivity):', error.message);
            return res.json({ success: false, sessionId: null, message: 'Supabase temporarily unavailable' });
        }
        console.error('Error starting live session:', error);
        next(error);
    }
});

// Helper: safely truncate string to max length (defense-in-depth)
const safeStr = (val: string | null | undefined, maxLen: number): string | null => {
    if (!val) return null;
    return val.length > maxLen ? val.substring(0, maxLen) : val;
};

// ===== UPDATE ENTRIES =====
// POST /api/live-view/update
router.post('/update', authMiddleware, async (req: Request, res: Response, next: NextFunction) => {
    try {
        const { session_id, entries } = req.body;
        const user_id = (req as any).user?.userId;

        if (!session_id || !Array.isArray(entries)) {
            return res.status(400).json({ error: 'Missing session_id or entries array' });
        }

        const supabase = getSupabase();

        // Check if session still exists, is active, AND belongs to the current user
        const { data: sessionCheck } = await supabaseRetry(
            () => supabase
                .from('live_entry_sessions')
                .select('session_id, is_active, user_id')
                .eq('session_id', session_id)
                .single(),
            'checkSessionOwner'
        );

        if (!sessionCheck || !sessionCheck.is_active) {
            // Session was cleaned up or ended - tell frontend to restart
            return res.json({ success: false, sessionExpired: true, message: 'Session expired or not found' });
        }

        // Owner check: only the session creator can update it
        if (sessionCheck.user_id !== user_id) {
            return res.status(403).json({ error: 'Not authorized to update this session' });
        }

        // Update session activity
        await supabase
            .from('live_entry_sessions')
            .update({
                last_activity_at: new Date().toISOString(),
                total_entries: entries.length
            })
            .eq('session_id', session_id);

        // Build valid entries (only non-empty WSNs) with safe string truncation
        const validEntries = entries
            .filter((e: LiveEntry) => e.wsn?.trim())
            .map((e: LiveEntry) => ({
                session_id,
                wsn: safeStr(e.wsn.trim().toUpperCase(), 100)!,
                product_title: safeStr(e.product_title, 500),
                brand: safeStr(e.brand, 200),
                mrp: e.mrp || null,
                fsp: e.fsp || null,
                cms_vertical: safeStr(e.cms_vertical, 200),
                fkqc_remarks: safeStr(e.fkqc_remarks, 500),
                p_type: safeStr(e.p_type, 100),
                p_size: safeStr(e.p_size, 100),
                source: safeStr(e.source, 100),
                wid: safeStr(e.wid, 100),
                fsn: safeStr(e.fsn, 100),
                order_id: safeStr(e.order_id, 100),
                fk_grade: safeStr(e.fk_grade, 200),
                hsn_sac: safeStr(e.hsn_sac, 200),
                igst_rate: e.igst_rate || null,
                vrp: e.vrp || null,
                yield_value: e.yield_value || null,
                invoice_date: safeStr(e.invoice_date, 100),
                fkt_link: safeStr(e.fkt_link, 500),
                wh_location: safeStr(e.wh_location, 100),
                quantity: e.quantity || null,
                dispatch_remarks: safeStr(e.dispatch_remarks, 500),
                other_remarks: safeStr(e.other_remarks, 500),
                rack_no: safeStr(e.rack_no, 100),
                qc_grade: safeStr(e.qc_grade, 200),
                qc_remarks: safeStr(e.qc_remarks, 500),
                product_serial_number: safeStr(e.product_serial_number, 200),
                row_index: e.row_index
            }));

        if (validEntries.length > 0) {
            // Atomic UPSERT: ON CONFLICT (session_id, row_index) DO UPDATE
            // This eliminates the race condition from the old DELETE + INSERT pattern
            const { error } = await supabaseRetry(
                () => supabase
                    .from('live_entries')
                    .upsert(validEntries, {
                        onConflict: 'session_id,row_index'
                    }),
                'upsertLiveEntries'
            );

            if (error) throw error;

            // Remove stale trailing rows that are no longer in the current entry set
            // (e.g., user deleted rows from the grid since last sync)
            const maxRowIndex = Math.max(...validEntries.map((e: any) => e.row_index));
            await supabase
                .from('live_entries')
                .delete()
                .eq('session_id', session_id)
                .gt('row_index', maxRowIndex);
        } else {
            // No valid entries — clear all entries for this session
            await supabase
                .from('live_entries')
                .delete()
                .eq('session_id', session_id);
        }

        res.json({
            success: true,
            entries_count: validEntries.length
        });
    } catch (error: any) {
        console.error('Error updating live entries:', error);
        next(error);
    }
});

// ===== END SESSION =====
// POST /api/live-view/end
router.post('/end', authMiddleware, async (req: Request, res: Response, next: NextFunction) => {
    try {
        const { session_id } = req.body;
        const user_id = (req as any).user?.userId;

        if (!session_id) {
            return res.status(400).json({ error: 'Missing session_id' });
        }

        const supabase = getSupabase();

        // Owner check: only the session creator can end it
        const { data: sessionCheck } = await supabaseRetry(
            () => supabase
                .from('live_entry_sessions')
                .select('session_id, user_id')
                .eq('session_id', session_id)
                .single(),
            'endSessionOwnerCheck'
        );

        if (!sessionCheck) {
            return res.status(404).json({ error: 'Session not found' });
        }

        if (sessionCheck.user_id !== user_id) {
            return res.status(403).json({ error: 'Not authorized to end this session' });
        }

        // Deactivate session
        await supabase
            .from('live_entry_sessions')
            .update({ is_active: false })
            .eq('session_id', session_id);

        // Clear entries
        await supabase
            .from('live_entries')
            .delete()
            .eq('session_id', session_id);

        res.json({ success: true, message: 'Live session ended' });
    } catch (error: any) {
        console.error('Error ending live session:', error);
        next(error);
    }
});

// ===== GET ACTIVE SESSIONS =====
// GET /api/live-view/sessions?warehouse_id=X&page_type=Y
router.get('/sessions', authMiddleware, async (req: Request, res: Response, next: NextFunction) => {
    try {
        const { warehouse_id, page_type } = req.query;
        const current_user_id = (req as any).user?.userId;

        // Validate: warehouse_id must be a valid number
        if (!warehouse_id || warehouse_id === 'undefined' || !page_type || page_type === 'undefined') {
            return res.json({ sessions: [] }); // Return empty instead of error
        }

        const warehouseIdNum = parseInt(warehouse_id as string, 10);
        if (isNaN(warehouseIdNum)) {
            return res.json({ sessions: [] });
        }

        const supabase = getSupabase();

        // Check cache first (prevents thundering herd of identical Supabase calls)
        const cacheKey = `${warehouseIdNum}:${page_type}:${current_user_id}`;
        const cached = getCachedSessions(cacheKey);
        if (cached) {
            return res.json(cached);
        }

        // Get active sessions (excluding current user) - fetch sessions first
        const { data, error } = await supabaseRetry(
            () => supabase
                .from('live_entry_sessions')
                .select(`
                    session_id,
                    user_id,
                    warehouse_id,
                    page_type,
                    started_at,
                    last_activity_at,
                    total_entries
                `)
                .eq('warehouse_id', warehouseIdNum)
                .eq('page_type', page_type)
                .eq('is_active', true)
                .neq('user_id', current_user_id)
                .gte('last_activity_at', new Date(Date.now() - 10 * 60 * 1000).toISOString()),
            'fetchActiveSessions'
        );

        if (error) {
            // Circuit breaker → return empty sessions silently
            if (error.isCircuitBreaker) {
                return res.json({ sessions: [] });
            }
            throw error;
        }

        // Fetch user names separately if we have sessions
        const userIds = [...new Set((data || []).map((s: any) => s.user_id))];
        let userMap: Record<number, string> = {};

        if (userIds.length > 0) {
            const { data: users } = await supabaseRetry(
                () => supabase
                    .from('users')
                    .select('id, full_name')
                    .in('id', userIds),
                'fetchSessionUserNames'
            );

            userMap = (users || []).reduce((acc: Record<number, string>, u: any) => {
                acc[u.id] = u.full_name || 'Unknown User';
                return acc;
            }, {});
        }

        const sessions = (data || []).map((s: any) => ({
            session_id: s.session_id,
            user_id: s.user_id,
            user_name: userMap[s.user_id] || 'Unknown User',
            warehouse_id: s.warehouse_id,
            page_type: s.page_type,
            started_at: s.started_at,
            last_activity_at: s.last_activity_at,
            total_entries: s.total_entries
        }));

        const result = { sessions };
        setCachedSessions(cacheKey, result);

        res.json(result);
    } catch (error: any) {
        // Only log full details for non-connectivity errors (connectivity errors are handled by circuit breaker)
        const isConnErr = error?.message?.includes('fetch failed') || error?.message?.includes('abort') || error?.message?.includes('timeout');
        if (isConnErr) {
            console.warn('[LiveView] fetchActiveSessions connectivity error:', error.message);
        } else {
            console.error('Error fetching live sessions:', error?.message || error);
            console.error('[LiveView] Full error details:', JSON.stringify({
                message: error?.message,
                code: error?.code,
                status: error?.status || error?.statusCode,
                hint: error?.hint,
                details: error?.details,
                isCloudflare: (error?.message || '').includes('cloudflare') || (error?.message || '').includes('<!DOCTYPE'),
                timestamp: new Date().toISOString(),
            }));
        }
        // Return empty sessions instead of error to prevent UI crash
        res.json({ sessions: [] });
    }
});

// ===== GET ENTRIES FOR A SESSION =====
// GET /api/live-view/entries/:session_id
router.get('/entries/:session_id', authMiddleware, async (req: Request, res: Response, next: NextFunction) => {
    try {
        const { session_id } = req.params;

        if (!session_id) {
            return res.status(400).json({ error: 'Missing session_id' });
        }

        const supabase = getSupabase();

        // Get session info
        const { data: session } = await supabaseRetry(
            () => supabase
                .from('live_entry_sessions')
                .select(`
                    session_id,
                    user_id,
                    page_type,
                    started_at,
                    total_entries,
                    users!inner(full_name)
                `)
                .eq('session_id', session_id)
                .single(),
            'fetchSessionInfo'
        );

        if (!session) {
            return res.status(404).json({ error: 'Session not found' });
        }

        // Get entries
        const { data: entries, error } = await supabaseRetry(
            () => supabase
                .from('live_entries')
                .select('*')
                .eq('session_id', session_id)
                .order('row_index', { ascending: true }),
            'fetchSessionEntries'
        );

        if (error) throw error;

        res.json({
            session: {
                ...session,
                user_name: (session as any).users?.full_name || 'Unknown User'
            },
            entries: entries || []
        });
    } catch (error: any) {
        console.error('Error fetching live entries:', error);
        next(error);
    }
});

// ===== EXPORT ENTRIES =====
// GET /api/live-view/export/:session_id
router.get('/export/:session_id', authMiddleware, async (req: Request, res: Response, next: NextFunction) => {
    try {
        const { session_id } = req.params;

        if (!session_id) {
            return res.status(400).json({ error: 'Missing session_id' });
        }

        const supabase = getSupabase();

        // Get session info
        const { data: session } = await supabaseRetry(
            () => supabase
                .from('live_entry_sessions')
                .select(`
                    user_id,
                    page_type,
                    warehouse_id,
                    started_at,
                    users!inner(full_name),
                    warehouses!inner(name)
                `)
                .eq('session_id', session_id)
                .single(),
            'exportSessionInfo'
        );

        if (!session) {
            return res.status(404).json({ error: 'Session not found' });
        }

        // Get entries
        const { data: entries, error } = await supabaseRetry(
            () => supabase
                .from('live_entries')
                .select('*')
                .eq('session_id', session_id)
                .order('row_index', { ascending: true }),
            'exportSessionEntries'
        );

        if (error) throw error;

        res.json({
            user_name: (session as any).users?.full_name || 'Unknown',
            warehouse_name: (session as any).warehouses?.name || 'Unknown',
            page_type: session.page_type,
            started_at: session.started_at,
            total_entries: (entries || []).length,
            entries: entries || []
        });
    } catch (error: any) {
        console.error('Error exporting live entries:', error);
        next(error);
    }
});

// ===== CLEANUP STALE SESSIONS =====
// POST /api/live-view/cleanup (internal/admin only)
router.post('/cleanup', authMiddleware, hasRole('admin', 'super_admin'), async (req: Request, res: Response, next: NextFunction) => {
    try {
        const supabase = getSupabase();

        // Call cleanup function
        const { data, error } = await supabaseRetry(
            () => supabase.rpc('cleanup_stale_live_sessions'),
            'cleanupStaleSessions'
        );

        if (error) throw error;

        res.json({
            success: true,
            cleaned_sessions: data || 0
        });
    } catch (error: any) {
        console.error('Error cleaning up live sessions:', error);
        next(error);
    }
});

export default router;
