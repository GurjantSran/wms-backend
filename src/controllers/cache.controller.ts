// File Path = warehouse-backend/src/controllers/cache.controller.ts
// Cache APIs for frontend IndexedDB sync - Fast WSN lookups
import { Request, Response } from 'express';
import { query } from '../config/database';
import { safeError } from '../utils/sanitizeError';

// Validate that the requested warehouseId is in the user's accessible warehouses
const validateWarehouseAccess = (req: Request, warehouseId: string | number): boolean => {
    const accessible = (req as any).accessibleWarehouses;
    if (accessible === null || accessible === undefined) return true; // null = admin/super_admin, all access
    return accessible.map(String).includes(String(warehouseId));
};

/**
 * GET /api/cache/pending
 * Returns master_data items that are NOT yet received (not in inbound) and NOT rejected
 * Used by Inbound page for instant WSN autofill
 */
export const getPendingInventory = async (req: Request, res: Response) => {
    try {
        const { warehouseId } = req.query;

        if (!warehouseId) {
            return res.status(400).json({ error: 'Warehouse ID required' });
        }

        if (!validateWarehouseAccess(req, warehouseId as string)) {
            return res.status(403).json({ error: 'Forbidden: You do not have access to this warehouse' });
        }

        console.log('📦 Loading pending inventory for cache, warehouse:', warehouseId);

        // First check if rejections table exists
        let hasRejectionsTable = false;
        try {
            const tableCheck = await query(
                `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'rejections')`
            );
            hasRejectionsTable = tableCheck.rows[0]?.exists === true;
        } catch (e) {
            console.log('⚠️ Rejections table check failed, skipping rejections filter');
        }

        // Get all master_data items that are:
        // 1. NOT in inbound table for this warehouse (not yet received)
        // 2. NOT in rejections table (not rejected) - if table exists
        // NOTE: master_data is global (no warehouse_id), but inbound is per warehouse
        // ⚡ OPTIMIZED: Use UPPER() only (not TRIM) so idx_inbound_wsn_upper and
        //    idx_master_data_wsn_upper indexes can be used for fast lookups
        let sql = `
            SELECT 
                m.id, m.wsn, m.wid, m.fsn, m.order_id, 
                m.product_title, m.brand, m.cms_vertical,
                m.mrp, m.fsp, m.hsn_sac, m.igst_rate, m.fkt_link,
                m.vrp, m.yield_value, m.p_type, m.p_size,
                m.fk_grade, m.fkqc_remark,
                m.batch_id, m.created_at
            FROM master_data m
            WHERE m.deleted_at IS NULL
              AND m.warehouse_id = $1
              AND NOT EXISTS (
                    SELECT 1 FROM inbound i 
                    WHERE UPPER(i.wsn) = UPPER(m.wsn)
                    AND i.warehouse_id = m.warehouse_id
                )
        `;

        // Add rejections filter only if table exists
        if (hasRejectionsTable) {
            sql += `
                AND NOT EXISTS (
                    SELECT 1 FROM rejections r 
                    WHERE UPPER(r.wsn) = UPPER(m.wsn)
                    AND r.warehouse_id = m.warehouse_id
                )
            `;
        }

        // ⚡ No ORDER BY needed for cache loading (saves sort cost on large datasets)

        const result = await query(sql, [warehouseId]);
        console.log(`✅ Found ${result.rows.length} pending inventory items for inbound cache`);

        res.json({
            success: true,
            count: result.rows.length,
            data: result.rows,
            timestamp: new Date().toISOString()
        });
    } catch (error: any) {
        console.error('❌ Pending inventory cache error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * GET /api/cache/available
 * Returns all items currently in warehouse (PICKING > QC > INBOUND priority)
 * that are NOT yet dispatched
 * Used by QC, Picking, Outbound pages for instant WSN autofill
 */
export const getAvailableInventory = async (req: Request, res: Response) => {
    try {
        const { warehouseId } = req.query;

        if (!warehouseId) {
            return res.status(400).json({ error: 'Warehouse ID required' });
        }

        if (!validateWarehouseAccess(req, warehouseId as string)) {
            return res.status(403).json({ error: 'Forbidden: You do not have access to this warehouse' });
        }

        console.log('📦 Loading available inventory for cache, warehouse:', warehouseId);

        // Get all items from PICKING, QC, INBOUND that are NOT yet dispatched
        // Priority: PICKING > QC > INBOUND (higher priority = more processed)
        // ⚡ OPTIMIZED: Use UPPER() only (not TRIM) so indexes can be used for fast lookups
        // ⚡ No ORDER BY needed for cache loading (saves sort cost on large datasets)
        const sql = `
      -- Items from PICKING (not yet dispatched) - highest priority
      SELECT 
        p.wsn, 'PICKING' as source, 'available' as status,
        p.picking_date as last_action_date, p.rack_no, p.picker_name as action_by,
        m.id as master_id, m.wid, m.fsn, m.order_id, m.product_title, m.brand, m.cms_vertical,
        m.mrp, m.fsp, m.hsn_sac, m.igst_rate, m.fkt_link,
        m.wh_location, m.vrp, m.yield_value, m.p_type, m.p_size,
        m.fk_grade, m.fkqc_remark, m.invoice_date
      FROM picking p
      LEFT JOIN master_data m ON UPPER(p.wsn) = UPPER(m.wsn) AND m.deleted_at IS NULL AND m.warehouse_id = p.warehouse_id
      WHERE p.warehouse_id = $1
        AND NOT EXISTS (SELECT 1 FROM outbound o WHERE UPPER(o.wsn) = UPPER(p.wsn))

      UNION ALL

      -- Items from QC (pass status, not in picking, not yet dispatched)
      SELECT 
        q.wsn, 'QC' as source, 'available' as status,
        q.qc_date as last_action_date, q.rack_no, q.qc_by_name as action_by,
        m.id as master_id, m.wid, m.fsn, m.order_id, m.product_title, m.brand, m.cms_vertical,
        m.mrp, m.fsp, m.hsn_sac, m.igst_rate, m.fkt_link,
        m.wh_location, m.vrp, m.yield_value, m.p_type, m.p_size,
        m.fk_grade, m.fkqc_remark, m.invoice_date
      FROM qc q
      LEFT JOIN master_data m ON UPPER(q.wsn) = UPPER(m.wsn) AND m.deleted_at IS NULL AND m.warehouse_id = q.warehouse_id
      WHERE q.warehouse_id = $1
        AND q.qc_status = 'Pass'
        AND NOT EXISTS (SELECT 1 FROM picking p WHERE UPPER(p.wsn) = UPPER(q.wsn))
        AND NOT EXISTS (SELECT 1 FROM outbound o WHERE UPPER(o.wsn) = UPPER(q.wsn))

      UNION ALL

      -- Items from INBOUND (not in QC, not in picking, not yet dispatched)
      SELECT 
        i.wsn, 'INBOUND' as source, 'available' as status,
        i.inbound_date as last_action_date, i.rack_no, i.created_user_name as action_by,
        m.id as master_id, m.wid, m.fsn, m.order_id, m.product_title, m.brand, m.cms_vertical,
        m.mrp, m.fsp, m.hsn_sac, m.igst_rate, m.fkt_link,
        m.wh_location, m.vrp, m.yield_value, m.p_type, m.p_size,
        m.fk_grade, m.fkqc_remark, m.invoice_date
      FROM inbound i
      LEFT JOIN master_data m ON UPPER(i.wsn) = UPPER(m.wsn) AND m.deleted_at IS NULL AND m.warehouse_id = i.warehouse_id
      WHERE i.warehouse_id = $1
        AND NOT EXISTS (SELECT 1 FROM qc q WHERE UPPER(q.wsn) = UPPER(i.wsn))
        AND NOT EXISTS (SELECT 1 FROM picking p WHERE UPPER(p.wsn) = UPPER(i.wsn))
        AND NOT EXISTS (SELECT 1 FROM outbound o WHERE UPPER(o.wsn) = UPPER(i.wsn))
    `;

        const result = await query(sql, [warehouseId]);
        console.log(`✅ Found ${result.rows.length} available inventory items for cache`);

        res.json({
            success: true,
            count: result.rows.length,
            data: result.rows,
            timestamp: new Date().toISOString()
        });
    } catch (error: any) {
        console.error('❌ Available inventory cache error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * GET /api/cache/stats
 * Returns cache-related statistics for monitoring
 */
export const getCacheStats = async (req: Request, res: Response) => {
    try {
        const { warehouseId } = req.query;

        if (!warehouseId) {
            return res.status(400).json({ error: 'Warehouse ID required' });
        }

        if (!validateWarehouseAccess(req, warehouseId as string)) {
            return res.status(403).json({ error: 'Forbidden: You do not have access to this warehouse' });
        }

        // Get counts for both pending and available
        // ⚡ OPTIMIZED: Use UPPER() only (not TRIM) so indexes can be used
        const pendingCountSql = `
      SELECT COUNT(*) as count FROM master_data m
      WHERE m.deleted_at IS NULL
        AND m.warehouse_id = $1
        AND NOT EXISTS (SELECT 1 FROM inbound i WHERE UPPER(i.wsn) = UPPER(m.wsn) AND i.warehouse_id = m.warehouse_id)
        AND NOT EXISTS (SELECT 1 FROM rejections r WHERE UPPER(r.wsn) = UPPER(m.wsn) AND r.warehouse_id = m.warehouse_id)
    `;

        const availableCountSql = `
      SELECT 
        (SELECT COUNT(*) FROM picking p WHERE p.warehouse_id = $1 
          AND NOT EXISTS (SELECT 1 FROM outbound o WHERE UPPER(o.wsn) = UPPER(p.wsn))) +
        (SELECT COUNT(*) FROM qc q WHERE q.warehouse_id = $1 AND q.qc_status = 'Pass'
          AND NOT EXISTS (SELECT 1 FROM picking p WHERE UPPER(p.wsn) = UPPER(q.wsn))
          AND NOT EXISTS (SELECT 1 FROM outbound o WHERE UPPER(o.wsn) = UPPER(q.wsn))) +
        (SELECT COUNT(*) FROM inbound i WHERE i.warehouse_id = $1
          AND NOT EXISTS (SELECT 1 FROM qc q WHERE UPPER(q.wsn) = UPPER(i.wsn))
          AND NOT EXISTS (SELECT 1 FROM picking p WHERE UPPER(p.wsn) = UPPER(i.wsn))
          AND NOT EXISTS (SELECT 1 FROM outbound o WHERE UPPER(o.wsn) = UPPER(i.wsn)))
        as count
    `;

        const [pendingResult, availableResult] = await Promise.all([
            query(pendingCountSql, [warehouseId]),
            query(availableCountSql, [warehouseId])
        ]);

        res.json({
            success: true,
            pending: parseInt(pendingResult.rows[0]?.count || '0'),
            available: parseInt(availableResult.rows[0]?.count || '0'),
            timestamp: new Date().toISOString()
        });
    } catch (error: any) {
        console.error('❌ Cache stats error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};
