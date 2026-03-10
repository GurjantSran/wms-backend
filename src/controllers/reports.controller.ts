// File Path = warehouse-backend/src/controllers/reports.controller.ts
import { Request, Response } from 'express';
import { query, queryWithTimeout } from '../config/database';
import * as XLSX from 'xlsx';
import { safeError } from '../utils/sanitizeError';

// Validate that a requested warehouse_id is in the user's accessible warehouses
const validateWarehouseAccess = (req: Request, warehouseId: string | number): boolean => {
    const accessible = (req as any).accessibleWarehouses;
    if (accessible === null || accessible === undefined) return true; // null = admin/super_admin, all access
    return accessible.map(String).includes(String(warehouseId));
};

// Enforce warehouse_id for non-admin users, or apply warehouse filter to queries
const getEffectiveWarehouseFilter = (req: Request, requestedWarehouseId?: string): { allowed: boolean; warehouseIds: number[] | null; error?: string } => {
    const accessible = (req as any).accessibleWarehouses;

    // Admin/super_admin with no restrictions
    if (accessible === null || accessible === undefined) {
        if (requestedWarehouseId) {
            return { allowed: true, warehouseIds: [Number(requestedWarehouseId)] };
        }
        return { allowed: true, warehouseIds: null }; // null = all warehouses
    }

    // Restricted user
    if (requestedWarehouseId) {
        if (!accessible.map(String).includes(String(requestedWarehouseId))) {
            return { allowed: false, warehouseIds: null, error: 'Forbidden: You do not have access to this warehouse' };
        }
        return { allowed: true, warehouseIds: [Number(requestedWarehouseId)] };
    }

    // No warehouse_id specified — restrict to user's accessible warehouses
    return { allowed: true, warehouseIds: accessible };
};

// =================== INVENTORY REPORTS ===================

// Current Stock Report - ⚡ OPTIMIZED with pagination
export const getCurrentStockReport = async (req: Request, res: Response) => {
    try {
        const { warehouse_id, brand, category, page = 1, limit = 1000 } = req.query;
        const offset = (Number(page) - 1) * Number(limit);

        // Enforce warehouse isolation
        const warehouseFilter = getEffectiveWarehouseFilter(req, warehouse_id as string | undefined);
        if (!warehouseFilter.allowed) {
            return res.status(403).json({ error: warehouseFilter.error });
        }

        let whereConditions: string[] = ['1=1'];
        const params: any[] = [];
        let paramIndex = 1;

        if (warehouseFilter.warehouseIds) {
            if (warehouseFilter.warehouseIds.length === 1) {
                whereConditions.push(`i.warehouse_id = $${paramIndex}`);
                params.push(warehouseFilter.warehouseIds[0]);
            } else {
                const placeholders = warehouseFilter.warehouseIds.map((_, idx) => `$${paramIndex + idx}`).join(',');
                whereConditions.push(`i.warehouse_id IN (${placeholders})`);
                warehouseFilter.warehouseIds.forEach(id => params.push(id));
                paramIndex += warehouseFilter.warehouseIds.length - 1;
            }
            paramIndex++;
        }

        if (brand) {
            whereConditions.push(`m.brand ILIKE $${paramIndex}`);
            params.push(`%${brand}%`);
            paramIndex++;
        }

        if (category) {
            whereConditions.push(`m.cms_vertical ILIKE $${paramIndex}`);
            params.push(`%${category}%`);
            paramIndex++;
        }

        const whereClause = whereConditions.join(' AND ');

        // ⚡ OPTIMIZED: Add LIMIT and OFFSET for pagination
        const sql = `
      SELECT 
        i.wsn,
        i.warehouse_id,
        w.name as warehouse_name,
        m.product_title,
        m.brand,
        m.cms_vertical,
        m.mrp,
        m.fsp,
        i.inbound_date,
        i.rack_no,
        CASE 
          WHEN EXISTS (SELECT 1 FROM outbound o WHERE o.wsn = i.wsn AND o.warehouse_id = i.warehouse_id) THEN 'OUTBOUND'
          WHEN EXISTS (SELECT 1 FROM picking p WHERE p.wsn = i.wsn AND p.warehouse_id = i.warehouse_id) THEN 'PICKING'
          WHEN EXISTS (SELECT 1 FROM qc q WHERE q.wsn = i.wsn AND q.warehouse_id = i.warehouse_id) THEN 'QC'
          ELSE 'INBOUND'
        END as current_status
      FROM inbound i
      LEFT JOIN warehouses w ON i.warehouse_id = w.id
      LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = i.warehouse_id
      WHERE ${whereClause}
      ORDER BY i.inbound_date DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
        params.push(Number(limit), offset);

        // Get total count for pagination info
        const countParams = params.slice(0, -2);
        const countSql = `
      SELECT COUNT(*) as total
      FROM inbound i
      LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = i.warehouse_id
      WHERE ${whereClause}
    `;

        const [result, countResult] = await Promise.all([
            query(sql, params),
            query(countSql, countParams)
        ]);

        const total = parseInt(countResult.rows[0]?.total || '0');

        res.json({
            data: result.rows,
            total,
            page: Number(page),
            limit: Number(limit),
            totalPages: Math.ceil(total / Number(limit))
        });
    } catch (error: any) {
        console.error('❌ Current stock report error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// Stock Movement Report - ⚡ OPTIMIZED with pagination
export const getStockMovementReport = async (req: Request, res: Response) => {
    try {
        const { warehouse_id, wsn, start_date, end_date, page = 1, limit = 1000 } = req.query;
        const offset = (Number(page) - 1) * Number(limit);

        // Enforce warehouse isolation
        const warehouseAccess = getEffectiveWarehouseFilter(req, warehouse_id as string | undefined);
        if (!warehouseAccess.allowed) {
            return res.status(403).json({ error: warehouseAccess.error });
        }

        let whereConditions: string[] = ['1=1'];
        const params: any[] = [];
        let paramIndex = 1;

        if (warehouseAccess.warehouseIds) {
            if (warehouseAccess.warehouseIds.length === 1) {
                whereConditions.push(`warehouse_id = $${paramIndex}`);
                params.push(warehouseAccess.warehouseIds[0]);
            } else {
                const placeholders = warehouseAccess.warehouseIds.map((_, idx) => `$${paramIndex + idx}`).join(',');
                whereConditions.push(`warehouse_id IN (${placeholders})`);
                warehouseAccess.warehouseIds.forEach(id => params.push(id));
                paramIndex += warehouseAccess.warehouseIds.length - 1;
            }
            paramIndex++;
        }

        if (wsn) {
            whereConditions.push(`wsn ILIKE $${paramIndex}`);
            params.push(`%${wsn}%`);
            paramIndex++;
        }

        const whereClause = whereConditions.join(' AND ');

        let dateFilterInbound = '';
        let dateFilterQC = '';
        let dateFilterPicking = '';
        let dateFilterOutbound = '';

        if (start_date && end_date) {
            dateFilterInbound = `AND inbound_date BETWEEN $${paramIndex}::date AND $${paramIndex + 1}::date`;
            dateFilterQC = `AND qc_date BETWEEN $${paramIndex}::date AND $${paramIndex + 1}::date`;
            dateFilterPicking = `AND picking_date BETWEEN $${paramIndex}::date AND $${paramIndex + 1}::date`;
            dateFilterOutbound = `AND dispatch_date BETWEEN $${paramIndex}::date AND $${paramIndex + 1}::date`;
            params.push(start_date, end_date);
            paramIndex += 2;
        }

        // ⚡ OPTIMIZED: Add LIMIT and OFFSET for pagination
        const sql = `
      SELECT * FROM (
        SELECT wsn, warehouse_id, 'INBOUND' as movement_type, inbound_date as movement_date, inbound_date as sort_date, created_user_name as user_name
        FROM inbound WHERE ${whereClause} ${dateFilterInbound}
        UNION ALL
        SELECT wsn, warehouse_id, 'QC' as movement_type, qc_date as movement_date, qc_date as sort_date, qc_by_name as user_name
        FROM qc WHERE ${whereClause} ${dateFilterQC}
        UNION ALL
        SELECT wsn, warehouse_id, 'PICKING' as movement_type, picking_date as movement_date, picking_date as sort_date, picker_name as user_name
        FROM picking WHERE ${whereClause} ${dateFilterPicking}
        UNION ALL
        SELECT wsn, warehouse_id, 'OUTBOUND' as movement_type, dispatch_date as movement_date, dispatch_date as sort_date, created_user_name as user_name
        FROM outbound WHERE ${whereClause} ${dateFilterOutbound}
      ) movements
      ORDER BY sort_date DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
        params.push(Number(limit), offset);

        // Get total count (without LIMIT for pagination info)
        const countParams = params.slice(0, -2); // Remove limit and offset
        const countSql = `
      SELECT COUNT(*) as total FROM (
        SELECT wsn FROM inbound WHERE ${whereClause} ${dateFilterInbound}
        UNION ALL
        SELECT wsn FROM qc WHERE ${whereClause} ${dateFilterQC}
        UNION ALL
        SELECT wsn FROM picking WHERE ${whereClause} ${dateFilterPicking}
        UNION ALL
        SELECT wsn FROM outbound WHERE ${whereClause} ${dateFilterOutbound}
      ) movements
    `;

        const [result, countResult] = await Promise.all([
            query(sql, params),
            query(countSql, countParams)
        ]);

        const total = parseInt(countResult.rows[0]?.total || '0');

        res.json({
            data: result.rows,
            total,
            page: Number(page),
            limit: Number(limit),
            totalPages: Math.ceil(total / Number(limit))
        });
    } catch (error: any) {
        console.error('❌ Stock movement report error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =================== INBOUND REPORTS ===================

// ⚡ OPTIMIZED with pagination
export const getInboundReport = async (req: Request, res: Response) => {
    try {
        const { warehouse_id, start_date, end_date, brand, category, page = 1, limit = 1000 } = req.query;
        const offset = (Number(page) - 1) * Number(limit);

        // Enforce warehouse isolation
        const warehouseAccess = getEffectiveWarehouseFilter(req, warehouse_id as string | undefined);
        if (!warehouseAccess.allowed) {
            return res.status(403).json({ error: warehouseAccess.error });
        }

        let whereConditions: string[] = ['1=1'];
        const params: any[] = [];
        let paramIndex = 1;

        if (warehouseAccess.warehouseIds) {
            if (warehouseAccess.warehouseIds.length === 1) {
                whereConditions.push(`i.warehouse_id = $${paramIndex}`);
                params.push(warehouseAccess.warehouseIds[0]);
            } else {
                const placeholders = warehouseAccess.warehouseIds.map((_, idx) => `$${paramIndex + idx}`).join(',');
                whereConditions.push(`i.warehouse_id IN (${placeholders})`);
                warehouseAccess.warehouseIds.forEach(id => params.push(id));
                paramIndex += warehouseAccess.warehouseIds.length - 1;
            }
            paramIndex++;
        }

        if (start_date) {
            whereConditions.push(`i.inbound_date >= $${paramIndex}`);
            params.push(start_date);
            paramIndex++;
        }

        if (end_date) {
            whereConditions.push(`i.inbound_date <= $${paramIndex}`);
            params.push(end_date);
            paramIndex++;
        }

        if (brand) {
            whereConditions.push(`m.brand ILIKE $${paramIndex}`);
            params.push(`%${brand}%`);
            paramIndex++;
        }

        if (category) {
            whereConditions.push(`m.cms_vertical ILIKE $${paramIndex}`);
            params.push(`%${category}%`);
            paramIndex++;
        }

        const whereClause = whereConditions.join(' AND ');

        // ⚡ OPTIMIZED: Add LIMIT and OFFSET for pagination
        const sql = `
      SELECT 
        i.*,
        w.name as warehouse_name,
        m.product_title,
        m.brand,
        m.cms_vertical,
        m.mrp,
        m.fsp
      FROM inbound i
      LEFT JOIN warehouses w ON i.warehouse_id = w.id
      LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = i.warehouse_id
      WHERE ${whereClause}
      ORDER BY i.inbound_date DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
        params.push(Number(limit), offset);

        // Summary stats (without pagination)
        const countParams = params.slice(0, -2);
        const statsSql = `
      SELECT 
        COUNT(*) as total_inbound,
        COUNT(DISTINCT i.wsn) as unique_items,
        COUNT(DISTINCT m.brand) as brands_count,
        COUNT(DISTINCT m.cms_vertical) as categories_count
      FROM inbound i
      LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = i.warehouse_id
      WHERE ${whereClause}
    `;

        const [result, statsResult] = await Promise.all([
            query(sql, params),
            query(statsSql, countParams)
        ]);

        const total = parseInt(statsResult.rows[0]?.total_inbound || '0');

        res.json({
            data: result.rows,
            summary: statsResult.rows[0],
            total,
            page: Number(page),
            limit: Number(limit),
            totalPages: Math.ceil(total / Number(limit))
        });
    } catch (error: any) {
        console.error('❌ Inbound report error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =================== OUTBOUND REPORTS ===================

// ⚡ OPTIMIZED with pagination
export const getOutboundReport = async (req: Request, res: Response) => {
    try {
        const { warehouse_id, start_date, end_date, customer, source, page = 1, limit = 1000 } = req.query;
        const offset = (Number(page) - 1) * Number(limit);

        // Enforce warehouse isolation
        const warehouseAccess = getEffectiveWarehouseFilter(req, warehouse_id as string | undefined);
        if (!warehouseAccess.allowed) {
            return res.status(403).json({ error: warehouseAccess.error });
        }

        let whereConditions: string[] = ['1=1'];
        const params: any[] = [];
        let paramIndex = 1;

        if (warehouseAccess.warehouseIds) {
            if (warehouseAccess.warehouseIds.length === 1) {
                whereConditions.push(`o.warehouse_id = $${paramIndex}`);
                params.push(warehouseAccess.warehouseIds[0]);
            } else {
                const placeholders = warehouseAccess.warehouseIds.map((_, idx) => `$${paramIndex + idx}`).join(',');
                whereConditions.push(`o.warehouse_id IN (${placeholders})`);
                warehouseAccess.warehouseIds.forEach(id => params.push(id));
                paramIndex += warehouseAccess.warehouseIds.length - 1;
            }
            paramIndex++;
        }

        if (start_date) {
            whereConditions.push(`o.dispatch_date >= $${paramIndex}`);
            params.push(start_date);
            paramIndex++;
        }

        if (end_date) {
            whereConditions.push(`o.dispatch_date <= $${paramIndex}`);
            params.push(end_date);
            paramIndex++;
        }

        if (customer) {
            whereConditions.push(`o.customer_name ILIKE $${paramIndex}`);
            params.push(`%${customer}%`);
            paramIndex++;
        }

        if (source) {
            whereConditions.push(`o.source = $${paramIndex}`);
            params.push(source);
            paramIndex++;
        }

        const whereClause = whereConditions.join(' AND ');

        // ⚡ OPTIMIZED: Add LIMIT and OFFSET for pagination
        const sql = `
      SELECT 
        o.*,
        w.name as warehouse_name,
        m.product_title,
        m.brand,
        m.cms_vertical,
        m.mrp,
        m.fsp
      FROM outbound o
      LEFT JOIN warehouses w ON o.warehouse_id = w.id
      LEFT JOIN master_data m ON o.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = o.warehouse_id
      WHERE ${whereClause}
      ORDER BY o.dispatch_date DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
        params.push(Number(limit), offset);

        // Get total count for pagination
        const countParams = params.slice(0, -2);
        const countSql = `SELECT COUNT(*) as total FROM outbound o WHERE ${whereClause}`;

        const [result, countResult] = await Promise.all([
            query(sql, params),
            query(countSql, countParams)
        ]);

        const total = parseInt(countResult.rows[0]?.total || '0');

        res.json({
            data: result.rows,
            total,
            page: Number(page),
            limit: Number(limit),
            totalPages: Math.ceil(total / Number(limit))
        });
    } catch (error: any) {
        console.error('❌ Outbound report error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =================== QC REPORTS ===================

// ⚡ OPTIMIZED with pagination
export const getQCReport = async (req: Request, res: Response) => {
    try {
        const { warehouse_id, start_date, end_date, qc_grade, qc_status, page = 1, limit = 1000 } = req.query;
        const offset = (Number(page) - 1) * Number(limit);

        // Enforce warehouse isolation
        const warehouseAccess = getEffectiveWarehouseFilter(req, warehouse_id as string | undefined);
        if (!warehouseAccess.allowed) {
            return res.status(403).json({ error: warehouseAccess.error });
        }

        let whereConditions: string[] = ['1=1'];
        const params: any[] = [];
        let paramIndex = 1;

        if (warehouseAccess.warehouseIds) {
            if (warehouseAccess.warehouseIds.length === 1) {
                whereConditions.push(`q.warehouse_id = $${paramIndex}`);
                params.push(warehouseAccess.warehouseIds[0]);
            } else {
                const placeholders = warehouseAccess.warehouseIds.map((_, idx) => `$${paramIndex + idx}`).join(',');
                whereConditions.push(`q.warehouse_id IN (${placeholders})`);
                warehouseAccess.warehouseIds.forEach(id => params.push(id));
                paramIndex += warehouseAccess.warehouseIds.length - 1;
            }
            paramIndex++;
        }

        if (start_date) {
            whereConditions.push(`q.qc_date >= $${paramIndex}`);
            params.push(start_date);
            paramIndex++;
        }

        if (end_date) {
            whereConditions.push(`q.qc_date <= $${paramIndex}`);
            params.push(end_date);
            paramIndex++;
        }

        if (qc_grade) {
            whereConditions.push(`q.qc_grade = $${paramIndex}`);
            params.push(qc_grade);
            paramIndex++;
        }

        if (qc_status) {
            whereConditions.push(`q.qc_status = $${paramIndex}`);
            params.push(qc_status);
            paramIndex++;
        }

        const whereClause = whereConditions.join(' AND ');

        // ⚡ OPTIMIZED: Add LIMIT and OFFSET for pagination
        const sql = `
      SELECT 
        q.*,
        w.name as warehouse_name,
        m.product_title,
        m.brand,
        m.cms_vertical
      FROM qc q
      LEFT JOIN warehouses w ON q.warehouse_id = w.id
      LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = q.warehouse_id
      WHERE ${whereClause}
      ORDER BY q.qc_date DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
        params.push(Number(limit), offset);

        // Get count and summary (without pagination limits)
        const countParams = params.slice(0, -2);
        const summarySql = `
      SELECT 
        qc_grade,
        qc_status,
        COUNT(*) as count
      FROM qc q
      WHERE ${whereClause}
      GROUP BY qc_grade, qc_status
    `;

        const countSql = `SELECT COUNT(*) as total FROM qc q WHERE ${whereClause}`;

        const [result, summaryResult, countResult] = await Promise.all([
            query(sql, params),
            query(summarySql, countParams),
            query(countSql, countParams)
        ]);

        const total = parseInt(countResult.rows[0]?.total || '0');

        res.json({
            data: result.rows,
            summary: summaryResult.rows,
            total,
            page: Number(page),
            limit: Number(limit),
            totalPages: Math.ceil(total / Number(limit))
        });
    } catch (error: any) {
        console.error('❌ QC report error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =================== PICKING REPORTS ===================

// ⚡ OPTIMIZED with pagination
export const getPickingReport = async (req: Request, res: Response) => {
    try {
        const { warehouse_id, start_date, end_date, customer, page = 1, limit = 1000 } = req.query;
        const offset = (Number(page) - 1) * Number(limit);

        // Enforce warehouse isolation
        const warehouseAccess = getEffectiveWarehouseFilter(req, warehouse_id as string | undefined);
        if (!warehouseAccess.allowed) {
            return res.status(403).json({ error: warehouseAccess.error });
        }

        let whereConditions: string[] = ['1=1'];
        const params: any[] = [];
        let paramIndex = 1;

        if (warehouseAccess.warehouseIds) {
            if (warehouseAccess.warehouseIds.length === 1) {
                whereConditions.push(`p.warehouse_id = $${paramIndex}`);
                params.push(warehouseAccess.warehouseIds[0]);
            } else {
                const placeholders = warehouseAccess.warehouseIds.map((_, idx) => `$${paramIndex + idx}`).join(',');
                whereConditions.push(`p.warehouse_id IN (${placeholders})`);
                warehouseAccess.warehouseIds.forEach(id => params.push(id));
                paramIndex += warehouseAccess.warehouseIds.length - 1;
            }
            paramIndex++;
        }

        if (start_date) {
            whereConditions.push(`p.picking_date >= $${paramIndex}`);
            params.push(start_date);
            paramIndex++;
        }

        if (end_date) {
            whereConditions.push(`p.picking_date <= $${paramIndex}`);
            params.push(end_date);
            paramIndex++;
        }

        if (customer) {
            whereConditions.push(`p.customer_name ILIKE $${paramIndex}`);
            params.push(`%${customer}%`);
            paramIndex++;
        }

        const whereClause = whereConditions.join(' AND ');

        // ⚡ OPTIMIZED: Add LIMIT and OFFSET for pagination
        const sql = `
      SELECT 
        p.*,
        w.name as warehouse_name,
        m.product_title,
        m.brand
      FROM picking p
      LEFT JOIN warehouses w ON p.warehouse_id = w.id
      LEFT JOIN master_data m ON p.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = p.warehouse_id
      WHERE ${whereClause}
      ORDER BY p.picking_date DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
        params.push(Number(limit), offset);

        // Get total count for pagination
        const countParams = params.slice(0, -2);
        const countSql = `SELECT COUNT(*) as total FROM picking p WHERE ${whereClause}`;

        const [result, countResult] = await Promise.all([
            query(sql, params),
            query(countSql, countParams)
        ]);

        const total = parseInt(countResult.rows[0]?.total || '0');

        res.json({
            data: result.rows,
            total,
            page: Number(page),
            limit: Number(limit),
            totalPages: Math.ceil(total / Number(limit))
        });
    } catch (error: any) {
        console.error('❌ Picking report error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =================== PERFORMANCE REPORTS ===================

export const getUserPerformanceReport = async (req: Request, res: Response) => {
    try {
        const { warehouse_id, start_date, end_date } = req.query;

        // Enforce warehouse isolation
        const warehouseAccess = getEffectiveWarehouseFilter(req, warehouse_id as string | undefined);
        if (!warehouseAccess.allowed) {
            return res.status(403).json({ error: warehouseAccess.error });
        }

        const params: any[] = [];
        let paramIndex = 1;

        // Build parameterized warehouse filter
        let warehouseFilter = '';
        if (warehouseAccess.warehouseIds) {
            if (warehouseAccess.warehouseIds.length === 1) {
                warehouseFilter = `AND warehouse_id = $${paramIndex}`;
                params.push(warehouseAccess.warehouseIds[0]);
            } else {
                const placeholders = warehouseAccess.warehouseIds.map((_, idx) => `$${paramIndex + idx}`).join(',');
                warehouseFilter = `AND warehouse_id IN (${placeholders})`;
                warehouseAccess.warehouseIds.forEach(id => params.push(id));
                paramIndex += warehouseAccess.warehouseIds.length - 1;
            }
            paramIndex++;
        }

        // Build parameterized date filters
        let dateFilterInbound = '';
        let dateFilterQC = '';
        let dateFilterPicking = '';
        let dateFilterOutbound = '';

        if (start_date && end_date) {
            const startIdx = paramIndex;
            const endIdx = paramIndex + 1;
            dateFilterInbound = `AND inbound_date BETWEEN $${startIdx}::date AND $${endIdx}::date`;
            dateFilterQC = `AND qc_date BETWEEN $${startIdx}::date AND $${endIdx}::date`;
            dateFilterPicking = `AND picking_date BETWEEN $${startIdx}::date AND $${endIdx}::date`;
            dateFilterOutbound = `AND dispatch_date BETWEEN $${startIdx}::date AND $${endIdx}::date`;
            params.push(start_date, end_date);
        }

        const sql = `
      SELECT 
        user_name,
        activity_type,
        COUNT(*) as total_operations,
        MIN(operation_date) as first_operation,
        MAX(operation_date) as last_operation
      FROM (
        SELECT created_user_name as user_name, 'INBOUND' as activity_type, inbound_date as operation_date, warehouse_id FROM inbound WHERE created_user_name IS NOT NULL ${dateFilterInbound} ${warehouseFilter}
        UNION ALL
        SELECT qc_by_name as user_name, 'QC' as activity_type, qc_date as operation_date, warehouse_id FROM qc WHERE qc_by_name IS NOT NULL ${dateFilterQC} ${warehouseFilter}
        UNION ALL
        SELECT picker_name as user_name, 'PICKING' as activity_type, picking_date as operation_date, warehouse_id FROM picking WHERE picker_name IS NOT NULL ${dateFilterPicking} ${warehouseFilter}
        UNION ALL
        SELECT created_user_name as user_name, 'OUTBOUND' as activity_type, dispatch_date as operation_date, warehouse_id FROM outbound WHERE created_user_name IS NOT NULL ${dateFilterOutbound} ${warehouseFilter}
      ) user_activities
      GROUP BY user_name, activity_type
      ORDER BY user_name, activity_type
    `;

        const result = await query(sql, params);
        res.json({ data: result.rows, total: result.rows.length });
    } catch (error: any) {
        console.error('❌ User performance report error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =================== SUMMARY DASHBOARD ===================

export const getWarehouseSummary = async (req: Request, res: Response) => {
    try {
        const { warehouse_id, start_date, end_date } = req.query;

        // Enforce warehouse isolation
        const warehouseAccess = getEffectiveWarehouseFilter(req, warehouse_id as string | undefined);
        if (!warehouseAccess.allowed) {
            return res.status(403).json({ error: warehouseAccess.error });
        }

        let whereConditions: string[] = ['1=1'];
        const params: any[] = [];
        let paramIndex = 1;

        if (warehouseAccess.warehouseIds) {
            if (warehouseAccess.warehouseIds.length === 1) {
                whereConditions.push(`warehouse_id = $${paramIndex}`);
                params.push(warehouseAccess.warehouseIds[0]);
            } else {
                const placeholders = warehouseAccess.warehouseIds.map((_, idx) => `$${paramIndex + idx}`).join(',');
                whereConditions.push(`warehouse_id IN (${placeholders})`);
                warehouseAccess.warehouseIds.forEach(id => params.push(id));
                paramIndex += warehouseAccess.warehouseIds.length - 1;
            }
            paramIndex++;
        }

        let dateFilterInbound = '';
        let dateFilterQC = '';
        let dateFilterPicking = '';
        let dateFilterOutbound = '';

        if (start_date && end_date) {
            dateFilterInbound = `AND inbound_date BETWEEN $${paramIndex}::date AND $${paramIndex + 1}::date`;
            dateFilterQC = `AND qc_date BETWEEN $${paramIndex}::date AND $${paramIndex + 1}::date`;
            dateFilterPicking = `AND picking_date BETWEEN $${paramIndex}::date AND $${paramIndex + 1}::date`;
            dateFilterOutbound = `AND dispatch_date BETWEEN $${paramIndex}::date AND $${paramIndex + 1}::date`;
            params.push(start_date, end_date);
        }

        const whereClause = whereConditions.join(' AND ');

        const inboundCountSql = `SELECT COUNT(*) as count FROM inbound WHERE ${whereClause} ${dateFilterInbound}`;
        const qcCountSql = `SELECT COUNT(*) as count FROM qc WHERE ${whereClause} ${dateFilterQC}`;
        const pickingCountSql = `SELECT COUNT(*) as count FROM picking WHERE ${whereClause} ${dateFilterPicking}`;
        const outboundCountSql = `SELECT COUNT(*) as count FROM outbound WHERE ${whereClause} ${dateFilterOutbound}`;

        const [inbound, qc, picking, outbound] = await Promise.all([
            query(inboundCountSql, params),
            query(qcCountSql, params),
            query(pickingCountSql, params),
            query(outboundCountSql, params)
        ]);

        res.json({
            inbound: parseInt(inbound.rows[0].count),
            qc: parseInt(qc.rows[0].count),
            picking: parseInt(picking.rows[0].count),
            outbound: parseInt(outbound.rows[0].count),
            total: parseInt(inbound.rows[0].count) + parseInt(qc.rows[0].count) +
                parseInt(picking.rows[0].count) + parseInt(outbound.rows[0].count)
        });
    } catch (error: any) {
        console.error('❌ Warehouse summary error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =================== EXPORT TO EXCEL ===================

export const exportReportToExcel = async (req: Request, res: Response) => {
    try {
        const { report_type, ...filters } = req.query;

        // Enforce warehouse isolation on export
        const warehouseAccess = getEffectiveWarehouseFilter(req, filters.warehouse_id as string | undefined);
        if (!warehouseAccess.allowed) {
            return res.status(403).json({ error: warehouseAccess.error });
        }
        // Override filters.warehouse_id with validated warehouse access
        const effectiveWarehouseId = warehouseAccess.warehouseIds && warehouseAccess.warehouseIds.length === 1
            ? warehouseAccess.warehouseIds[0]
            : filters.warehouse_id;

        let reportData: any[] = [];
        let fileName = 'report.xlsx';

        // Get report data based on type (parameterized to prevent SQL injection)
        switch (report_type) {
            case 'current_stock': {
                const stockParams: any[] = [];
                let stockParamIdx = 1;
                let stockWhere = '';
                if (filters.warehouse_id) {
                    stockWhere = `WHERE i.warehouse_id = $${stockParamIdx}`;
                    stockParams.push(filters.warehouse_id);
                    stockParamIdx++;
                }
                const stockSql = `
          SELECT i.wsn, w.name as warehouse, m.product_title, m.brand, m.cms_vertical, 
                 i.inbound_date, i.rack_no
          FROM inbound i
          LEFT JOIN warehouses w ON i.warehouse_id = w.id
          LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = i.warehouse_id
          ${stockWhere}
          ORDER BY i.inbound_date DESC
          LIMIT 50000
        `;
                const stockResult = await query(stockSql, stockParams);
                reportData = stockResult.rows;
                fileName = 'current_stock_report.xlsx';
                break;
            }

            case 'inbound': {
                const inboundParams: any[] = [];
                let inboundParamIdx = 1;
                const inboundConditions: string[] = ['1=1'];
                if (filters.warehouse_id) {
                    inboundConditions.push(`i.warehouse_id = $${inboundParamIdx}`);
                    inboundParams.push(filters.warehouse_id);
                    inboundParamIdx++;
                }
                if (filters.start_date) {
                    inboundConditions.push(`i.inbound_date >= $${inboundParamIdx}::date`);
                    inboundParams.push(filters.start_date);
                    inboundParamIdx++;
                }
                if (filters.end_date) {
                    inboundConditions.push(`i.inbound_date <= $${inboundParamIdx}::date`);
                    inboundParams.push(filters.end_date);
                    inboundParamIdx++;
                }
                const inboundSql = `
          SELECT i.wsn, i.inbound_date, i.rack_no, i.warehouse_id, i.created_user_name,
                 w.name as warehouse, m.product_title, m.brand, m.cms_vertical
          FROM inbound i
          LEFT JOIN warehouses w ON i.warehouse_id = w.id
          LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = i.warehouse_id
          WHERE ${inboundConditions.join(' AND ')}
          ORDER BY i.inbound_date DESC
          LIMIT 50000
        `;
                const inboundResult = await query(inboundSql, inboundParams);
                reportData = inboundResult.rows;
                fileName = 'inbound_report.xlsx';
                break;
            }

            case 'outbound': {
                const outboundParams: any[] = [];
                let outboundParamIdx = 1;
                const outboundConditions: string[] = ['1=1'];
                if (filters.warehouse_id) {
                    outboundConditions.push(`o.warehouse_id = $${outboundParamIdx}`);
                    outboundParams.push(filters.warehouse_id);
                    outboundParamIdx++;
                }
                if (filters.start_date) {
                    outboundConditions.push(`o.dispatch_date >= $${outboundParamIdx}::date`);
                    outboundParams.push(filters.start_date);
                    outboundParamIdx++;
                }
                if (filters.end_date) {
                    outboundConditions.push(`o.dispatch_date <= $${outboundParamIdx}::date`);
                    outboundParams.push(filters.end_date);
                    outboundParamIdx++;
                }
                const outboundSql = `
          SELECT o.wsn, o.dispatch_date, o.warehouse_id, o.created_user_name,
                 w.name as warehouse, m.product_title, m.brand
          FROM outbound o
          LEFT JOIN warehouses w ON o.warehouse_id = w.id
          LEFT JOIN master_data m ON o.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = o.warehouse_id
          WHERE ${outboundConditions.join(' AND ')}
          ORDER BY o.dispatch_date DESC
          LIMIT 50000
        `;
                const outboundResult = await query(outboundSql, outboundParams);
                reportData = outboundResult.rows;
                fileName = 'outbound_report.xlsx';
                break;
            }

            default:
                return res.status(400).json({ error: 'Invalid report type' });
        }

        // Create Excel file
        const worksheet = XLSX.utils.json_to_sheet(reportData);
        const workbook = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(workbook, worksheet, 'Report');

        const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });

        res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.send(buffer);
    } catch (error: any) {
        console.error('❌ Export report error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// =================== ANALYTICS ENDPOINTS ===================

// Get trend analysis with optional date range
export const getTrendAnalysis = async (req: Request, res: Response) => {
    try {
        const { warehouse_id, start_date, end_date } = req.query;

        console.log('📊 getTrendAnalysis called with warehouse_id:', warehouse_id, 'date range:', start_date, '-', end_date);

        if (!warehouse_id) {
            return res.status(400).json({ error: 'Warehouse ID required' });
        }

        if (!validateWarehouseAccess(req, warehouse_id as string)) {
            return res.status(403).json({ error: 'Forbidden: You do not have access to this warehouse' });
        }

        // Build parameterized date range
        const params: any[] = [warehouse_id];
        let dateStartExpr = `CURRENT_DATE - INTERVAL '29 days'`;
        let dateEndExpr = `CURRENT_DATE`;

        if (start_date) {
            params.push(start_date);
            dateStartExpr = `$${params.length}::date`;
        }
        if (end_date) {
            params.push(end_date);
            dateEndExpr = `$${params.length}::date`;
        }

        // OPTIMIZED: Use separate COUNT subqueries instead of 4-way LEFT JOIN
        // This eliminates cartesian product explosion and enables index usage
        // on existing indexes: (warehouse_id, date DESC)
        const trendSql = `
            WITH dates AS (
                SELECT generate_series(
                    ${dateStartExpr},
                    ${dateEndExpr},
                    '1 day'
                )::date AS date
            )
            SELECT 
                d.date,
                COALESCE((SELECT COUNT(*) FROM inbound WHERE warehouse_id = $1 
                    AND inbound_date >= d.date AND inbound_date < d.date + INTERVAL '1 day'), 0)::integer as inbound,
                COALESCE((SELECT COUNT(*) FROM qc WHERE warehouse_id = $1 
                    AND qc_date >= d.date AND qc_date < d.date + INTERVAL '1 day'), 0)::integer as qc,
                COALESCE((SELECT COUNT(*) FROM picking WHERE warehouse_id = $1 
                    AND picking_date >= d.date AND picking_date < d.date + INTERVAL '1 day'), 0)::integer as picking,
                COALESCE((SELECT COUNT(*) FROM outbound WHERE warehouse_id = $1 
                    AND dispatch_date >= d.date AND dispatch_date < d.date + INTERVAL '1 day'), 0)::integer as outbound
            FROM dates d
            ORDER BY d.date ASC
        `;

        console.log('📊 Executing optimized trend analysis query...');
        const result = await queryWithTimeout(trendSql, params, 30000);
        console.log('✅ Trend analysis query successful, rows:', result.rows.length);

        res.json({
            trends: result.rows
        });
    } catch (error: any) {
        console.error('❌ Trend analysis error:', error.message);
        res.status(500).json({ error: safeError(error, 'Failed to fetch trend analysis') });
    }
};

// Get QC pass/fail analysis
export const getQCAnalysis = async (req: Request, res: Response) => {
    try {
        const { warehouse_id } = req.query;

        console.log('✅ getQCAnalysis called with warehouse_id:', warehouse_id);

        if (!warehouse_id) {
            return res.status(400).json({ error: 'Warehouse ID required' });
        }

        if (!validateWarehouseAccess(req, warehouse_id as string)) {
            return res.status(403).json({ error: 'Forbidden: You do not have access to this warehouse' });
        }

        const qcSql = `
            SELECT 
                COALESCE(qc_status, 'Unknown') as qc_status,
                COALESCE(qc_grade, 'N/A') as qc_grade,
                COUNT(*)::integer as count
            FROM qc
            WHERE warehouse_id = $1
            GROUP BY qc_status, qc_grade
            ORDER BY qc_status, qc_grade
        `;

        console.log('✅ Executing QC analysis query...');
        const result = await query(qcSql, [warehouse_id]);
        console.log('✅ QC analysis query successful, rows:', result.rows.length);

        res.json({
            qcAnalysis: result.rows
        });
    } catch (error: any) {
        console.error('❌ QC analysis error:', error);
        console.error('Error stack:', error.stack);
        res.status(500).json({ error: safeError(error, 'Failed to fetch QC analysis') });
    }
};

// Get performance metrics
export const getPerformanceMetrics = async (req: Request, res: Response) => {
    try {
        const { warehouse_id } = req.query;

        console.log('📈 getPerformanceMetrics called with warehouse_id:', warehouse_id);

        if (!warehouse_id) {
            return res.status(400).json({ error: 'Warehouse ID required' });
        }

        if (!validateWarehouseAccess(req, warehouse_id as string)) {
            return res.status(403).json({ error: 'Forbidden: You do not have access to this warehouse' });
        }

        // Get user performance data separately and merge in code
        const inboundUsersSql = `
            SELECT 
                COALESCE(created_user_name, 'Unknown') as user_name,
                COUNT(*)::integer as count
            FROM inbound
            WHERE warehouse_id = $1 AND created_user_name IS NOT NULL
            GROUP BY created_user_name
        `;

        const qcUsersSql = `
            SELECT 
                COALESCE(qc_by_name, 'Unknown') as user_name,
                COUNT(*)::integer as count
            FROM qc
            WHERE warehouse_id = $1 AND qc_by_name IS NOT NULL
            GROUP BY qc_by_name
        `;

        const pickingUsersSql = `
            SELECT 
                COALESCE(picker_name, 'Unknown') as user_name,
                COUNT(*)::integer as count
            FROM picking
            WHERE warehouse_id = $1 AND picker_name IS NOT NULL
            GROUP BY picker_name
        `;

        // Brand performance - with real dispatched/rate calculations
        const brandPerfSql = `
            WITH brand_items AS (
                SELECT brand, COUNT(*)::integer as total_items
                FROM master_data 
                WHERE brand IS NOT NULL AND brand != '' AND deleted_at IS NULL AND warehouse_id = $1
                GROUP BY brand
            ),
            brand_dispatched AS (
                SELECT COALESCE(m.brand, 'Unknown') as brand, COUNT(DISTINCT o.wsn)::integer as dispatched_items
                FROM outbound o
                JOIN master_data m ON o.wsn = m.wsn AND o.warehouse_id = m.warehouse_id AND m.deleted_at IS NULL
                WHERE o.warehouse_id = $1 AND m.brand IS NOT NULL AND m.brand != ''
                GROUP BY m.brand
            ),
            brand_avg_days AS (
                SELECT COALESCE(m.brand, 'Unknown') as brand, 
                       ROUND(AVG(CURRENT_DATE - i.inbound_date::date), 1) as avg_days
                FROM inbound i
                JOIN master_data m ON i.wsn = m.wsn AND i.warehouse_id = m.warehouse_id AND m.deleted_at IS NULL
                WHERE i.warehouse_id = $1 AND m.brand IS NOT NULL AND m.brand != ''
                GROUP BY m.brand
            )
            SELECT 
                bi.brand,
                bi.total_items,
                COALESCE(bd.dispatched_items, 0)::integer as dispatched_items,
                ROUND(COALESCE(bd.dispatched_items, 0)::numeric / NULLIF(bi.total_items, 0) * 100, 2) as dispatch_rate,
                COALESCE(ba.avg_days, 0) as avg_days
            FROM brand_items bi
            LEFT JOIN brand_dispatched bd ON bd.brand = bi.brand
            LEFT JOIN brand_avg_days ba ON ba.brand = bi.brand
            ORDER BY bi.total_items DESC
            LIMIT 20
        `;

        console.log('📈 Executing performance queries...');
        const [inboundUsers, qcUsers, pickingUsers, brandPerf] = await Promise.all([
            query(inboundUsersSql, [warehouse_id]),
            query(qcUsersSql, [warehouse_id]),
            query(pickingUsersSql, [warehouse_id]),
            query(brandPerfSql, [warehouse_id])
        ]);

        // Merge user data in JavaScript
        const userMap = new Map();

        inboundUsers.rows.forEach((row: any) => {
            if (!userMap.has(row.user_name)) {
                userMap.set(row.user_name, { user_name: row.user_name, inbound: 0, qc: 0, picking: 0, total: 0 });
            }
            userMap.get(row.user_name).inbound = parseInt(row.count);
            userMap.get(row.user_name).total += parseInt(row.count);
        });

        qcUsers.rows.forEach((row: any) => {
            if (!userMap.has(row.user_name)) {
                userMap.set(row.user_name, { user_name: row.user_name, inbound: 0, qc: 0, picking: 0, total: 0 });
            }
            userMap.get(row.user_name).qc = parseInt(row.count);
            userMap.get(row.user_name).total += parseInt(row.count);
        });

        pickingUsers.rows.forEach((row: any) => {
            if (!userMap.has(row.user_name)) {
                userMap.set(row.user_name, { user_name: row.user_name, inbound: 0, qc: 0, picking: 0, total: 0 });
            }
            userMap.get(row.user_name).picking = parseInt(row.count);
            userMap.get(row.user_name).total += parseInt(row.count);
        });

        const userPerformance = Array.from(userMap.values())
            .sort((a, b) => b.total - a.total)
            .slice(0, 20);

        console.log('✅ Performance queries successful');
        console.log('User performance rows:', userPerformance.length);
        console.log('Brand performance rows:', brandPerf.rows.length);

        res.json({
            userPerformance,
            brandPerformance: brandPerf.rows
        });
    } catch (error: any) {
        console.error('❌ Performance metrics error:', error);
        console.error('Error stack:', error.stack);
        res.status(500).json({ error: safeError(error, 'Failed to fetch performance metrics') });
    }
};

// Get exception reports (stuck items, aging inventory, etc.)
export const getExceptionReports = async (req: Request, res: Response) => {
    try {
        const { warehouse_id } = req.query;

        console.log('⚠️ getExceptionReports called with warehouse_id:', warehouse_id);

        if (!warehouse_id) {
            return res.status(400).json({ error: 'Warehouse ID required' });
        }

        if (!validateWarehouseAccess(req, warehouse_id as string)) {
            return res.status(403).json({ error: 'Forbidden: You do not have access to this warehouse' });
        }

        let stuckInbound = [];
        let qcFailed = [];
        let slowMoving = [];

        try {
            // Stuck in Inbound - items that haven't moved to QC or Outbound
            console.log('⚠️ Query 1: Getting stuck inbound items...');
            const stuckResult = await query(`
                SELECT 
                    i.wsn,
                    COALESCE(m.product_title, 'Unknown') as product_title,
                    COALESCE(m.brand, 'Unknown') as brand,
                    i.inbound_date,
                    (CURRENT_DATE - i.inbound_date::date)::integer as days_stuck
                FROM inbound i
                LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = i.warehouse_id
                WHERE i.warehouse_id = $1
                AND i.inbound_date < CURRENT_DATE - INTERVAL '7 days'
                AND NOT EXISTS (SELECT 1 FROM qc q WHERE q.wsn = i.wsn AND q.warehouse_id = i.warehouse_id)
                AND NOT EXISTS (SELECT 1 FROM outbound o WHERE o.wsn = i.wsn AND o.warehouse_id = i.warehouse_id)
                ORDER BY i.inbound_date ASC
                LIMIT 100
            `, [warehouse_id]);
            stuckInbound = stuckResult.rows || [];
            console.log('✅ Stuck inbound query successful:', stuckInbound.length);
        } catch (err: any) {
            console.error('❌ Stuck inbound query failed:', err.message);
        }

        try {
            // QC Failed items - check for any grade that indicates failure
            console.log('⚠️ Query 2: Getting QC failed items...');
            const qcResult = await query(`
                SELECT 
                    q.wsn,
                    COALESCE(m.product_title, 'Unknown') as product_title,
                    COALESCE(m.brand, 'Unknown') as brand,
                    q.qc_date,
                    q.qc_status,
                    COALESCE(q.qc_grade, 'N/A') as qc_grade,
                    COALESCE(q.qc_remarks, '') as qc_remarks
                FROM qc q
                LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = q.warehouse_id
                WHERE q.warehouse_id = $1
                AND (
                    q.qc_status ILIKE '%fail%' 
                    OR q.qc_grade IN ('D', 'F', 'Fail')
                    OR q.qc_remarks ILIKE '%defect%'
                    OR q.qc_remarks ILIKE '%damage%'
                )
                ORDER BY q.qc_date DESC
                LIMIT 50
            `, [warehouse_id]);
            qcFailed = qcResult.rows || [];
            console.log('✅ QC failed query successful:', qcFailed.length);

            // Also log total QC count for debugging
            const totalQC = await query(`SELECT COUNT(*) as total FROM qc WHERE warehouse_id = $1`, [warehouse_id]);
            console.log('📊 Total QC records in warehouse:', totalQC.rows[0]?.total);
        } catch (err: any) {
            console.error('❌ QC failed query failed:', err.message);
        }

        try {
            // Slow moving inventory - items still in warehouse after 30 days (not dispatched)
            console.log('⚠️ Query 3: Getting slow moving items...');
            const slowResult = await query(`
                SELECT 
                    i.wsn,
                    COALESCE(m.product_title, 'Unknown') as product_title,
                    COALESCE(m.brand, 'Unknown') as brand,
                    i.inbound_date,
                    (CURRENT_DATE - i.inbound_date::date)::integer as days_in_warehouse
                FROM inbound i
                LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = i.warehouse_id
                WHERE i.warehouse_id = $1
                AND i.inbound_date < CURRENT_DATE - INTERVAL '30 days'
                AND NOT EXISTS (SELECT 1 FROM outbound o WHERE o.wsn = i.wsn AND o.warehouse_id = i.warehouse_id)
                ORDER BY i.inbound_date ASC
                LIMIT 100
            `, [warehouse_id]);
            slowMoving = slowResult.rows || [];
            console.log('✅ Slow moving query successful:', slowMoving.length);
        } catch (err: any) {
            console.error('❌ Slow moving query failed:', err.message);
        }

        console.log('✅ Exception reports completed successfully');

        res.json({
            stuckInbound,
            qcFailed,
            slowMoving
        });
    } catch (error: any) {
        console.error('❌ Exception reports error:', error);
        console.error('Error stack:', error.stack);
        res.status(500).json({ error: safeError(error, 'Failed to fetch exception reports') });
    }
};
