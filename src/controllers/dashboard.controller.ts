// File Path = warehouse-backend/src/controllers/dashboard.controller.ts
import { Request, Response } from 'express';
import { query } from '../config/database';
import { safeError } from '../utils/sanitizeError';

// Get complete inventory pipeline with all stages - OPTIMIZED for 1M+ rows
export const getInventoryPipeline = async (req: Request, res: Response) => {
  try {
    const { warehouseId, page = 1, limit = 50, search, stage, availableOnly, brand, category, dateFrom, dateTo } = req.query;

    // Validate warehouse access - get accessible warehouses from middleware
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;

    // If user has warehouse restrictions, validate the requested warehouse
    if (accessibleWarehouses && accessibleWarehouses.length > 0 && warehouseId) {
      const requestedId = parseInt(warehouseId as string);
      if (!accessibleWarehouses.includes(requestedId)) {
        return res.status(403).json({ error: 'Access denied to this warehouse' });
      }
    }

    if (!warehouseId) {
      return res.status(400).json({ error: "Warehouse ID required" });
    }

    const offset = (Number(page) - 1) * Number(limit);

    // Determine which JOINs are actually needed
    const needsMasterJoin = Boolean(search || brand || category);
    const needsStageJoins = Boolean(stage && stage !== 'all') || availableOnly === 'true';

    const conditions: string[] = ["i.warehouse_id = $1"];
    const countConditions: string[] = ["i.warehouse_id = $1"];
    const params: any[] = [warehouseId];
    const countParams: any[] = [warehouseId];
    let paramIndex = 2;
    let countParamIndex = 2;

    /* 🔎 SEARCH */
    if (search) {
      conditions.push(`(i.wsn ILIKE $${paramIndex} OR m.product_title ILIKE $${paramIndex})`);
      countConditions.push(`(i.wsn ILIKE $${countParamIndex} OR m.product_title ILIKE $${countParamIndex})`);
      params.push(`%${search}%`);
      countParams.push(`%${search}%`);
      paramIndex++;
      countParamIndex++;
    }

    /* 🏷 BRAND */
    if (brand) {
      conditions.push(`m.brand = $${paramIndex}`);
      countConditions.push(`m.brand = $${countParamIndex}`);
      params.push(brand);
      countParams.push(brand);
      paramIndex++;
      countParamIndex++;
    }

    /* 📂 CATEGORY */
    if (category) {
      conditions.push(`m.cms_vertical = $${paramIndex}`);
      countConditions.push(`m.cms_vertical = $${countParamIndex}`);
      params.push(category);
      countParams.push(category);
      paramIndex++;
      countParamIndex++;
    }

    /* 📅 DATE FILTERS */
    if (dateFrom && dateTo) {
      conditions.push(`i.inbound_date BETWEEN $${paramIndex}::date AND $${paramIndex + 1}::date`);
      countConditions.push(`i.inbound_date BETWEEN $${countParamIndex}::date AND $${countParamIndex + 1}::date`);
      params.push(dateFrom, dateTo);
      countParams.push(dateFrom, dateTo);
      paramIndex += 2;
      countParamIndex += 2;
    } else if (dateFrom) {
      conditions.push(`i.inbound_date >= $${paramIndex}::date`);
      countConditions.push(`i.inbound_date >= $${countParamIndex}::date`);
      params.push(dateFrom);
      countParams.push(dateFrom);
      paramIndex++;
      countParamIndex++;
    } else if (dateTo) {
      conditions.push(`i.inbound_date <= $${paramIndex}::date`);
      countConditions.push(`i.inbound_date <= $${countParamIndex}::date`);
      params.push(dateTo);
      countParams.push(dateTo);
      paramIndex++;
      countParamIndex++;
    }

    /* 📦 AVAILABLE ONLY */
    if (availableOnly === 'true') {
      conditions.push(`NOT EXISTS (SELECT 1 FROM outbound WHERE outbound.wsn = i.wsn AND outbound.warehouse_id = i.warehouse_id AND outbound.dispatch_date IS NOT NULL)`);
      countConditions.push(`NOT EXISTS (SELECT 1 FROM outbound WHERE outbound.wsn = i.wsn AND outbound.warehouse_id = i.warehouse_id AND outbound.dispatch_date IS NOT NULL)`);
    }

    /* 🔥 STAGE FILTER */
    let stageCondition = '';
    if (stage && stage !== "all") {
      if (stage === 'inbound') {
        // All items (no additional filter needed)
      } else if (stage === 'qc') {
        stageCondition = `AND EXISTS (SELECT 1 FROM qc WHERE qc.wsn = i.wsn AND qc.warehouse_id = i.warehouse_id AND qc.qc_grade IS NOT NULL)`;
      } else if (stage === 'picking') {
        stageCondition = `AND EXISTS (SELECT 1 FROM picking WHERE picking.wsn = i.wsn AND picking.warehouse_id = i.warehouse_id)`;
      } else if (stage === 'dispatched') {
        stageCondition = `AND EXISTS (SELECT 1 FROM outbound WHERE outbound.wsn = i.wsn AND outbound.warehouse_id = i.warehouse_id AND outbound.dispatch_date IS NOT NULL)`;
      }
    }

    const whereClause = conditions.join(" AND ");
    const countWhereClause = countConditions.join(" AND ");

    /* OPTIMIZED: Run count and ID queries in PARALLEL */
    const countSql = needsMasterJoin
      ? `SELECT COUNT(*) FROM inbound i LEFT JOIN master_data m ON m.wsn = i.wsn AND m.deleted_at IS NULL WHERE ${countWhereClause} ${stageCondition}`
      : `SELECT COUNT(*) FROM inbound i WHERE ${countWhereClause} ${stageCondition}`;

    const idsSql = `
      SELECT i.id
      FROM inbound i
      ${needsMasterJoin ? 'LEFT JOIN master_data m ON m.wsn = i.wsn AND m.deleted_at IS NULL' : ''}
      WHERE ${whereClause} ${stageCondition}
      ORDER BY i.created_at DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
    params.push(Number(limit), offset);

    // Run both queries in parallel
    const [countResult, idsResult] = await Promise.all([
      query(countSql, countParams),
      query(idsSql, params)
    ]);

    const total = Number(countResult.rows[0].count);
    const ids = idsResult.rows.map((r: any) => r.id);

    if (ids.length === 0) {
      return res.json({
        data: [],
        pagination: {
          page: Number(page),
          limit: Number(limit),
          total,
          totalPages: Math.ceil(total / Number(limit)),
        },
      });
    }

    /* PHASE 2: Fetch full data for IDs */
    const sql = `
      SELECT
        i.id AS inbound_id,
        i.wsn,
        m.wid, m.fsn, m.order_id, m.product_title, m.brand, m.cms_vertical,
        m.mrp, m.fsp, i.inbound_date, 'INBOUND_RECEIVED' AS inbound_status,
        i.rack_no, m.wh_location AS warehouse_location, m.hsn_sac, m.igst_rate,
        m.invoice_date, m.fkt_link, m.p_type, m.p_size, m.vrp, m.yield_value,
        m.fkqc_remark, m.fk_grade,
        q.id AS qc_id, q.qc_date,
        COALESCE(q.qc_grade, '') AS qc_grade,
        CASE WHEN q.id IS NOT NULL THEN 'DONE' ELSE 'Pending' END AS qc_status,
        p.id AS picking_id, p.picking_date,
        CASE WHEN p.id IS NOT NULL THEN 'DONE' ELSE 'PENDING' END AS picking_status,
        o.id AS outbound_id, o.dispatch_date AS outbound_date,
        CASE WHEN o.id IS NOT NULL THEN 'DONE' ELSE 'PENDING' END AS outbound_status, o.vehicle_no,
        CASE
          WHEN o.id IS NOT NULL AND o.dispatch_date IS NOT NULL THEN 'DISPATCHED'
          WHEN p.id IS NOT NULL THEN 'PICKING'
          WHEN q.id IS NOT NULL AND q.qc_grade IS NOT NULL THEN 'QC_DONE'
          ELSE 'INBOUND'
        END AS current_stage
      FROM inbound i
      LEFT JOIN master_data m ON m.wsn = i.wsn AND m.deleted_at IS NULL
      LEFT JOIN qc q ON i.wsn = q.wsn AND i.warehouse_id = q.warehouse_id
      LEFT JOIN picking p ON i.wsn = p.wsn AND i.warehouse_id = p.warehouse_id
      LEFT JOIN outbound o ON i.wsn = o.wsn AND i.warehouse_id = o.warehouse_id
      WHERE i.id = ANY($1)
      ORDER BY i.created_at DESC
    `;
    const result = await query(sql, [ids]);

    return res.json({
      data: result.rows,
      pagination: {
        page: Number(page),
        limit: Number(limit),
        total,
        totalPages: Math.ceil(total / Number(limit)),
      },
    });

  } catch (error: any) {
    console.error("Get inventory pipeline error:", error);
    return res.status(500).json({ error: safeError(error) });
  }
};

export const getInventoryMetrics = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;

    // Validate warehouse access
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;
    if (accessibleWarehouses && accessibleWarehouses.length > 0 && warehouseId) {
      const requestedId = parseInt(warehouseId as string);
      if (!accessibleWarehouses.includes(requestedId)) {
        return res.status(403).json({ error: 'Access denied to this warehouse' });
      }
    }

    if (!warehouseId) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    const sql = `
      WITH master_count AS (
        -- Master Data total (filtered by warehouse)
        SELECT COUNT(*) as total FROM master_data WHERE deleted_at IS NULL AND warehouse_id = $1
      ),
      inbound_count AS (
        -- Inbound per warehouse (jaisa pehle tha)
        SELECT COUNT(*) as total FROM inbound WHERE warehouse_id = $1
      ),
      qc_count AS (
        SELECT
          SUM(CASE WHEN qc_status = 'Pass' THEN 1 ELSE 0 END) as qcPassed,
          SUM(CASE WHEN qc_status = 'Fail' THEN 1 ELSE 0 END) as qcFailed,
          SUM(CASE WHEN qc_status = 'Done' THEN 1 ELSE 0 END) as qcDone,
          SUM(CASE WHEN (qc_status IS NULL OR qc_status = '' OR qc_status = 'Pending') THEN 1 ELSE 0 END) as qcPending
        FROM qc WHERE warehouse_id = $1
      ),
      picking_count AS (
        SELECT
          SUM(CASE WHEN picking_date IS NOT NULL THEN 1 ELSE 0 END) as pickingCompleted,
          SUM(CASE WHEN picking_date IS NULL THEN 1 ELSE 0 END) as pickingPending
        FROM picking WHERE warehouse_id = $1
      ),
      outbound_count AS (
        SELECT
          SUM(CASE WHEN dispatch_date IS NOT NULL THEN 1 ELSE 0 END) as outboundDispatched,
          SUM(CASE WHEN dispatch_date IS NULL THEN 1 ELSE 0 END) as outboundReady
        FROM outbound WHERE warehouse_id = $1
      )
      SELECT
        COALESCE(mc.total, 0)  as mastertotal,
        COALESCE(ic.total, 0)  as inboundtotal,
        COALESCE(qc.qcPending, 0)        as qcPending,
        COALESCE(qc.qcPassed, 0)         as qcPassed,
        COALESCE(qc.qcFailed, 0)         as qcFailed,
        COALESCE(qc.qcDone, 0)           as qcDone,
        COALESCE(p.pickingPending, 0)    as pickingPending,
        COALESCE(p.pickingCompleted, 0)  as pickingCompleted,
        COALESCE(o.outboundReady, 0)     as outboundReady,
        COALESCE(o.outboundDispatched, 0) as outboundDispatched
      FROM master_count mc, inbound_count ic, qc_count qc, picking_count p, outbound_count o
    `;

    const result = await query(sql, [warehouseId]);
    const metrics = result.rows[0];

    const qcPassed = parseInt(metrics.qcpassed) || 0;
    const qcDone = parseInt(metrics.qcdone) || 0;

    res.json({
      // Master Data total (export / uploads se aayega, inbound ke bina bhi)
      total: parseInt(metrics.mastertotal) || 0,
      // Inbound total alag se
      inbound: parseInt(metrics.inboundtotal) || 0,
      qcPending: parseInt(metrics.qcpending) || 0,
      qcPassed: qcPassed,
      qcDone: qcDone,
      // qcTotal counts both explicit Pass and Done entries
      qcTotal: qcPassed + qcDone,
      qcFailed: parseInt(metrics.qcfailed) || 0,
      pickingPending: parseInt(metrics.pickingpending) || 0,
      pickingCompleted: parseInt(metrics.pickingcompleted) || 0,
      outboundReady: parseInt(metrics.outboundready) || 0,
      outboundDispatched: parseInt(metrics.outbounddispatched) || 0
    });
  } catch (error: any) {
    console.error('Get metrics error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// Get activity logs
export const getActivityLogs = async (req: Request, res: Response) => {
  try {
    const { warehouseId, page = 1, limit = 50 } = req.query;

    // Validate warehouse access
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;
    if (accessibleWarehouses && accessibleWarehouses.length > 0 && warehouseId) {
      const requestedId = parseInt(warehouseId as string);
      if (!accessibleWarehouses.includes(requestedId)) {
        return res.status(403).json({ error: 'Access denied to this warehouse' });
      }
    }

    if (!warehouseId) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    const offset = (Number(page) - 1) * Number(limit);

    const sql = `
      SELECT
        ual.id,
        ual.user_id,
        u.full_name as user_name,
        ual.activity_type,
        ual.module,
        ual.details,
        ual.timestamp
      FROM user_activity_logs ual
      LEFT JOIN users u ON ual.user_id = u.id
      WHERE u.warehouse_id = $1
      ORDER BY ual.timestamp DESC
      LIMIT $2 OFFSET $3
    `;

    const result = await query(sql, [warehouseId, Number(limit), offset]);

    res.json(result.rows);
  } catch (error: any) {
    console.error('Get activity logs error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ✅ EXPORT WITH COMPLETE DATA - FIXED: DATE CASTING + COLUMN NAME
export const getInventoryDataForExport = async (req: Request, res: Response) => {
  try {
    const {
      warehouseId,
      dateFrom,
      dateTo,
      stage,
      brand,
      category,
      search: searchText,   // 🔁 rename: frontend 'search' -> searchText
    } = req.query as any;

    // Validate warehouse access
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;
    if (accessibleWarehouses && accessibleWarehouses.length > 0 && warehouseId) {
      const requestedId = parseInt(warehouseId as string);
      if (!accessibleWarehouses.includes(requestedId)) {
        return res.status(403).json({ error: 'Access denied to this warehouse' });
      }
    }

    if (!warehouseId) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    // Build dynamic WHERE clause
    let whereConditions: string[] = ['i.warehouse_id = $1'];
    const params: any[] = [warehouseId];
    let paramIndex = 2;

    // Date filter
    if (dateFrom && dateTo) {
      whereConditions.push(`i.inbound_date::text BETWEEN $${paramIndex} AND $${paramIndex + 1}`);
      params.push(dateFrom, dateTo);
      paramIndex += 2;
    } else if (dateFrom) {
      whereConditions.push(`i.inbound_date::text >= $${paramIndex}`);
      params.push(dateFrom);
      paramIndex += 1;
    } else if (dateTo) {
      whereConditions.push(`i.inbound_date::text <= $${paramIndex}`);
      params.push(dateTo);
      paramIndex += 1;
    }



    // Brand filter
    if (brand && brand !== '') {
      whereConditions.push(`m.brand = $${paramIndex}`);
      params.push(brand);
      paramIndex++;
    }

    // Category filter
    if (category && category !== '') {
      whereConditions.push(`m.cms_vertical = $${paramIndex}`);
      params.push(category);
      paramIndex++;
    }

    // Search filter
    if (searchText && searchText !== '') {
      whereConditions.push(`(i.wsn ILIKE $${paramIndex} OR i.product_title ILIKE $${paramIndex})`);
      params.push(`%${searchText}%`);
      paramIndex++;
    }


    // Available Only (export scope) - exclude only dispatched items
    if ((req.query as any).availableOnly === 'true') {
      whereConditions.push(`NOT EXISTS (SELECT 1 FROM outbound WHERE outbound.wsn = i.wsn AND outbound.warehouse_id = i.warehouse_id AND outbound.dispatch_date IS NOT NULL)`);
    }

    // Stage filter – simplified to match app workflow (inbound/qc/picking/dispatched)
    let stageFilter = '';
    if (stage && stage !== 'all') {
      if (stage === 'inbound') {
        // Ensure export includes all inbounded items regardless of downstream state
        stageFilter = `AND (
          CASE
            WHEN o.id IS NOT NULL AND o.dispatch_date IS NOT NULL THEN 'DISPATCHED'
            WHEN p.id IS NOT NULL THEN 'PICKING'
            WHEN q.id IS NOT NULL AND q.qc_grade IS NOT NULL THEN 'QC_DONE'
            ELSE 'INBOUND'
          END
        ) IN ('INBOUND','QC_DONE','PICKING','DISPATCHED')`;
      } else if (stage === 'qc') {
        stageFilter = 'AND (q.qc_grade IS NOT NULL)';
      } else if (stage === 'picking') {
        stageFilter = 'AND (p.id IS NOT NULL)';
      } else if (stage === 'dispatched') {
        stageFilter = 'AND (o.dispatch_date IS NOT NULL)';
      }
    }
    const whereClause = whereConditions.join(' AND ');

    const sql = `
      SELECT
        i.wsn,
        m.wid,
        m.fsn,
        m.order_id,
        m.product_title,
        m.brand,
        m.cms_vertical,
        m.fsp,
        m.mrp,
        i.inbound_date,
        i.vehicle_no,
        i.rack_no,
        m.wh_location,
        m.hsn_sac,
        m.igst_rate,
        m.invoice_date,
        m.fkt_link,
        m.p_type,
        m.p_size,
        m.vrp,
        m.yield_value,
        -- QC Details
        COALESCE(q.qc_date::TEXT, '') as qc_date,
        COALESCE(q.qc_by::TEXT, 'N/A') as qc_by,
        COALESCE(q.qc_grade, 'PENDING') as qc_grade,
        COALESCE(q.qc_remarks, '') as qc_remarks,
        -- NOTE: fkqc_remark is in master_data
        COALESCE(m.fkqc_remark, '') as fkqc_remark,
        -- Picking Details
        COALESCE(p.picking_date::TEXT, '') as picking_date,
        COALESCE(p.customer_name, '') as customer_name,
        COALESCE(p.picking_remarks, '') as picking_remarks,
        -- Outbound Details
        COALESCE(o.dispatch_date::TEXT, '') as dispatch_date,
        COALESCE(o.vehicle_no, '') as dispatch_vehicle,
        COALESCE(o.dispatch_remarks, '') as dispatch_remarks,
        -- Status
        CASE
          WHEN o.id IS NOT NULL THEN 'DISPATCHED'
          WHEN p.id IS NOT NULL THEN 'PICKED'
          WHEN q.id IS NOT NULL THEN 'QC_DONE'
          ELSE 'INBOUND'
        END as current_status,
        -- Batch Info
        COALESCE(i.batch_id::TEXT, 'SINGLE') as batch_id,
        i.created_at,
        COALESCE(i.created_user_name, 'System') as created_by
      FROM inbound i
      LEFT JOIN master_data m ON m.wsn = i.wsn AND m.deleted_at IS NULL
      LEFT JOIN qc q ON i.wsn = q.wsn AND i.warehouse_id = q.warehouse_id
      LEFT JOIN picking p ON i.wsn = p.wsn AND i.warehouse_id = p.warehouse_id
      LEFT JOIN outbound o ON i.wsn = o.wsn AND i.warehouse_id = o.warehouse_id

      WHERE ${whereClause}
      ${stageFilter}
      ORDER BY i.created_at DESC
      LIMIT 50000
    `;

    const result = await query(sql, params);

    return res.json({
      success: true,
      totalRecords: result.rows.length,
      data: result.rows,
      exportedAt: new Date().toISOString(),
    });
  } catch (error: any) {
    console.error('Export data error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================================================
// PIVOT TABLE APIs - Server-side aggregation for large data (5-10 lakh+ rows)
// ============================================================================

/**
 * Get pivot summary - Category-wise count of available inventory
 * Optimized for large datasets using SQL aggregation
 */
export const getPivotSummary = async (req: Request, res: Response) => {
  try {
    const { warehouseId, groupBy = 'cms_vertical', brand, category } = req.query;

    // Validate warehouse access
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;
    if (accessibleWarehouses && accessibleWarehouses.length > 0 && warehouseId) {
      const requestedId = parseInt(warehouseId as string);
      if (!accessibleWarehouses.includes(requestedId)) {
        return res.status(403).json({ error: 'Access denied to this warehouse' });
      }
    }

    if (!warehouseId) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    // Validate groupBy field to prevent SQL injection
    const allowedGroupFields = ['cms_vertical', 'brand', 'qc_grade', 'current_stage'];
    const groupField = allowedGroupFields.includes(groupBy as string) ? groupBy : 'cms_vertical';

    // Map groupBy to actual column reference
    const groupColumnMap: Record<string, string> = {
      'cms_vertical': 'm.cms_vertical',
      'brand': 'm.brand',
      'qc_grade': 'COALESCE(q.qc_grade, \'PENDING\')',
      'current_stage': `CASE
        WHEN o.id IS NOT NULL AND o.dispatch_date IS NOT NULL THEN 'DISPATCHED'
        WHEN p.id IS NOT NULL THEN 'PICKING'
        WHEN q.id IS NOT NULL AND q.qc_grade IS NOT NULL THEN 'QC_DONE'
        ELSE 'INBOUND'
      END`
    };

    const groupColumn = groupColumnMap[groupField as string] || 'm.cms_vertical';

    // Build dynamic WHERE conditions for filters
    const conditions: string[] = ['i.warehouse_id = $1', 'NOT (o.id IS NOT NULL AND o.dispatch_date IS NOT NULL)'];
    const params: any[] = [warehouseId];
    let paramIndex = 2;

    if (brand) {
      conditions.push(`m.brand = $${paramIndex}`);
      params.push(brand);
      paramIndex++;
    }

    if (category) {
      conditions.push(`m.cms_vertical = $${paramIndex}`);
      params.push(category);
      paramIndex++;
    }

    // Optimized SQL for pivot summary - Only available inventory (not dispatched)
    const sql = `
      SELECT 
        COALESCE(${groupColumn}, 'Unknown') as category,
        COUNT(*) as qty,
        COALESCE(SUM(CAST(NULLIF(m.fsp, '') AS NUMERIC)), 0) as total_fsp,
        COALESCE(SUM(CAST(NULLIF(m.mrp, '') AS NUMERIC)), 0) as total_mrp,
        COALESCE(SUM(CAST(NULLIF(m.vrp, '') AS NUMERIC)), 0) as total_vrp
      FROM inbound i
      LEFT JOIN master_data m ON m.wsn = i.wsn AND m.deleted_at IS NULL
      LEFT JOIN qc q ON i.wsn = q.wsn AND i.warehouse_id = q.warehouse_id
      LEFT JOIN picking p ON i.wsn = p.wsn AND i.warehouse_id = p.warehouse_id
      LEFT JOIN outbound o ON i.wsn = o.wsn AND i.warehouse_id = o.warehouse_id
      WHERE ${conditions.join(' AND ')}
      GROUP BY ${groupColumn}
      ORDER BY qty DESC
    `;

    const result = await query(sql, params);

    // Calculate grand total
    const grandTotal = result.rows.reduce((acc, row) => ({
      qty: acc.qty + parseInt(row.qty),
      total_fsp: acc.total_fsp + parseFloat(row.total_fsp || 0),
      total_mrp: acc.total_mrp + parseFloat(row.total_mrp || 0),
      total_vrp: acc.total_vrp + parseFloat(row.total_vrp || 0),
    }), { qty: 0, total_fsp: 0, total_mrp: 0, total_vrp: 0 });

    return res.json({
      success: true,
      groupBy: groupField,
      data: result.rows.map(row => ({
        category: row.category || 'Unknown',
        qty: parseInt(row.qty),
        total_fsp: parseFloat(row.total_fsp || 0),
        total_mrp: parseFloat(row.total_mrp || 0),
        total_vrp: parseFloat(row.total_vrp || 0),
      })),
      grandTotal,
      generatedAt: new Date().toISOString(),
    });

  } catch (error: any) {
    console.error('Pivot summary error:', error);
    return res.status(500).json({ error: safeError(error) });
  }
};

/**
 * Get all unique brands and categories for pivot filters
 */
export const getPivotFilters = async (req: Request, res: Response) => {
  try {
    const { warehouseId, brand, category } = req.query;

    // Validate warehouse access
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;
    if (accessibleWarehouses && accessibleWarehouses.length > 0 && warehouseId) {
      const requestedId = parseInt(warehouseId as string);
      if (!accessibleWarehouses.includes(requestedId)) {
        return res.status(403).json({ error: 'Access denied to this warehouse' });
      }
    }

    if (!warehouseId) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    // Build dynamic filter conditions
    const params: any[] = [warehouseId];
    let brandCondition = '';
    let categoryCondition = '';

    if (brand && typeof brand === 'string' && brand.trim() !== '') {
      params.push(brand);
      brandCondition = ` AND m.brand = $${params.length}`;
    }
    if (category && typeof category === 'string' && category.trim() !== '') {
      params.push(category);
      categoryCondition = ` AND m.cms_vertical = $${params.length}`;
    }

    // Get unique brands (filtered by selected category) and categories (filtered by selected brand)
    const sql = `
      SELECT 
        (
          SELECT ARRAY_AGG(DISTINCT m2.brand) 
          FROM inbound i2
          LEFT JOIN master_data m2 ON m2.wsn = i2.wsn AND m2.deleted_at IS NULL
          LEFT JOIN outbound o2 ON i2.wsn = o2.wsn AND i2.warehouse_id = o2.warehouse_id
          WHERE i2.warehouse_id = $1
            AND NOT (o2.id IS NOT NULL AND o2.dispatch_date IS NOT NULL)
            AND m2.brand IS NOT NULL AND m2.brand != ''
            ${categoryCondition.replace(/m\./g, 'm2.')}
        ) as brands,
        (
          SELECT ARRAY_AGG(DISTINCT m3.cms_vertical)
          FROM inbound i3
          LEFT JOIN master_data m3 ON m3.wsn = i3.wsn AND m3.deleted_at IS NULL
          LEFT JOIN outbound o3 ON i3.wsn = o3.wsn AND i3.warehouse_id = o3.warehouse_id
          WHERE i3.warehouse_id = $1
            AND NOT (o3.id IS NOT NULL AND o3.dispatch_date IS NOT NULL)
            AND m3.cms_vertical IS NOT NULL AND m3.cms_vertical != ''
            ${brandCondition.replace(/m\./g, 'm3.')}
        ) as categories
    `;

    const result = await query(sql, params);
    const row = result.rows[0] || {};

    return res.json({
      success: true,
      brands: (row.brands || []).sort(),
      categories: (row.categories || []).sort(),
    });

  } catch (error: any) {
    console.error('Pivot filters error:', error);
    return res.status(500).json({ error: safeError(error) });
  }
};

/**
 * Get drill-down data for a specific category
 * Returns full master_data details with pagination for large results
 */
export const getPivotDrilldown = async (req: Request, res: Response) => {
  try {
    const {
      warehouseId,
      groupBy = 'cms_vertical',
      categoryValue,
      page = 1,
      limit = 100,
      exportAll = false  // If true, returns all data for Excel export
    } = req.query;

    // Validate warehouse access
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;
    if (accessibleWarehouses && accessibleWarehouses.length > 0 && warehouseId) {
      const requestedId = parseInt(warehouseId as string);
      if (!accessibleWarehouses.includes(requestedId)) {
        return res.status(403).json({ error: 'Access denied to this warehouse' });
      }
    }

    if (!warehouseId) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    if (!categoryValue) {
      return res.status(400).json({ error: 'Category value required' });
    }

    // Map groupBy to actual column reference
    const groupColumnMap: Record<string, string> = {
      'cms_vertical': 'm.cms_vertical',
      'brand': 'm.brand',
      'qc_grade': 'COALESCE(q.qc_grade, \'PENDING\')',
      'current_stage': `CASE
        WHEN o.id IS NOT NULL AND o.dispatch_date IS NOT NULL THEN 'DISPATCHED'
        WHEN p.id IS NOT NULL THEN 'PICKING'
        WHEN q.id IS NOT NULL AND q.qc_grade IS NOT NULL THEN 'QC_DONE'
        ELSE 'INBOUND'
      END`
    };

    const groupColumn = groupColumnMap[groupBy as string] || 'm.cms_vertical';
    const offset = (Number(page) - 1) * Number(limit);

    // Count query
    const countSql = `
      SELECT COUNT(*) as total
      FROM inbound i
      LEFT JOIN master_data m ON m.wsn = i.wsn AND m.deleted_at IS NULL
      LEFT JOIN qc q ON i.wsn = q.wsn AND i.warehouse_id = q.warehouse_id
      LEFT JOIN picking p ON i.wsn = p.wsn AND i.warehouse_id = p.warehouse_id
      LEFT JOIN outbound o ON i.wsn = o.wsn AND i.warehouse_id = o.warehouse_id
      WHERE i.warehouse_id = $1
        AND NOT (o.id IS NOT NULL AND o.dispatch_date IS NOT NULL)
        AND COALESCE(${groupColumn}, 'Unknown') = $2
    `;

    // Data query - ALL master_data columns + related data
    const dataSql = `
      SELECT 
        -- Master Data (ALL columns)
        m.id as master_id,
        m.wsn,
        m.wid,
        m.fsn,
        m.order_id,
        m.fkqc_remark,
        m.fk_grade,
        m.product_title,
        m.hsn_sac,
        m.igst_rate,
        m.fsp,
        m.mrp,
        m.invoice_date,
        m.fkt_link,
        m.wh_location,
        m.brand,
        m.cms_vertical,
        m.vrp,
        m.yield_value,
        m.p_type,
        m.p_size,
        m.batch_id as master_batch_id,
        m.actual_received,
        m.upload_date,
        m.created_user_name as master_created_by,
        
        -- Inbound Data
        i.id as inbound_id,
        i.inbound_date,
        i.vehicle_no as inbound_vehicle,
        i.unload_remarks,
        i.quantity as inbound_qty,
        i.rack_no,
        i.warehouse_id,
        i.warehouse_name,
        i.batch_id as inbound_batch_id,
        i.created_user_name as inbound_created_by,
        i.created_at as inbound_created_at,
        
        -- QC Data
        q.id as qc_id,
        q.qc_date,
        q.qc_grade,
        q.qc_status,
        q.qc_remarks,
        q.qc_by,
        
        -- Picking Data
        p.id as picking_id,
        p.picking_date,
        p.customer_name,
        p.picking_remarks,
        
        -- Outbound Data (will be null for available items)
        o.id as outbound_id,
        o.dispatch_date,
        o.vehicle_no as dispatch_vehicle,
        o.dispatch_remarks,
        
        -- Computed Stage
        CASE
          WHEN o.id IS NOT NULL AND o.dispatch_date IS NOT NULL THEN 'DISPATCHED'
          WHEN p.id IS NOT NULL THEN 'PICKING'
          WHEN q.id IS NOT NULL AND q.qc_grade IS NOT NULL THEN 'QC_DONE'
          ELSE 'INBOUND'
        END as current_stage
        
      FROM inbound i
      LEFT JOIN master_data m ON m.wsn = i.wsn AND m.deleted_at IS NULL
      LEFT JOIN qc q ON i.wsn = q.wsn AND i.warehouse_id = q.warehouse_id
      LEFT JOIN picking p ON i.wsn = p.wsn AND i.warehouse_id = p.warehouse_id
      LEFT JOIN outbound o ON i.wsn = o.wsn AND i.warehouse_id = o.warehouse_id
      WHERE i.warehouse_id = $1
        AND NOT (o.id IS NOT NULL AND o.dispatch_date IS NOT NULL)
        AND COALESCE(${groupColumn}, 'Unknown') = $2
      ORDER BY i.created_at DESC
      ${exportAll === 'true' ? '' : `LIMIT $3 OFFSET $4`}
    `;

    // Execute queries
    const countResult = await query(countSql, [warehouseId, categoryValue]);
    const total = parseInt(countResult.rows[0].total);

    const dataParams = exportAll === 'true'
      ? [warehouseId, categoryValue]
      : [warehouseId, categoryValue, Number(limit), offset];

    const dataResult = await query(dataSql, dataParams);

    return res.json({
      success: true,
      categoryValue,
      groupBy,
      data: dataResult.rows,
      pagination: exportAll === 'true' ? null : {
        page: Number(page),
        limit: Number(limit),
        total,
        totalPages: Math.ceil(total / Number(limit)),
      },
      generatedAt: new Date().toISOString(),
    });

  } catch (error: any) {
    console.error('Pivot drilldown error:', error);
    return res.status(500).json({ error: safeError(error) });
  }
};

/**
 * Export all available inventory data (for pivot table export)
 * Exports all items not yet dispatched
 */
export const getPivotExportAll = async (req: Request, res: Response) => {
  try {
    const { warehouseId, brand, category } = req.query;

    // Validate warehouse access
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;
    if (accessibleWarehouses && accessibleWarehouses.length > 0 && warehouseId) {
      const requestedId = parseInt(warehouseId as string);
      if (!accessibleWarehouses.includes(requestedId)) {
        return res.status(403).json({ error: 'Access denied to this warehouse' });
      }
    }

    if (!warehouseId) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    // Build WHERE clause with optional filters
    let whereConditions = ['i.warehouse_id = $1', 'NOT (o.id IS NOT NULL AND o.dispatch_date IS NOT NULL)'];
    const params: any[] = [warehouseId];
    let paramIndex = 2;

    if (brand && brand !== '') {
      whereConditions.push(`m.brand = $${paramIndex}`);
      params.push(brand);
      paramIndex++;
    }

    if (category && category !== '') {
      whereConditions.push(`m.cms_vertical = $${paramIndex}`);
      params.push(category);
      paramIndex++;
    }

    const whereClause = whereConditions.join(' AND ');

    // Data query - ALL master_data columns + related data
    const dataSql = `
      SELECT 
        -- Master Data (ALL columns)
        m.id as master_id,
        m.wsn,
        m.wid,
        m.fsn,
        m.order_id,
        m.fkqc_remark,
        m.fk_grade,
        m.product_title,
        m.hsn_sac,
        m.igst_rate,
        m.fsp,
        m.mrp,
        m.invoice_date,
        m.fkt_link,
        m.wh_location,
        m.brand,
        m.cms_vertical,
        m.vrp,
        m.yield_value,
        m.p_type,
        m.p_size,
        m.batch_id as master_batch_id,
        m.actual_received,
        m.upload_date,
        m.created_user_name as master_created_by,
        
        -- Inbound Data
        i.id as inbound_id,
        i.inbound_date,
        i.vehicle_no as inbound_vehicle,
        i.unload_remarks,
        i.quantity as inbound_qty,
        i.rack_no,
        i.warehouse_id,
        i.warehouse_name,
        i.batch_id as inbound_batch_id,
        i.created_user_name as inbound_created_by,
        i.created_at as inbound_created_at,
        
        -- QC Data
        q.id as qc_id,
        q.qc_date,
        q.qc_grade,
        q.qc_status,
        q.qc_remarks,
        q.qc_by,
        
        -- Picking Data
        p.id as picking_id,
        p.picking_date,
        p.customer_name,
        p.picking_remarks,
        
        -- Outbound Data (will be null for available items)
        o.id as outbound_id,
        o.dispatch_date,
        o.vehicle_no as dispatch_vehicle,
        o.dispatch_remarks,
        
        -- Computed Stage
        CASE
          WHEN o.id IS NOT NULL AND o.dispatch_date IS NOT NULL THEN 'DISPATCHED'
          WHEN p.id IS NOT NULL THEN 'PICKING'
          WHEN q.id IS NOT NULL AND q.qc_grade IS NOT NULL THEN 'QC_DONE'
          ELSE 'INBOUND'
        END as current_stage
        
      FROM inbound i
      LEFT JOIN master_data m ON m.wsn = i.wsn AND m.deleted_at IS NULL
      LEFT JOIN qc q ON i.wsn = q.wsn AND i.warehouse_id = q.warehouse_id
      LEFT JOIN picking p ON i.wsn = p.wsn AND i.warehouse_id = p.warehouse_id
      LEFT JOIN outbound o ON i.wsn = o.wsn AND i.warehouse_id = o.warehouse_id
      WHERE ${whereClause}
      ORDER BY i.created_at DESC
      LIMIT 50000
    `;

    const dataResult = await query(dataSql, params);

    return res.json({
      success: true,
      data: dataResult.rows,
      totalRecords: dataResult.rows.length,
      generatedAt: new Date().toISOString(),
    });

  } catch (error: any) {
    console.error('Pivot export all error:', error);
    return res.status(500).json({ error: safeError(error) });
  }
};