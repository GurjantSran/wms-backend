// File Path = warehouse-backend/src/controllers/inventory.controller.ts
import { Request, Response } from 'express';
import { query } from '../config/database';
import { safeError } from '../utils/sanitizeError';

// Validate that the requested warehouseId is in the user's accessible warehouses
const validateWarehouseAccess = (req: Request, warehouseId: string | number): boolean => {
  const accessible = (req as any).accessibleWarehouses;
  if (accessible === null || accessible === undefined) return true; // null = admin/super_admin, all access
  return accessible.map(String).includes(String(warehouseId));
};

// Get inventory summary by warehouse
export const getInventorySummary = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;

    if (!warehouseId) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    if (!validateWarehouseAccess(req, warehouseId as string)) {
      return res.status(403).json({ error: 'Forbidden: You do not have access to this warehouse' });
    }

    // Get complete inventory breakdown
    const sql = `
      WITH
      inbound_data AS (
        SELECT COUNT(*) as total FROM inbound WHERE warehouse_id = $1
      ),
      outbound_data AS (
        SELECT COUNT(*) as total FROM outbound WHERE warehouse_id = $1
      ),
      qc_pending AS (
        SELECT COUNT(*) as total 
        FROM inbound i 
        WHERE i.warehouse_id = $1 
        AND NOT EXISTS (SELECT 1 FROM qc WHERE qc.wsn = i.wsn)
      ),
      qc_passed AS (
        SELECT COUNT(*) as total 
        FROM qc 
        WHERE warehouse_id = $1 AND qc_status = 'Pass'
      ),
      qc_failed AS (
        SELECT COUNT(*) as total 
        FROM qc 
        WHERE warehouse_id = $1 AND qc_status = 'Fail'
      ),
      picking_pending AS (
        SELECT COUNT(*) as total 
        FROM qc 
        WHERE warehouse_id = $1 
        AND qc_status = 'Pass'
        AND NOT EXISTS (SELECT 1 FROM picking WHERE picking.wsn = qc.wsn)
      ),
      picked_items AS (
        SELECT COUNT(*) as total FROM picking WHERE warehouse_id = $1
      ),
      ready_for_dispatch AS (
        SELECT COUNT(*) as total 
        FROM picking p
        WHERE p.warehouse_id = $1
        AND NOT EXISTS (SELECT 1 FROM outbound WHERE outbound.wsn = p.wsn)
      ),
      dispatched_items AS (
        SELECT COUNT(*) as total 
        FROM outbound 
        WHERE warehouse_id = $1 AND dispatch_date IS NOT NULL
      ),
      available_stock AS (
        SELECT COUNT(*) as total
        FROM inbound i
        WHERE i.warehouse_id = $1 
        AND NOT EXISTS (SELECT 1 FROM outbound WHERE outbound.wsn = i.wsn AND outbound.warehouse_id = i.warehouse_id)
      )
      SELECT
        COALESCE(i.total, 0) as total_inbound,
        COALESCE(ob.total, 0) as total_outbound,
        COALESCE(qp.total, 0) as qc_pending,
        COALESCE(qpass.total, 0) as qc_passed,
        COALESCE(qfail.total, 0) as qc_failed,
        COALESCE(pp.total, 0) as picking_pending,
        COALESCE(pi.total, 0) as picked_items,
        COALESCE(rfd.total, 0) as ready_for_dispatch,
        COALESCE(di.total, 0) as dispatched_items,
        COALESCE(ast.total, 0) as available_stock
      FROM inbound_data i, outbound_data ob, qc_pending qp, qc_passed qpass, qc_failed qfail,
           picking_pending pp, picked_items pi, ready_for_dispatch rfd, 
           dispatched_items di, available_stock ast
    `;

    const result = await query(sql, [warehouseId]);
    res.json(result.rows[0]);
  } catch (error: any) {
    console.error('❌ Inventory summary error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// Get available stock list with details
export const getAvailableStock = async (req: Request, res: Response) => {
  try {
    const { warehouseId, page = 1, limit = 100, search = '' } = req.query;

    if (!warehouseId) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    if (!validateWarehouseAccess(req, warehouseId as string)) {
      return res.status(403).json({ error: 'Forbidden: You do not have access to this warehouse' });
    }

    const offset = (Number(page) - 1) * Number(limit);
    let whereConditions = ['q.warehouse_id = $1', "q.qc_status = 'Pass'"];
    const params: any[] = [warehouseId];
    let paramIndex = 2;

    // Add search filter
    if (search) {
      whereConditions.push(`(q.wsn ILIKE $${paramIndex} OR m.product_title ILIKE $${paramIndex})`);
      params.push(`%${search}%`);
      paramIndex++;
    }

    // Items that passed QC but not yet picked or dispatched
    whereConditions.push(`NOT EXISTS (SELECT 1 FROM picking WHERE picking.wsn = q.wsn)`);
    whereConditions.push(`NOT EXISTS (SELECT 1 FROM outbound WHERE outbound.wsn = q.wsn)`);

    const sql = `
      SELECT 
        q.id, q.wsn, q.qc_date, q.qc_status, q.qc_grade, q.rack_no,
        m.product_title, m.brand, m.cms_vertical, m.mrp, m.fsp,
        i.inbound_date, i.vehicle_no
      FROM qc q
      LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL
      LEFT JOIN inbound i ON q.wsn = i.wsn AND q.warehouse_id = i.warehouse_id
      WHERE ${whereConditions.join(' AND ')}
      ORDER BY q.qc_date DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;

    params.push(limit, offset);

    const countSql = `
      SELECT COUNT(*) as total
      FROM qc q
      LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE ${whereConditions.join(' AND ')}
    `;

    const [dataResult, countResult] = await Promise.all([
      query(sql, params),
      query(countSql, params.slice(0, -2))
    ]);

    res.json({
      data: dataResult.rows,
      pagination: {
        page: Number(page),
        limit: Number(limit),
        total: Number(countResult.rows[0].total)
      }
    });
  } catch (error: any) {
    console.error('❌ Available stock error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// Get stock by status
export const getStockByStatus = async (req: Request, res: Response) => {
  try {
    const { warehouseId, status } = req.query;

    if (!warehouseId || !status) {
      return res.status(400).json({ error: 'Warehouse ID and status required' });
    }

    if (!validateWarehouseAccess(req, warehouseId as string)) {
      return res.status(403).json({ error: 'Forbidden: You do not have access to this warehouse' });
    }

    let sql = '';

    switch (status) {
      case 'in_qc':
        sql = `
          SELECT 
            q.id, q.wsn, q.qc_date, q.qc_status, q.qc_grade, q.rack_no,
            m.product_title, m.brand, m.cms_vertical, m.mrp, m.fsp,
            i.inbound_date
          FROM qc q
          LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL
          LEFT JOIN inbound i ON q.wsn = i.wsn AND q.warehouse_id = i.warehouse_id
          WHERE q.warehouse_id = $1 AND q.qc_status = 'Pass'
          AND NOT EXISTS (SELECT 1 FROM picking WHERE picking.wsn = q.wsn)
          ORDER BY q.qc_date DESC
          LIMIT 5000
        `;
        break;

      case 'qc_passed':
        sql = `
          SELECT 
            q.id, q.wsn, q.qc_date, q.qc_status, q.qc_grade, q.rack_no,
            m.product_title, m.brand, m.cms_vertical, m.mrp, m.fsp
          FROM qc q
          LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL
          WHERE q.warehouse_id = $1 AND q.qc_status = 'Pass'
          ORDER BY q.qc_date DESC
          LIMIT 5000
        `;
        break;

      case 'picked':
        sql = `
          SELECT 
            p.id, p.wsn, p.picking_date, p.customer_id, p.order_id,
            m.product_title, m.brand, m.cms_vertical, m.mrp, m.fsp,
            q.qc_grade, q.rack_no
          FROM picking p
          LEFT JOIN master_data m ON p.wsn = m.wsn AND m.deleted_at IS NULL
          LEFT JOIN qc q ON p.wsn = q.wsn AND p.warehouse_id = q.warehouse_id
          WHERE p.warehouse_id = $1
          ORDER BY p.picking_date DESC
          LIMIT 5000
        `;
        break;

      case 'dispatched':
        sql = `
          SELECT 
            o.id, o.wsn, o.dispatch_date, o.vehicle_no, o.driver_name,
            m.product_title, m.brand, m.cms_vertical, m.mrp, m.fsp,
            c.customer_name
          FROM outbound o
          LEFT JOIN master_data m ON o.wsn = m.wsn AND m.deleted_at IS NULL
          LEFT JOIN customers c ON o.customer_id = c.id
          WHERE o.warehouse_id = $1 AND o.dispatch_date IS NOT NULL
          ORDER BY o.dispatch_date DESC
          LIMIT 5000
        `;
        break;

      default:
        return res.status(400).json({ error: 'Invalid status' });
    }

    const result = await query(sql, [warehouseId]);
    res.json({ data: result.rows });
  } catch (error: any) {
    console.error('❌ Stock by status error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// Get movement history for a WSN
export const getMovementHistory = async (req: Request, res: Response) => {
  try {
    const { wsn } = req.query;

    if (!wsn) {
      return res.status(400).json({ error: 'WSN required' });
    }

    // Movement history returns warehouse_id per row — filter results to accessible warehouses
    const accessible = (req as any).accessibleWarehouses;

    const sql = `
      SELECT 
        'Inbound' as stage,
        i.inbound_date as date,
        i.vehicle_no,
        CONCAT('Received at warehouse') as details,
        i.warehouse_id
      FROM inbound i
      WHERE i.wsn = $1
      
      UNION ALL
      
      SELECT 
        'QC' as stage,
        q.qc_date as date,
        NULL as vehicle_no,
        CONCAT('Status: ', q.qc_status, ', Grade: ', q.qc_grade) as details,
        q.warehouse_id
      FROM qc q
      WHERE q.wsn = $1
      
      UNION ALL
      
      SELECT 
        'Picking' as stage,
        p.picking_date as date,
        NULL as vehicle_no,
        CONCAT('Order ID: ', p.order_id) as details,
        p.warehouse_id
      FROM picking p
      WHERE p.wsn = $1
      
      UNION ALL
      
      SELECT 
        'Outbound' as stage,
        o.dispatch_date as date,
        o.vehicle_no,
        CONCAT('Dispatched - ', o.dispatch_remarks) as details,
        o.warehouse_id
      FROM outbound o
      WHERE o.wsn = $1
      
      ORDER BY date ASC
    `;

    const result = await query(sql, [wsn]);

    // Filter results to only show movement in accessible warehouses
    let filteredRows = result.rows;
    if (accessible !== null && accessible !== undefined) {
      const accessibleStrings = accessible.map(String);
      filteredRows = result.rows.filter((row: any) => accessibleStrings.includes(String(row.warehouse_id)));
    }

    res.json({ data: filteredRows });
  } catch (error: any) {
    console.error('❌ Movement history error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};
