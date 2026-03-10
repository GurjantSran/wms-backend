// File Path = warehouse-backend/src/controllers/picking.controller.ts
import { Request, Response } from 'express';
import { query, getPool } from '../config/database';
import { safeError } from '../utils/sanitizeError';
import { logChanges, logChangeSimple } from '../utils/changeLogger';
import { backupScheduler } from '../services/backupScheduler';

// ====== GET SOURCE DATA BY WSN (QC → INBOUND → MASTER) ======
export const getSourceByWSN = async (req: Request, res: Response) => {
  try {
    const { wsn: rawWsn, warehouseId } = req.query;

    if (!rawWsn || !warehouseId) {
      return res.status(400).json({ error: 'WSN and warehouse ID required' });
    }

    // Normalize WSN to uppercase
    const wsn = typeof rawWsn === 'string' ? rawWsn.trim().toUpperCase() : rawWsn;

    // Priority 1: Check QC with master_data join
    let sql = `
      SELECT 
        q.*, 
        m.product_title, m.brand, m.cms_vertical, m.mrp, m.fsp,
        m.hsn_sac, m.igst_rate, m.p_type, m.p_size, m.vrp,
        m.wid, m.fsn, m.order_id, m.fkqc_remark, m.fk_grade,
        m.invoice_date, m.fkt_link, m.wh_location, m.yield_value,
        'QC' as source 
      FROM qc q 
      LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE q.wsn = $1 AND q.warehouse_id = $2 
      LIMIT 1
    `;
    let result = await query(sql, [wsn, warehouseId]);

    if (result.rows.length === 0) {
      // Priority 2: Check Inbound with master_data join
      sql = `
        SELECT 
          i.*, 
          m.product_title, m.brand, m.cms_vertical, m.mrp, m.fsp,
          m.hsn_sac, m.igst_rate, m.p_type, m.p_size, m.vrp,
          m.wid, m.fsn, m.order_id, m.fkqc_remark, m.fk_grade,
          m.invoice_date, m.fkt_link, m.wh_location, m.yield_value,
          'INBOUND' as source 
        FROM inbound i 
        LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL
        WHERE i.wsn = $1 AND i.warehouse_id = $2 
        LIMIT 1
      `;
      result = await query(sql, [wsn, warehouseId]);

      if (result.rows.length === 0) {
        // Priority 3: Check Master Data only
        sql = `SELECT m.*, 'MASTER' as source FROM master_data m WHERE m.wsn = $1 AND m.deleted_at IS NULL LIMIT 1`;
        result = await query(sql, [wsn]);

        if (result.rows.length === 0) {
          return res.status(404).json({ error: 'WSN not found in any table' });
        }
      }
    }

    const row = result.rows[0];

    res.json({
      wsn: row.wsn,

      // ---- MASTER DISPLAY FIELDS (GRID KE LIYE) ----
      product_title:
        row.product_title ||
        row.product_name ||
        row.title ||
        null,

      brand:
        row.brand ||
        row.brand_name ||
        null,

      mrp:
        row.mrp ||
        row.mrp_price ||
        null,

      fsp:
        row.fsp ||
        row.fsp_price ||
        null,

      cms_vertical:
        row.cms_vertical ||
        row.category ||
        null,

      p_type:
        row.p_type ||
        row.product_type ||
        null,

      p_size:
        row.p_size ||
        row.size ||
        null,

      vrp:
        row.vrp ||
        null,

      hsn_sac:
        row.hsn_sac ||
        null,

      igst_rate:
        row.igst_rate ||
        null,

      // ---- RACK INFO ----
      rack_no:
        row.rack_no ||
        null,

      // ---- QC SPECIFIC FIELDS ----
      fkqc_remark:
        row.fkqc_remark ||
        null,

      fk_grade:
        row.fk_grade ||
        null,

      // ---- INBOUND SPECIFIC FIELDS ----
      invoice_date:
        row.invoice_date ||
        null,

      fkt_link:
        row.fkt_link ||
        null,

      wh_location:
        row.wh_location ||
        null,

      yield_value:
        row.yield_value ||
        null,

      // ---- EXTRA IDENTIFIERS ----
      wid: row.wid || null,
      fsn: row.fsn || null,
      order_id: row.order_id || null,

      // ---- META ----
      source: row.source
    });

  } catch (error: any) {
    console.error('Get source by WSN error:', error);
    res.status(500).json({ error: safeError(error) });
  }

};

// ====== MULTI PICKING ENTRY WITH AUTO BATCH ID ======
export const multiPickingEntry = async (req: Request, res: Response) => {
  const pool = getPool();
  const client = await pool.connect();

  try {
    const { entries, warehouse_id } = req.body;

    if (!entries || !Array.isArray(entries) || entries.length === 0) {
      return res.status(400).json({ error: 'Entries array required' });
    }

    if (!warehouse_id) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    // Generate batch ID: PICK_MULTI_YYYYMMDD_HHMMSS
    const now = new Date();
    const dateStr = now.toISOString().slice(0, 10).replace(/-/g, '');
    const timeStr = now.toTimeString().slice(0, 8).replace(/:/g, '');
    const batchId = `PICK_MULTI_${dateStr}_${timeStr}`;

    const errors: any[] = [];

    // ⚡ OPTIMIZED: Collect all WSNs first
    const allWSNs = entries
      .map((e: any) => e.wsn?.trim()?.toUpperCase())
      .filter(Boolean);

    if (allWSNs.length === 0) {
      return res.status(400).json({ error: 'No valid WSNs provided' });
    }

    // ⚡ PARALLEL VALIDATION: Run all 3 checks simultaneously before transaction
    const existingPickingMap = new Map<string, boolean>();
    const sourceMap = new Map<string, string>();

    const [pickingCheckResult, qcResult, inboundResult] = await Promise.all([
      client.query(
        `SELECT UPPER(wsn) as wsn FROM picking WHERE UPPER(wsn) = ANY($1) AND warehouse_id = $2`,
        [allWSNs, warehouse_id]
      ),
      client.query(
        `SELECT UPPER(wsn) as wsn FROM qc WHERE UPPER(wsn) = ANY($1) AND warehouse_id = $2`,
        [allWSNs, warehouse_id]
      ),
      client.query(
        `SELECT UPPER(wsn) as wsn FROM inbound WHERE UPPER(wsn) = ANY($1) AND warehouse_id = $2`,
        [allWSNs, warehouse_id]
      ),
    ]);

    pickingCheckResult.rows.forEach((row: any) => existingPickingMap.set(row.wsn, true));
    // QC takes priority over Inbound
    qcResult.rows.forEach((row: any) => sourceMap.set(row.wsn, 'QC'));
    // Only set Inbound source if not already found in QC
    inboundResult.rows.forEach((row: any) => {
      if (!sourceMap.has(row.wsn)) sourceMap.set(row.wsn, 'INBOUND');
    });

    // Begin transaction for atomic inserts
    await client.query('BEGIN');

    // ⚡ VALIDATE: Filter valid entries for bulk insert
    const validEntries: any[] = [];
    const processedWSNs = new Set<string>();

    for (const entry of entries) {
      const wsn = entry.wsn?.trim()?.toUpperCase();

      if (!wsn) {
        errors.push({ wsn: entry.wsn || 'EMPTY', error: 'WSN required' });
        continue;
      }

      // Check batch duplicate
      if (processedWSNs.has(wsn)) {
        errors.push({ wsn, error: 'Duplicate in batch' });
        continue;
      }
      processedWSNs.add(wsn);

      // Check existing picking
      if (existingPickingMap.has(wsn)) {
        errors.push({ wsn, error: 'Already picked in this warehouse' });
        continue;
      }

      // Check source exists
      const sourceType = sourceMap.get(wsn);
      if (!sourceType) {
        errors.push({ wsn, error: 'WSN not found in QC/Inbound for this warehouse' });
        continue;
      }

      validEntries.push({
        picking_date: entry.picking_date || null,
        picker_name: entry.picker_name || null,
        customer_name: entry.customer_name || null,
        wsn,
        picking_remarks: entry.picking_remarks || null,
        quantity: entry.quantity || 1,
        source: sourceType,
        batch_id: batchId,
        warehouse_id,
        warehouse_name: entry.warehouse_name || null,
        other_remarks: entry.other_remarks || null,
        rack_no: entry.rack_no || null,
        created_user_name: entry.created_user_name || null,
      });
    }

    let successCount = 0;

    // ⚡ BULK INSERT: Insert in batches of 100
    if (validEntries.length > 0) {
      const BATCH_SIZE = 100;

      for (let i = 0; i < validEntries.length; i += BATCH_SIZE) {
        const batch = validEntries.slice(i, i + BATCH_SIZE);

        const values: any[] = [];
        const valuePlaceholders: string[] = [];

        batch.forEach((entry, idx) => {
          const offset = idx * 13;
          valuePlaceholders.push(
            `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12}, $${offset + 13}, NOW())`
          );
          values.push(
            entry.picking_date,
            entry.picker_name,
            entry.customer_name,
            entry.wsn,
            entry.picking_remarks,
            entry.quantity,
            entry.source,
            entry.batch_id,
            entry.warehouse_id,
            entry.warehouse_name,
            entry.other_remarks,
            entry.rack_no,
            entry.created_user_name
          );
        });

        const bulkSql = `
          INSERT INTO picking (
            picking_date, picker_name, customer_name, wsn, 
            picking_remarks, quantity, source, batch_id,
            warehouse_id, warehouse_name, other_remarks, rack_no, created_user_name, created_at
          ) VALUES ${valuePlaceholders.join(', ')}
        `;

        try {
          await client.query(`SAVEPOINT picking_batch_${i}`);
          await client.query(bulkSql, values);
          successCount += batch.length;
        } catch (err: any) {
          // Fallback to individual inserts for this batch
          await client.query(`ROLLBACK TO SAVEPOINT picking_batch_${i}`);
          console.log(`Picking bulk insert failed for batch ${i}, falling back to individual`);

          for (const entry of batch) {
            try {
              await client.query(`SAVEPOINT picking_entry_${entry.wsn.replace(/[^a-zA-Z0-9]/g, '_')}`);
              await client.query(`
                INSERT INTO picking (
                  picking_date, picker_name, customer_name, wsn, 
                  picking_remarks, quantity, source, batch_id,
                  warehouse_id, warehouse_name, other_remarks, rack_no, created_user_name, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
              `, [
                entry.picking_date, entry.picker_name, entry.customer_name, entry.wsn,
                entry.picking_remarks, entry.quantity, entry.source, entry.batch_id,
                entry.warehouse_id, entry.warehouse_name, entry.other_remarks, entry.rack_no,
                entry.created_user_name
              ]);
              successCount++;
            } catch (individualErr: any) {
              await client.query(`ROLLBACK TO SAVEPOINT picking_entry_${entry.wsn.replace(/[^a-zA-Z0-9]/g, '_')}`);
              errors.push({ wsn: entry.wsn, error: individualErr.code === '23505' ? 'Already picked (concurrent entry)' : individualErr.message });
            }
          }
        }
      }
    }

    // Commit all successful inserts FIRST, then do non-critical work
    await client.query('COMMIT');

    // Send response immediately - don't block on logging/SSE/backup
    res.json({
      success: true,
      batchId,
      successCount,
      totalCount: entries.length,
      errors: errors.length > 0 ? errors : undefined
    });

    // ⚡ FIRE-AND-FORGET: Log, SSE broadcast, and backup AFTER response sent
    Promise.resolve().then(async () => {
      // Log all successful inserts for CCTV-style tracking
      if (successCount > 0) {
        try {
          const successEntries = validEntries.filter(e => !errors.some((err: any) => err.wsn === e.wsn));
          await logChanges(client, 'picking', 'INSERT',
            successEntries.map(e => ({ wsn: e.wsn, newData: { wsn: e.wsn, batch_id: batchId, warehouse_id, customer_name: e.customer_name } })),
            { batchId, userId: (req as any).user?.id, userName: (req as any).user?.full_name || 'Unknown', warehouseId: warehouse_id }
          );
        } catch { /* logging is best-effort */ }
      }

      // SSE: Broadcast to other devices
      if (successCount > 0) {
        try {
          const { sseManager } = require('../services/sseManager');
          const deviceId = req.headers['x-device-id'] as string || '';
          sseManager.broadcast(warehouse_id, 'picking', 'data-submitted', {
            successCount,
            totalCount: entries.length,
            batchId,
            submittedBy: (req as any).user?.full_name || 'Unknown',
          }, deviceId);
        } catch { /* SSE broadcast is best-effort */ }
      }

      // Trigger event backup
      backupScheduler.triggerEventBackup(`picking multi-entry: ${successCount} entries`).catch(() => { });
    }).catch(() => { });
  } catch (error: any) {
    // Rollback entire transaction on unexpected error
    await client.query('ROLLBACK');
    console.error('Multi picking entry error:', error);
    res.status(500).json({ error: safeError(error) });
  } finally {
    client.release();
  }
};

// ====== GET PICKING LIST WITH FILTERS & PAGINATION ======
export const getPickingList = async (req: Request, res: Response) => {
  try {
    const {
      page = 1,
      limit = 50,
      warehouseId,
      search,
      source,
      brand,
      category,
      batchId,
      customer,
      startDate,
      endDate
    } = req.query;

    // Validate warehouse access - get accessible warehouses from middleware
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;

    // If user has warehouse restrictions, validate the requested warehouse
    if (accessibleWarehouses && accessibleWarehouses.length > 0 && warehouseId) {
      const requestedId = parseInt(warehouseId as string);
      if (!accessibleWarehouses.includes(requestedId)) {
        return res.status(403).json({ error: 'Access denied to this warehouse' });
      }
    }

    // Require warehouseId for non-super users
    if (!warehouseId && (!accessibleWarehouses || accessibleWarehouses.length === 0)) {
      // Super admin/admin without restrictions still needs warehouse
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    const offset = (Number(page) - 1) * Number(limit);
    const conditions: string[] = [];
    const params: any[] = [];
    let paramIndex = 1;

    // Warehouse filter - apply restriction or requested ID
    if (accessibleWarehouses && accessibleWarehouses.length > 0) {
      if (warehouseId) {
        conditions.push(`p.warehouse_id = $${paramIndex}`);
        params.push(warehouseId);
        paramIndex++;
      } else {
        // Filter to user's accessible warehouses
        conditions.push(`p.warehouse_id = ANY($${paramIndex}::int[])`);
        params.push(accessibleWarehouses);
        paramIndex++;
      }
    } else if (warehouseId) {
      conditions.push(`p.warehouse_id = $${paramIndex}`);
      params.push(warehouseId);
      paramIndex++;
    }

    if (search) {
      // Search across all relevant columns
      conditions.push(`(
        p.wsn ILIKE $${paramIndex} 
        OR m.product_title ILIKE $${paramIndex} 
        OR m.brand ILIKE $${paramIndex}
        OR m.cms_vertical ILIKE $${paramIndex}
        OR p.customer_name ILIKE $${paramIndex}
        OR p.picker_name ILIKE $${paramIndex}
        OR p.batch_id ILIKE $${paramIndex}
        OR p.rack_no ILIKE $${paramIndex}
        OR p.picking_remarks ILIKE $${paramIndex}
        OR m.fsn ILIKE $${paramIndex}
        OR m.order_id ILIKE $${paramIndex}
        OR CAST(m.mrp AS TEXT) ILIKE $${paramIndex}
        OR CAST(m.fsp AS TEXT) ILIKE $${paramIndex}
      )`);
      params.push(`%${search}%`);
      paramIndex++;
    }

    if (source) {
      conditions.push(`p.source = $${paramIndex}`);
      params.push(source);
      paramIndex++;
    }

    if (brand) {
      conditions.push(`m.brand ILIKE $${paramIndex}`);
      params.push(`%${brand}%`);
      paramIndex++;
    }

    if (category) {
      conditions.push(`m.cms_vertical ILIKE $${paramIndex}`);
      params.push(`%${category}%`);
      paramIndex++;
    }

    if (batchId) {
      conditions.push(`p.batch_id = $${paramIndex}`);
      params.push(batchId);
      paramIndex++;
    }

    // Customer filter
    if (customer) {
      conditions.push(`p.customer_name ILIKE $${paramIndex}`);
      params.push(`%${customer}%`);
      paramIndex++;
    }

    // Date range filters
    if (startDate) {
      conditions.push(`p.picking_date >= $${paramIndex}`);
      params.push(startDate);
      paramIndex++;
    }

    if (endDate) {
      conditions.push(`p.picking_date <= $${paramIndex}`);
      params.push(endDate);
      paramIndex++;
    }

    const whereClause = conditions.join(' AND ');

    const countSql = `
      SELECT COUNT(*) 
      FROM picking p 
      LEFT JOIN master_data m ON p.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE ${whereClause}
    `;
    const countResult = await query(countSql, params);
    const total = parseInt(countResult.rows[0].count);

    // Data query with ALL master_data columns
    const dataSql = `
      SELECT 
        p.id, p.picking_date, p.picker_name, p.customer_name, p.wsn, 
        p.picking_remarks, p.quantity, p.source, p.batch_id, p.warehouse_id, 
        p.warehouse_name, p.other_remarks, p.rack_no, p.created_user_name,
        m.product_title, m.brand, m.cms_vertical, m.mrp, m.fsp,
        m.wid, m.fsn, m.order_id, m.hsn_sac, m.igst_rate, 
        m.p_type, m.p_size, m.vrp, m.yield_value, m.wh_location,
        m.fkqc_remark, m.fk_grade, m.invoice_date, m.fkt_link
      FROM picking p 
      LEFT JOIN master_data m ON p.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE ${whereClause}
      ORDER BY p.id DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
    params.push(Number(limit), offset);
    const dataResult = await query(dataSql, params);

    res.json({
      data: dataResult.rows,
      pagination: {
        page: Number(page),
        limit: Number(limit),
        total,
        totalPages: Math.ceil(total / Number(limit))
      }
    });
  } catch (error: any) {
    console.error('Get picking list error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== GET CUSTOMERS - SIMPLE STRING ARRAY (EXACTLY LIKE OUTBOUND) ======
export const getCustomers = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;

    if (!warehouseId) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    let sql = `
      SELECT DISTINCT customer_name
      FROM picking
      WHERE warehouse_id = $1 AND customer_name IS NOT NULL AND customer_name != ''
      ORDER BY customer_name ASC
      LIMIT 100
    `;

    let result = await query(sql, [warehouseId]);

    // RETURN SIMPLE STRING ARRAY (SAME AS OUTBOUND)
    const customerNames = result.rows.map((r: any) => r.customer_name);

    res.json(customerNames);
  } catch (error: any) {
    res.status(500).json({ error: safeError(error) });
  }
};


// ====== CHECK WSN EXISTS ======
export const checkWSNExists = async (req: Request, res: Response) => {
  try {
    const { wsn, warehouseId } = req.query;

    if (!wsn || !warehouseId) {
      return res.status(400).json({ error: 'WSN and warehouse ID required' });
    }

    const sql = `SELECT COUNT(*) as count FROM picking WHERE wsn = $1 AND warehouse_id = $2`;
    const result = await query(sql, [wsn, warehouseId]);

    res.json({
      exists: parseInt(result.rows[0].count) > 0
    });
  } catch (error: any) {
    console.error('Check WSN exists error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== GET ALL EXISTING WSNs FOR DUPLICATE CHECK ======
export const getExistingWSNs = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;

    if (!warehouseId) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    const sql = `SELECT DISTINCT UPPER(TRIM(wsn)) as wsn FROM picking WHERE warehouse_id = $1 LIMIT 10000`;
    const result = await query(sql, [warehouseId]);

    res.json(result.rows.map((r: any) => r.wsn));
  } catch (error: any) {
    console.error('Get existing WSNs error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== GET BATCHES ======
export const getBatches = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;
    // Get accessible warehouses from middleware (user's allowed warehouses)
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;

    let sql = `
      SELECT 
        batch_id,
        COUNT(*) as count,
        MAX(created_at) as created_at,
        MIN(id) as id,
        STRING_AGG(DISTINCT warehouse_name, ', ') as warehouse_names,
        MAX(created_user_name) as uploaded_by
      FROM picking
      WHERE batch_id IS NOT NULL
    `;
    const params: any[] = [];
    let paramIndex = 1;

    // Apply warehouse filter - prioritize middleware restriction, then query param
    if (accessibleWarehouses && accessibleWarehouses.length > 0) {
      if (warehouseId) {
        const requestedId = parseInt(warehouseId as string);
        if (!accessibleWarehouses.includes(requestedId)) {
          return res.status(403).json({ error: 'Access denied to this warehouse' });
        }
        sql += ` AND warehouse_id = $${paramIndex}`;
        params.push(requestedId);
        paramIndex++;
      } else {
        sql += ` AND warehouse_id = ANY($${paramIndex}::int[])`;
        params.push(accessibleWarehouses);
        paramIndex++;
      }
    } else if (warehouseId) {
      sql += ` AND warehouse_id = $${paramIndex}`;
      params.push(warehouseId);
      paramIndex++;
    } else {
      // No warehouse specified and no restrictions - still require warehouse for non-admin
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    sql += ` GROUP BY batch_id ORDER BY MAX(created_at) DESC NULLS LAST LIMIT 500`;

    const result = await query(sql, params);

    // Normalize fields for consistent frontend formatting
    const rows = result.rows.map((r: any) => ({
      ...r,
      created_at: r.created_at ? new Date(r.created_at).toISOString() : null,
      last_updated: r.created_at ? new Date(r.created_at).toISOString() : null,
      warehouse_names: r.warehouse_names || null,
      uploaded_by: r.uploaded_by || null
    }));

    res.json(rows);
  } catch (error: any) {
    console.error('Get batches error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== DELETE BATCH ======
export const deleteBatch = async (req: Request, res: Response) => {
  try {
    const { batchId } = req.params;
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;

    if (!batchId) {
      return res.status(400).json({ error: 'Batch ID required' });
    }

    // First check if the batch belongs to accessible warehouses
    if (accessibleWarehouses && accessibleWarehouses.length > 0) {
      const checkResult = await query(
        'SELECT DISTINCT warehouse_id FROM picking WHERE batch_id = $1',
        [batchId]
      );

      if (checkResult.rows.length > 0) {
        const batchWarehouseIds = checkResult.rows.map((r: any) => r.warehouse_id);
        const hasAccess = batchWarehouseIds.every((wId: number) => accessibleWarehouses.includes(wId));
        if (!hasAccess) {
          return res.status(403).json({ error: 'Access denied: batch contains items from warehouses you cannot access' });
        }
      }
    }

    const sql = `DELETE FROM picking WHERE batch_id = $1 RETURNING *`;
    const result = await query(sql, [batchId]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Batch not found' });
    }

    // Log deletions asynchronously for CCTV-style tracking
    const userId = (req as any).user?.id;
    const userName = (req as any).user?.full_name || (req as any).user?.username || 'Unknown';
    const warehouseId = result.rows[0]?.warehouse_id;
    Promise.resolve().then(async () => {
      for (const row of result.rows) {
        await logChangeSimple('picking', 'DELETE', {
          id: row.id, wsn: row.wsn, oldData: row
        }, { batchId, userId, userName, warehouseId });
      }
    }).catch(() => { });

    res.json({
      message: 'Batch deleted successfully',
      deletedCount: result.rows.length
    });
  } catch (error: any) {
    console.error('Delete batch error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== GET BRANDS - from picking joined with master_data ======
export const getBrands = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;

    let sql = `
      SELECT DISTINCT m.brand 
      FROM picking p
      LEFT JOIN master_data m ON p.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE m.brand IS NOT NULL AND m.brand != ''
    `;

    const params: any[] = [];

    if (warehouseId) {
      sql += ` AND p.warehouse_id = $1`;
      params.push(warehouseId);
    }

    sql += ` ORDER BY m.brand LIMIT 1000`;

    const result = await query(sql, params);
    res.json(result.rows.map((r: any) => r.brand));
  } catch (error: any) {
    console.error('❌ Get picking brands error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== GET CATEGORIES - from picking joined with master_data ======
export const getCategories = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;

    let sql = `
      SELECT DISTINCT m.cms_vertical 
      FROM picking p
      LEFT JOIN master_data m ON p.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE m.cms_vertical IS NOT NULL AND m.cms_vertical != ''
    `;

    const params: any[] = [];

    if (warehouseId) {
      sql += ` AND p.warehouse_id = $1`;
      params.push(warehouseId);
    }

    sql += ` ORDER BY m.cms_vertical LIMIT 1000`;

    const result = await query(sql, params);
    res.json(result.rows.map((r: any) => r.cms_vertical));
  } catch (error: any) {
    console.error('❌ Get picking categories error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// SAVE DRAFT - Save multi-entry draft to database
// ============================================
export const savePickingDraft = async (req: Request, res: Response) => {
  try {
    const { draft_data, warehouse_id, customer_name, common_date, draft_source } = req.body;
    const userId = (req as any).user?.userId || (req as any).user?.id;

    if (!userId) {
      return res.status(400).json({ error: 'User ID required' });
    }

    if (!warehouse_id) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    if (!draft_data || !Array.isArray(draft_data)) {
      return res.status(400).json({ error: 'draft_data must be an array' });
    }

    const source = draft_source === 'mobile' ? 'mobile' : 'desktop';

    // Count rows that have actual WSN data
    const rowCount = draft_data.filter((r: any) => r.wsn && r.wsn.trim()).length;

    // Upsert: insert or update if exists (one draft per user per warehouse per source)
    const sql = `
      INSERT INTO picking_multi_entry_drafts (user_id, warehouse_id, draft_data, customer_name, common_date, row_count, draft_source, saved_at, updated_at)
      VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, NOW(), NOW())
      ON CONFLICT (user_id, warehouse_id, draft_source)
      DO UPDATE SET
        draft_data = $3::jsonb,
        customer_name = $4,
        common_date = $5,
        row_count = $6,
        saved_at = NOW(),
        updated_at = NOW()
      RETURNING id, saved_at, row_count
    `;

    const result = await query(sql, [
      userId,
      warehouse_id,
      JSON.stringify(draft_data),
      customer_name || '',
      common_date || '',
      rowCount,
      source,
    ]);

    res.json({
      success: true,
      id: result.rows[0]?.id,
      saved_at: result.rows[0]?.saved_at,
      row_count: result.rows[0]?.row_count,
    });

    // 📡 SSE: Notify other devices about draft update
    try {
      const { sseManager } = require('../services/sseManager');
      const deviceId = req.headers['x-device-id'] as string || '';
      sseManager.broadcast(warehouse_id, 'picking', 'draft-updated', { userId, rowCount }, deviceId);
    } catch { /* best-effort */ }
  } catch (error: any) {
    console.error('❌ Save picking draft error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// LOAD DRAFT - Load multi-entry draft from database
// ============================================
export const loadPickingDraft = async (req: Request, res: Response) => {
  try {
    const { warehouse_id } = req.query;
    const userId = (req as any).user?.userId || (req as any).user?.id;

    if (!userId) {
      return res.status(400).json({ error: 'User ID required' });
    }

    if (!warehouse_id) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    const sql = `
      SELECT draft_data, customer_name, common_date, row_count, saved_at, updated_at
      FROM picking_multi_entry_drafts
      WHERE user_id = $1 AND warehouse_id = $2 AND draft_source = $3
    `;

    const source = req.query.draft_source === 'mobile' ? 'mobile' : 'desktop';
    const result = await query(sql, [userId, warehouse_id, source]);

    if (result.rows.length === 0) {
      return res.json({ exists: false, draft: null });
    }

    const draft = result.rows[0];
    res.json({
      exists: true,
      draft: {
        rows: draft.draft_data || [],
        customer_name: draft.customer_name || '',
        common_date: draft.common_date || '',
        row_count: draft.row_count || 0,
        saved_at: draft.saved_at,
        updated_at: draft.updated_at,
      },
    });
  } catch (error: any) {
    console.error('❌ Load picking draft error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// CLEAR DRAFT - Delete multi-entry draft from database
// ============================================
export const clearPickingDraft = async (req: Request, res: Response) => {
  try {
    const { warehouse_id } = req.query;
    const userId = (req as any).user?.userId || (req as any).user?.id;

    if (!userId) {
      return res.status(400).json({ error: 'User ID required' });
    }

    const source = req.query.draft_source === 'mobile' ? 'mobile' : 'desktop';

    let sql = `DELETE FROM picking_multi_entry_drafts WHERE user_id = $1 AND draft_source = $3`;
    const params: any[] = [userId];

    if (warehouse_id) {
      sql += ` AND warehouse_id = $2`;
      params.push(warehouse_id);
    }
    params.push(source);

    const result = await query(sql, params);

    res.json({
      success: true,
      cleared: result.rowCount,
      message: `Draft cleared successfully`,
    });

    // 📡 SSE: Notify other devices about draft clear
    try {
      const { sseManager } = require('../services/sseManager');
      const deviceId = req.headers['x-device-id'] as string || '';
      if (warehouse_id) {
        sseManager.broadcast(Number(warehouse_id), 'picking', 'draft-cleared', { userId }, deviceId);
      }
    } catch { /* best-effort */ }
  } catch (error: any) {
    console.error('❌ Clear picking draft error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// SYNC ROWS - Relay multi-entry row changes to same user's other devices via SSE
// No DB write — pure SSE relay for real-time cross-device sync
// ============================================
export const syncPickingRows = async (req: Request, res: Response) => {
  try {
    const { rows, warehouseId } = req.body;
    const userId = (req as any).user?.userId || (req as any).user?.id;
    const deviceId = req.headers['x-device-id'] as string || '';

    if (!rows || !Array.isArray(rows) || !warehouseId) {
      return res.status(400).json({ error: 'rows (array) and warehouseId are required' });
    }

    // 📡 SSE: Relay row changes to same user's other devices only
    try {
      const { sseManager } = require('../services/sseManager');
      console.log(`[SYNC-ROWS] picking: userId=${userId}(type=${typeof userId}) warehouseId=${warehouseId} deviceId=${deviceId} rows=${rows.length}`);
      sseManager.broadcastToUser(Number(warehouseId), 'picking', userId, 'entry-synced', {
        rows,
        userId,
      }, deviceId);
    } catch (err: any) { console.error('[SYNC-ROWS] picking broadcast error:', err?.message); }

    res.json({ success: true });
  } catch (error: any) {
    console.error('❌ Sync picking rows error:', error);
    res.status(500).json({ error: 'Failed to sync rows' });
  }
};