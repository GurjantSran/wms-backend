// File Path = warehouse-backend/src/controllers/outbound.controller.ts
import { Request, Response } from 'express';
import { query, getPool } from '../config/database';
import { generateBatchId } from '../utils/helpers';
import * as XLSX from 'xlsx';
import ExcelJS from 'exceljs';
import fs from 'fs';
import fsPromises from 'fs/promises';
import { safeError } from '../utils/sanitizeError';
import { logChange, logChanges, logChangeSimple, logChangeSimpleBatch } from '../utils/changeLogger';
import { backupScheduler } from '../services/backupScheduler';

// ====== GET ALL EXISTING OUTBOUND WSNs (for duplicate checking) ======
export const getAllOutboundWSNs = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;
    const params: any[] = [];
    let whereSql = `wsn IS NOT NULL AND wsn != ''`;
    if (warehouseId) {
      params.push(warehouseId);
      whereSql += ` AND warehouse_id = $${params.length}`;
    }
    const result = await query(
      `SELECT DISTINCT UPPER(TRIM(wsn)) as wsn FROM outbound WHERE ${whereSql} ORDER BY wsn LIMIT 10000`,
      params
    );
    const wsns = result.rows.map((row: any) => row.wsn);
    res.json(wsns);
  } catch (error: any) {
    console.error('❌ Error fetching all outbound WSNs:', error);
    res.status(500).json({ error: 'Failed to fetch outbound WSNs' });
  }
};

// ====== GET PENDING WSNs FOR OUTBOUND (from PICKING/QC) ======
export const getPendingForOutbound = async (req: Request, res: Response) => {
  try {
    const { warehouseId, search: rawSearch } = req.query;
    const search = typeof rawSearch === 'string' ? rawSearch.trim().toUpperCase() : rawSearch;

    let sql = `
      SELECT 
        p.id, p.wsn, p.picked_date, p.rack_no, p.picked_by_name,
        m.product_title, m.brand, m.mrp, m.fsp,
        'PICKING' as source
      FROM picking p
      LEFT JOIN master_data m ON p.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE NOT EXISTS (SELECT 1 FROM outbound WHERE outbound.wsn = p.wsn)
    `;

    const params: any[] = [];
    let paramIndex = 1;

    if (warehouseId) {
      sql += ` AND p.warehouse_id = $${paramIndex}`;
      params.push(warehouseId);
      paramIndex++;
    }

    if (search) {
      sql += ` AND p.wsn = $${paramIndex}`;
      params.push(search);
      paramIndex++;
    }

    sql += `
      UNION ALL
      SELECT 
        q.id, q.wsn, q.qc_date as picked_date, q.rack_no, q.qc_by_name as picked_by_name,
        m.product_title, m.brand, m.mrp, m.fsp,
        'QC' as source
      FROM qc q
      LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE NOT EXISTS (SELECT 1 FROM outbound WHERE outbound.wsn = q.wsn)
        AND NOT EXISTS (SELECT 1 FROM picking WHERE picking.wsn = q.wsn)
    `;

    if (warehouseId) {
      sql += ` AND q.warehouse_id = $${paramIndex}`;
      params.push(warehouseId);
      paramIndex++;
    }

    if (search) {
      sql += ` AND q.wsn = $${paramIndex}`;
      params.push(search);
      paramIndex++;
    }

    sql += ` ORDER BY picked_date DESC LIMIT 1000`;

    const result = await query(sql, params);
    res.json(result.rows);
  } catch (error: any) {
    console.error('❌ Pending outbound error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== GET ALL AVAILABLE INVENTORY FOR OUTBOUND CACHING ======
export const getAvailableForOutbound = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;

    if (!warehouseId) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    console.log('📦 Loading available inventory for outbound cache, warehouse:', warehouseId);

    // Get all items from PICKING, QC, INBOUND that are NOT yet dispatched
    const sql = `
      -- Items from PICKING (not yet dispatched)
      SELECT 
        p.wsn, 'PICKING' as source,
        p.picking_date as picked_date, p.rack_no, p.picker_name as picked_by_name,
        m.wid, m.fsn, m.order_id, m.product_title, m.brand, m.cms_vertical,
        m.mrp, m.fsp, m.hsn_sac, m.igst_rate, m.fkt_link,
        m.wh_location, m.vrp, m.yield_value, m.p_type, m.p_size,
        m.fk_grade, m.fkqc_remark, m.invoice_date
      FROM picking p
      LEFT JOIN master_data m ON UPPER(TRIM(p.wsn)) = UPPER(TRIM(m.wsn)) AND m.deleted_at IS NULL
      WHERE p.warehouse_id = $1
        AND NOT EXISTS (SELECT 1 FROM outbound o WHERE UPPER(TRIM(o.wsn)) = UPPER(TRIM(p.wsn)))

      UNION ALL

      -- Items from QC (pass status, not in picking, not yet dispatched)
      SELECT 
        q.wsn, 'QC' as source,
        q.qc_date as picked_date, q.rack_no, q.qc_by_name as picked_by_name,
        m.wid, m.fsn, m.order_id, m.product_title, m.brand, m.cms_vertical,
        m.mrp, m.fsp, m.hsn_sac, m.igst_rate, m.fkt_link,
        m.wh_location, m.vrp, m.yield_value, m.p_type, m.p_size,
        m.fk_grade, m.fkqc_remark, m.invoice_date
      FROM qc q
      LEFT JOIN master_data m ON UPPER(TRIM(q.wsn)) = UPPER(TRIM(m.wsn)) AND m.deleted_at IS NULL
      WHERE q.warehouse_id = $1
        AND q.qc_status = 'Pass'
        AND NOT EXISTS (SELECT 1 FROM picking p WHERE UPPER(TRIM(p.wsn)) = UPPER(TRIM(q.wsn)))
        AND NOT EXISTS (SELECT 1 FROM outbound o WHERE UPPER(TRIM(o.wsn)) = UPPER(TRIM(q.wsn)))

      UNION ALL

      -- Items from INBOUND (not in QC, not in picking, not yet dispatched)
      SELECT 
        i.wsn, 'INBOUND' as source,
        i.inbound_date as picked_date, i.rack_no, i.created_user_name as picked_by_name,
        m.wid, m.fsn, m.order_id, m.product_title, m.brand, m.cms_vertical,
        m.mrp, m.fsp, m.hsn_sac, m.igst_rate, m.fkt_link,
        m.wh_location, m.vrp, m.yield_value, m.p_type, m.p_size,
        m.fk_grade, m.fkqc_remark, m.invoice_date
      FROM inbound i
      LEFT JOIN master_data m ON UPPER(TRIM(i.wsn)) = UPPER(TRIM(m.wsn)) AND m.deleted_at IS NULL
      WHERE i.warehouse_id = $1
        AND NOT EXISTS (SELECT 1 FROM qc q WHERE UPPER(TRIM(q.wsn)) = UPPER(TRIM(i.wsn)))
        AND NOT EXISTS (SELECT 1 FROM picking p WHERE UPPER(TRIM(p.wsn)) = UPPER(TRIM(i.wsn)))
        AND NOT EXISTS (SELECT 1 FROM outbound o WHERE UPPER(TRIM(o.wsn)) = UPPER(TRIM(i.wsn)))

      ORDER BY picked_date DESC
      LIMIT 5000
    `;

    const result = await query(sql, [warehouseId]);
    console.log(`✅ Found ${result.rows.length} available inventory items for outbound`);
    res.json(result.rows);
  } catch (error: any) {
    console.error('❌ Available inventory error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== GET SOURCE BY WSN (PICKING → QC → INBOUND) ======
export const getSourceByWSN = async (req: Request, res: Response) => {
  try {
    const { wsn: rawWsn, warehouseId } = req.query;

    if (!rawWsn || !warehouseId) {
      return res.status(400).json({ error: 'WSN and warehouse ID required' });
    }

    const wsn = typeof rawWsn === 'string' ? rawWsn.trim().toUpperCase() : rawWsn;

    // Check if already dispatched
    const dispatchedCheck = await query(
      'SELECT id, dispatch_date, customer_name FROM outbound WHERE wsn = $1 AND warehouse_id = $2 LIMIT 1',
      [wsn, warehouseId]
    );

    if (dispatchedCheck.rows.length > 0) {
      return res.status(409).json({
        error: 'WSN already dispatched',
        existingData: dispatchedCheck.rows[0],
        canUpdate: true
      });
    }

    // 1. Check PICKING TABLE first
    let sql = `
      SELECT 
        p.*,
        m.wid, m.fsn, m.order_id, m.fkqc_remark, m.fk_grade, m.product_title,
        m.hsn_sac, m.igst_rate, m.fsp, m.mrp, m.invoice_date, m.fkt_link,
        m.wh_location, m.brand, m.cms_vertical, m.vrp, m.yield_value, m.p_type, m.p_size,
        'PICKING' as source
      FROM picking p
      LEFT JOIN master_data m ON p.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE p.wsn = $1 AND p.warehouse_id = $2
      LIMIT 1
    `;
    let result = await query(sql, [wsn, warehouseId]);

    if (result.rows.length === 0) {
      // 2. Check QC TABLE
      sql = `
        SELECT 
          q.*,
          m.wid, m.fsn, m.order_id, m.fkqc_remark, m.fk_grade, m.product_title,
          m.hsn_sac, m.igst_rate, m.fsp, m.mrp, m.invoice_date, m.fkt_link,
          m.wh_location, m.brand, m.cms_vertical, m.vrp, m.yield_value, m.p_type, m.p_size,
          i.inbound_date, i.vehicle_no as inbound_vehicle_no, i.unload_remarks,
          'QC' as source
        FROM qc q
        LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL
        LEFT JOIN inbound i ON q.wsn = i.wsn
        WHERE q.wsn = $1 AND q.warehouse_id = $2
        LIMIT 1
      `;
      result = await query(sql, [wsn, warehouseId]);

      if (result.rows.length === 0) {
        // 3. Check INBOUND TABLE
        sql = `
          SELECT 
            i.*,
            m.wid, m.fsn, m.order_id, m.fkqc_remark, m.fk_grade, m.product_title,
            m.hsn_sac, m.igst_rate, m.fsp, m.mrp, m.invoice_date, m.fkt_link,
            m.wh_location, m.brand, m.cms_vertical, m.vrp, m.yield_value, m.p_type, m.p_size,
            'INBOUND' as source
          FROM inbound i
          LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL
          WHERE i.wsn = $1 AND i.warehouse_id = $2
          LIMIT 1
        `;
        result = await query(sql, [wsn, warehouseId]);

        if (result.rows.length === 0) {
          return res.status(404).json({ error: 'WSN not found in Picking, QC or Inbound' });
        }
      }
    }

    res.json(result.rows[0]);
  } catch (error: any) {
    console.error('❌ Get source by WSN error:', error);
    res.status(500).json({ error: 'Failed to fetch data' });
  }
};

// ====== CREATE SINGLE OUTBOUND ENTRY ======
export const createSingleEntry = async (req: Request, res: Response) => {
  const pool = getPool();
  const client = await pool.connect();

  try {
    const {
      wsn: rawWsn,
      dispatch_date,
      customer_name,
      vehicle_no,
      dispatch_remarks,
      other_remarks,
      warehouse_id,
      update_existing
    } = req.body;

    // Normalize WSN to uppercase
    const wsn = typeof rawWsn === 'string' ? rawWsn.trim().toUpperCase() : rawWsn;

    const userId = (req as any).user?.id;
    const userName = (req as any).user?.full_name ||
      (req as any).user?.name ||
      (req as any).user?.username ||
      'Unknown';

    console.log('📦 Creating single outbound entry:', { wsn, warehouse_id });

    // Start transaction for atomicity
    await client.query('BEGIN');

    // Check if WSN already dispatched (with row lock to prevent race conditions)
    const checkSql = `SELECT id, warehouse_id FROM outbound WHERE UPPER(wsn) = $1 LIMIT 1 FOR UPDATE`;
    const checkResult = await client.query(checkSql, [wsn]);

    if (checkResult.rows.length > 0) {
      const existingWarehouse = checkResult.rows[0].warehouse_id;

      if (existingWarehouse !== Number(warehouse_id)) {
        await client.query('ROLLBACK');
        return res.status(403).json({
          error: 'WSN already dispatched from different warehouse.',
          existingWarehouseId: existingWarehouse
        });
      }

      if (!update_existing) {
        await client.query('ROLLBACK');
        return res.status(409).json({
          error: 'Duplicate WSN - already dispatched',
          existingId: checkResult.rows[0].id,
          canUpdate: true
        });
      }

      // Update existing outbound entry
      const updateSql = `
        UPDATE outbound 
        SET dispatch_date = $1,
            customer_name = $2,
            vehicle_no = $3,
            dispatch_remarks = $4,
            other_remarks = $5
        WHERE id = $6
        RETURNING id, dispatch_date, customer_name, wsn, vehicle_no, dispatch_remarks, 
                  other_remarks, quantity, source, warehouse_id, warehouse_name, 
                  created_user_name, batch_id
      `;
      const updateResult = await client.query(updateSql, [
        dispatch_date,
        customer_name,
        vehicle_no,
        dispatch_remarks,
        other_remarks,
        checkResult.rows[0].id
      ]);

      // Log update for CCTV-style tracking
      await logChange(client, 'outbound', 'UPDATE', {
        id: checkResult.rows[0].id, wsn, newData: updateResult.rows[0]
      }, { userId, userName, warehouseId: warehouse_id });

      await client.query('COMMIT');
      console.log('✅ Outbound entry updated');
      return res.json({ ...updateResult.rows[0], action: 'updated' });
    }

    // Get warehouse name
    const whSql = `SELECT name FROM warehouses WHERE id = $1`;
    const whResult = await client.query(whSql, [warehouse_id]);
    const warehouseName = whResult.rows[0]?.name || '';

    // Fetch source data (PICKING → QC → INBOUND)
    let sourceData: any = null;
    let sourceType = '';

    // Check PICKING
    let sourceSql = `
      SELECT 
        p.*,
        m.wid, m.fsn, m.order_id, m.fkqc_remark, m.fk_grade, m.product_title,
        m.hsn_sac, m.igst_rate, m.fsp, m.mrp, m.invoice_date, m.fkt_link,
        m.wh_location, m.brand, m.cms_vertical, m.vrp, m.yield_value, m.p_type, m.p_size,
        'PICKING' as source
      FROM picking p
      LEFT JOIN master_data m ON p.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE p.wsn = $1 AND p.warehouse_id = $2
      LIMIT 1
    `;
    let sourceResult = await client.query(sourceSql, [wsn, warehouse_id]);

    if (sourceResult.rows.length > 0) {
      sourceData = sourceResult.rows[0];
      sourceType = 'PICKING';
    } else {
      // Check QC
      sourceSql = `
        SELECT 
          q.*,
          m.wid, m.fsn, m.order_id, m.fkqc_remark, m.fk_grade, m.product_title,
          m.hsn_sac, m.igst_rate, m.fsp, m.mrp, m.invoice_date, m.fkt_link,
          m.wh_location, m.brand, m.cms_vertical, m.vrp, m.yield_value, m.p_type, m.p_size,
          i.inbound_date, i.vehicle_no as inbound_vehicle_no, i.unload_remarks,
          'QC' as source
        FROM qc q
        LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL
        LEFT JOIN inbound i ON q.wsn = i.wsn
        WHERE q.wsn = $1 AND q.warehouse_id = $2
        LIMIT 1
      `;
      sourceResult = await client.query(sourceSql, [wsn, warehouse_id]);

      if (sourceResult.rows.length > 0) {
        sourceData = sourceResult.rows[0];
        sourceType = 'QC';
      } else {
        // Check INBOUND
        sourceSql = `
          SELECT 
            i.*,
            m.wid, m.fsn, m.order_id, m.fkqc_remark, m.fk_grade, m.product_title,
            m.hsn_sac, m.igst_rate, m.fsp, m.mrp, m.invoice_date, m.fkt_link,
            m.wh_location, m.brand, m.cms_vertical, m.vrp, m.yield_value, m.p_type, m.p_size,
            'INBOUND' as source
          FROM inbound i
          LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL
          WHERE i.wsn = $1 AND i.warehouse_id = $2
          LIMIT 1
        `;
        sourceResult = await client.query(sourceSql, [wsn, warehouse_id]);

        if (sourceResult.rows.length === 0) {
          await client.query('ROLLBACK');
          return res.status(404).json({ error: 'WSN not found in Picking, QC or Inbound' });
        }

        sourceData = sourceResult.rows[0];
        sourceType = 'INBOUND';
      }
    }

    // Insert into outbound with only existing columns
    const insertSql = `
      INSERT INTO outbound (
        dispatch_date, customer_name, wsn, vehicle_no, dispatch_remarks, other_remarks,
        quantity, source, warehouse_id, warehouse_name, created_user_name
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
      )
      RETURNING *
    `;

    const result = await client.query(insertSql, [
      dispatch_date,
      customer_name,
      wsn,
      vehicle_no || null,
      dispatch_remarks || null,
      other_remarks || null,
      1,
      sourceType,
      warehouse_id,
      warehouseName,
      userName
    ]);

    // Log insert for CCTV-style tracking
    await logChange(client, 'outbound', 'INSERT', {
      id: result.rows[0].id, wsn, newData: result.rows[0]
    }, { userId, userName, warehouseId: warehouse_id });

    // 🗑️ Delete dispatched WSN from picking table (no longer needed after dispatch)
    try {
      const delResult = await client.query(
        `DELETE FROM picking WHERE UPPER(wsn) = $1 AND warehouse_id = $2`,
        [wsn, warehouse_id]
      );
      if ((delResult.rowCount ?? 0) > 0) {
        await logChange(client, 'picking', 'DELETE', {
          wsn, oldData: { wsn, warehouse_id, reason: 'auto-removed after outbound dispatch' }
        }, { userId, userName, warehouseId: warehouse_id });
        console.log(`🗑️ Deleted WSN ${wsn} from picking (dispatched)`);
      }
    } catch (delErr) {
      console.warn('⚠️ Failed to delete picking entry after dispatch:', delErr);
      // Non-critical: don't fail the outbound entry
    }

    await client.query('COMMIT');
    console.log('✅ Single outbound entry created');

    // 📡 SSE: Notify picking page to refresh (cross-page broadcast)
    try {
      const { sseManager } = require('../services/sseManager');
      sseManager.broadcast(warehouse_id, 'picking', 'data-submitted', {
        successCount: 1,
        submittedWSNs: [wsn],
        submittedBy: userName,
      }, '');
    } catch { /* SSE broadcast is best-effort */ }

    res.status(201).json({ ...result.rows[0], action: 'created' });
  } catch (error: any) {
    await client.query('ROLLBACK').catch(() => { });
    console.error('❌ Create outbound error:', error);
    res.status(500).json({ error: safeError(error) });
  } finally {
    client.release();
  }
};

// ====== MULTI OUTBOUND ENTRY (WITH BATCH ID) ======
export const multiEntry = async (req: Request, res: Response) => {
  const pool = getPool();
  const client = await pool.connect();

  try {
    const { entries, warehouse_id } = req.body;
    const userId = (req as any).user?.id;
    const userName = (req as any).user?.full_name ||
      (req as any).user?.name ||
      (req as any).user?.username ||
      'Unknown';

    if (!entries || entries.length === 0) {
      client.release();
      return res.status(400).json({ error: 'No entries provided' });
    }

    // Get warehouse name
    const whSql = `SELECT name FROM warehouses WHERE id = $1`;
    const whResult = await client.query(whSql, [warehouse_id]);
    const warehouseName = whResult.rows[0]?.name || '';

    // ⚡ OPTIMIZED: Collect all WSNs first
    const allWSNs = entries
      .map((e: any) => e.wsn?.trim()?.toUpperCase())
      .filter(Boolean);

    if (allWSNs.length === 0) {
      client.release();
      return res.status(400).json({ error: 'No valid WSNs provided' });
    }

    // Begin transaction early so checks + inserts are atomic
    await client.query('BEGIN');

    // ⚡ BULK CHECK: Get existing outbound in ONE query
    const existingMap = new Map<string, boolean>();
    const checkRes = await client.query(
      `SELECT UPPER(wsn) as wsn FROM outbound WHERE UPPER(wsn) = ANY($1)`,
      [allWSNs]
    );
    checkRes.rows.forEach((row: any) => existingMap.set(row.wsn, true));

    // ⚡ BULK CHECK: Get source types in priority order (PICKING > QC > INBOUND)
    const sourceMap = new Map<string, string>();

    // Check PICKING
    const pickingRes = await client.query(
      `SELECT UPPER(wsn) as wsn FROM picking WHERE UPPER(wsn) = ANY($1) AND warehouse_id = $2`,
      [allWSNs, warehouse_id]
    );
    pickingRes.rows.forEach((row: any) => sourceMap.set(row.wsn, 'PICKING'));

    // Check QC (only for WSNs not in picking)
    const wsnNotInPicking = allWSNs.filter((wsn: string) => !sourceMap.has(wsn));
    if (wsnNotInPicking.length > 0) {
      const qcRes = await client.query(
        `SELECT UPPER(wsn) as wsn FROM qc WHERE UPPER(wsn) = ANY($1) AND warehouse_id = $2`,
        [wsnNotInPicking, warehouse_id]
      );
      qcRes.rows.forEach((row: any) => sourceMap.set(row.wsn, 'QC'));
    }

    // Check INBOUND (only for WSNs not in picking/qc)
    const wsnNotInQC = allWSNs.filter((wsn: string) => !sourceMap.has(wsn));
    if (wsnNotInQC.length > 0) {
      const inboundRes = await client.query(
        `SELECT UPPER(wsn) as wsn FROM inbound WHERE UPPER(wsn) = ANY($1) AND warehouse_id = $2`,
        [wsnNotInQC, warehouse_id]
      );
      inboundRes.rows.forEach((row: any) => sourceMap.set(row.wsn, 'INBOUND'));
    }

    // GENERATE BATCH ID - OUT_MULTI_YYYYMMDD_HHMMSS
    const now = new Date();
    const dateStr = now.toISOString().slice(0, 10).replace(/-/g, '');
    const timeStr = now.toTimeString().slice(0, 8).replace(/:/g, '');
    const batchId = `OUT_MULTI_${dateStr}_${timeStr}`;

    const results: any[] = [];

    // ⚡ VALIDATE: Filter valid entries for bulk insert
    const validEntries: any[] = [];
    const processedWSNs = new Set<string>();

    for (const entry of entries) {
      const wsn = entry.wsn?.trim()?.toUpperCase();
      if (!wsn) continue;

      // Check batch duplicate
      if (processedWSNs.has(wsn)) {
        results.push({ wsn, status: 'DUPLICATE', message: 'Duplicate in batch' });
        continue;
      }
      processedWSNs.add(wsn);

      // Check existing outbound
      if (existingMap.has(wsn)) {
        results.push({ wsn, status: 'DUPLICATE', message: 'WSN already dispatched' });
        continue;
      }

      // Check source exists
      const sourceType = sourceMap.get(wsn);
      if (!sourceType) {
        results.push({ wsn, status: 'NOT_FOUND', message: 'WSN not found in Picking/QC/Inbound' });
        continue;
      }

      validEntries.push({
        dispatch_date: entry.dispatch_date,
        customer_name: entry.customer_name,
        wsn,
        vehicle_no: entry.vehicle_no,
        dispatch_remarks: entry.dispatch_remarks,
        other_remarks: entry.other_remarks,
        quantity: entry.quantity || 1,
        source: sourceType,
        warehouse_id,
        warehouse_name: warehouseName,
        batch_id: batchId,
        created_user_name: userName,
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
          const offset = idx * 12;
          valuePlaceholders.push(
            `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12})`
          );
          values.push(
            entry.dispatch_date,
            entry.customer_name,
            entry.wsn,
            entry.vehicle_no,
            entry.dispatch_remarks,
            entry.other_remarks,
            entry.quantity,
            entry.source,
            entry.warehouse_id,
            entry.warehouse_name,
            entry.batch_id,
            entry.created_user_name
          );
        });

        const bulkSql = `
          INSERT INTO outbound (
            dispatch_date, customer_name, wsn, vehicle_no, dispatch_remarks, other_remarks,
            quantity, source, warehouse_id, warehouse_name, batch_id, created_user_name
          ) VALUES ${valuePlaceholders.join(', ')}
        `;

        try {
          await client.query(`SAVEPOINT out_batch_${i}`);
          await client.query(bulkSql, values);

          batch.forEach((entry) => {
            results.push({ wsn: entry.wsn, status: 'SUCCESS' });
            successCount++;
          });
        } catch (err: any) {
          // Fallback to individual inserts for this batch
          await client.query(`ROLLBACK TO SAVEPOINT out_batch_${i}`);
          console.log(`Outbound bulk insert failed for batch ${i}, falling back to individual`);

          for (const entry of batch) {
            try {
              await client.query(`SAVEPOINT out_entry_${entry.wsn.replace(/[^a-zA-Z0-9]/g, '_')}`);
              await client.query(`
                INSERT INTO outbound (
                  dispatch_date, customer_name, wsn, vehicle_no, dispatch_remarks, other_remarks,
                  quantity, source, warehouse_id, warehouse_name, batch_id, created_user_name
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
              `, [
                entry.dispatch_date, entry.customer_name, entry.wsn, entry.vehicle_no,
                entry.dispatch_remarks, entry.other_remarks, entry.quantity, entry.source,
                entry.warehouse_id, entry.warehouse_name, entry.batch_id, entry.created_user_name
              ]);
              results.push({ wsn: entry.wsn, status: 'SUCCESS' });
              successCount++;
            } catch (individualErr: any) {
              await client.query(`ROLLBACK TO SAVEPOINT out_entry_${entry.wsn.replace(/[^a-zA-Z0-9]/g, '_')}`);
              const msg = individualErr.code === '23505' ? 'WSN already dispatched (concurrent entry)' : individualErr.message;
              results.push({ wsn: entry.wsn, status: individualErr.code === '23505' ? 'DUPLICATE' : 'ERROR', message: msg });
            }
          }
        }
      }
    }

    // Log all successful inserts for CCTV-style tracking
    const successfulResults = results.filter((r: any) => r.status === 'SUCCESS');
    const submittedWSNs = successfulResults.map((r: any) => r.wsn);
    if (successfulResults.length > 0) {
      await logChanges(client, 'outbound', 'INSERT',
        successfulResults.map((r: any) => ({ wsn: r.wsn, newData: { wsn: r.wsn, batch_id: batchId, warehouse_id } })),
        { batchId, userId, userName, warehouseId: warehouse_id }
      );
    }

    // 🗑️ Delete dispatched WSNs from picking table (no longer needed after dispatch)
    if (submittedWSNs.length > 0) {
      try {
        const delResult = await client.query(
          `DELETE FROM picking WHERE UPPER(wsn) = ANY($1) AND warehouse_id = $2`,
          [submittedWSNs, warehouse_id]
        );
        if ((delResult.rowCount ?? 0) > 0) {
          await logChanges(client, 'picking', 'DELETE',
            submittedWSNs.map((wsn: string) => ({ wsn, oldData: { wsn, warehouse_id, reason: 'auto-removed after outbound dispatch' } })),
            { batchId, userId, userName, warehouseId: warehouse_id }
          );
          console.log(`🗑️ Deleted ${delResult.rowCount} WSN(s) from picking (dispatched via batch ${batchId})`);
        }
      } catch (delErr) {
        console.warn('⚠️ Failed to delete picking entries after dispatch:', delErr);
        // Non-critical: don't fail the outbound entries
      }
    }

    // Commit all successful inserts + picking deletions
    await client.query('COMMIT');

    // 📡 SSE: Broadcast to other devices on same warehouse+page
    if (successCount > 0) {
      try {
        const { sseManager } = require('../services/sseManager');
        const deviceId = req.headers['x-device-id'] as string || '';
        sseManager.broadcast(warehouse_id, 'outbound', 'data-submitted', {
          successCount,
          totalCount: entries.length,
          batchId,
          submittedWSNs,
          submittedBy: userName,
        }, deviceId);

        // 📡 Cross-page: Notify picking page to refresh (dispatched WSNs removed)
        sseManager.broadcast(warehouse_id, 'picking', 'data-submitted', {
          successCount,
          batchId,
          submittedWSNs,
          submittedBy: userName,
        }, ''); // empty skipDeviceId = notify ALL picking page clients
      } catch { /* SSE broadcast is best-effort */ }
    }

    res.json({
      batchId,
      totalCount: entries.length,
      successCount,
      results
    });
  } catch (error: any) {
    // Rollback entire transaction on unexpected error
    await client.query('ROLLBACK');
    console.error('❌ Multi Entry ERROR:', error);
    res.status(500).json({ error: safeError(error) });
  } finally {
    client.release();
  }
};

// ====== BULK UPLOAD (COMPLETE IMPLEMENTATION) ======
// ====== BULK UPLOAD - HIGHLY OPTIMIZED for 500K-5M rows ======
export const bulkUpload = async (req: Request, res: Response) => {
  const startTime = Date.now();
  let client: any = null;
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const warehouseId = req.body.warehouse_id;
    const bulkDispatchDate = req.body.dispatch_date || '';
    const bulkCustomerName = req.body.customer_name || '';
    const userId = (req as any).user?.id;
    const userName = (req as any).user?.full_name ||
      (req as any).user?.name ||
      (req as any).user?.username ||
      'Unknown';

    // Get warehouse name - use dedicated client for all DB ops (PgBouncer safe)
    client = await getPool().connect();
    await client.query('BEGIN');
    const whSql = `SELECT name FROM warehouses WHERE id = $1`;
    const whResult = await client.query(whSql, [warehouseId]);
    const warehouseName = whResult.rows[0]?.name || '';

    // Determine file type from extension
    const fileName = req.file.originalname?.toLowerCase() || '';
    const filePath = req.file.path; // Using disk storage now

    // Check file signature for file type detection
    let isCSV = fileName.endsWith('.csv');
    if (!isCSV) {
      // Read first 2 bytes to check for Excel signature (PK = 0x50, 0x4B)
      const fd = fs.openSync(filePath, 'r');
      const signatureBuffer = Buffer.alloc(2);
      fs.readSync(fd, signatureBuffer, 0, 2, 0);
      fs.closeSync(fd);
      isCSV = signatureBuffer[0] !== 0x50 || signatureBuffer[1] !== 0x4B;
    }

    const data: any[] = [];
    const headers: string[] = [];

    console.log(`📂 Parsing ${isCSV ? 'CSV' : 'Excel'} file: ${fileName}`);

    if (isCSV) {
      // Parse CSV file from disk
      const csvContent = await fsPromises.readFile(filePath, 'utf-8');
      const lines = csvContent.split(/\r?\n/).filter(line => line.trim());

      if (lines.length < 2) {
        return res.status(400).json({ error: 'CSV file must have at least a header row and one data row' });
      }

      // Parse header row
      const headerLine = lines[0];
      const csvHeaders = headerLine.split(',').map(h => h.trim().replace(/^"|"$/g, ''));
      csvHeaders.forEach(h => headers.push(h));

      // Parse data rows
      for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;

        const values: string[] = [];
        let current = '';
        let inQuotes = false;

        for (let j = 0; j < line.length; j++) {
          const char = line[j];
          if (char === '"') {
            inQuotes = !inQuotes;
          } else if (char === ',' && !inQuotes) {
            values.push(current.trim());
            current = '';
          } else {
            current += char;
          }
        }
        values.push(current.trim());

        const obj: any = {};
        for (let k = 0; k < headers.length; k++) {
          obj[headers[k] || `col_${k + 1}`] = values[k] || '';
        }
        data.push(obj);
      }
    } else {
      // Read Excel file using ExcelJS from disk
      const workbook = new ExcelJS.Workbook();
      await workbook.xlsx.readFile(filePath);
      const worksheet = workbook.worksheets[0];

      worksheet.eachRow((row, rowNumber) => {
        const values = row.values as any[];
        if (rowNumber === 1) {
          for (let i = 1; i < values.length; i++) {
            headers.push(String(values[i] ?? '').trim());
          }
        } else {
          const obj: any = {};
          for (let i = 1; i < values.length; i++) {
            obj[headers[i - 1] || `col_${i}`] = values[i];
          }
          data.push(obj);
        }
      });
    }

    if (data.length === 0) {
      return res.status(400).json({ error: 'Empty file' });
    }

    console.log(`📊 Parsed ${data.length} rows in ${Date.now() - startTime}ms`);

    // GENERATE BATCH ID
    const now = new Date();
    const dateStr = now.toISOString().slice(0, 10).replace(/-/g, '');
    const timeStr = now.toTimeString().slice(0, 8).replace(/:/g, '');
    const batchId = `OUT_BULK_${dateStr}_${timeStr}`;

    // Helper function to get value with multiple possible column names
    const getValue = (row: any, ...keys: string[]): string => {
      for (const key of keys) {
        if (row[key] !== undefined && row[key] !== null && row[key] !== '') {
          return String(row[key]).trim();
        }
      }
      return '';
    };

    // Helper function to parse date from various formats (Excel Date objects, strings, numbers)
    const parseDate = (row: any, ...keys: string[]): string => {
      for (const key of keys) {
        const val = row[key];
        if (val === undefined || val === null || val === '') continue;

        // Handle JavaScript Date objects (from ExcelJS)
        if (val instanceof Date) {
          return val.toISOString().split('T')[0]; // Returns YYYY-MM-DD
        }

        // Handle Excel serial date numbers
        if (typeof val === 'number') {
          const jsDate = new Date((val - 25569) * 86400 * 1000);
          return jsDate.toISOString().split('T')[0];
        }

        // Handle string dates
        if (typeof val === 'string') {
          const str = val.trim();

          // Already in YYYY-MM-DD format
          if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
            return str;
          }

          // DD/MM/YYYY format
          if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(str)) {
            const parts = str.split('/');
            return `${parts[2]}-${parts[1].padStart(2, '0')}-${parts[0].padStart(2, '0')}`;
          }

          // MM/DD/YYYY format (US format)
          if (/^\d{1,2}\/\d{1,2}\/\d{2}$/.test(str)) {
            const parts = str.split('/');
            const year = parseInt(parts[2]) > 50 ? `19${parts[2]}` : `20${parts[2]}`;
            return `${year}-${parts[0].padStart(2, '0')}-${parts[1].padStart(2, '0')}`;
          }

          // Try parsing as Date string (handles "Tue Jan 20 2026..." format)
          const parsed = new Date(str);
          if (!isNaN(parsed.getTime())) {
            return parsed.toISOString().split('T')[0];
          }
        }
      }
      // Default to today's date if nothing found
      return new Date().toISOString().split('T')[0];
    };

    // Get all WSNs from file (normalize to uppercase)
    const wsns = data.map((row: any) => {
      const wsn = getValue(row, 'WSN', 'wsn', 'Wsn');
      return wsn ? wsn.toUpperCase().trim() : null;
    }).filter(Boolean) as string[];

    console.log(`🔍 Checking ${wsns.length} WSNs for duplicates...`);

    // BULK CHECK: Get all existing outbound WSNs in one query
    const existingSet = new Set<string>();
    if (wsns.length > 0) {
      const checkSql = `SELECT UPPER(TRIM(wsn)) as wsn FROM outbound WHERE UPPER(TRIM(wsn)) = ANY($1)`;
      const checkRes = await client.query(checkSql, [wsns]);
      checkRes.rows.forEach((row: any) => existingSet.add(row.wsn));
    }

    console.log(`✅ Found ${existingSet.size} existing WSNs, checking source data...`);

    // BULK FETCH: Get source data for all WSNs at once (PICKING → QC → INBOUND priority)
    const sourceMap = new Map<string, { source: string }>();

    // Fetch from PICKING in bulk
    if (wsns.length > 0) {
      const pickingSql = `SELECT UPPER(TRIM(wsn)) as wsn FROM picking WHERE UPPER(TRIM(wsn)) = ANY($1) AND warehouse_id = $2`;
      const pickingRes = await client.query(pickingSql, [wsns, warehouseId]);
      pickingRes.rows.forEach((row: any) => sourceMap.set(row.wsn, { source: 'PICKING' }));
    }

    // Fetch from QC in bulk (only for WSNs not in PICKING)
    const wsnsNotInPicking = wsns.filter(w => !sourceMap.has(w));
    if (wsnsNotInPicking.length > 0) {
      const qcSql = `SELECT UPPER(TRIM(wsn)) as wsn FROM qc WHERE UPPER(TRIM(wsn)) = ANY($1) AND warehouse_id = $2`;
      const qcRes = await client.query(qcSql, [wsnsNotInPicking, warehouseId]);
      qcRes.rows.forEach((row: any) => {
        if (!sourceMap.has(row.wsn)) sourceMap.set(row.wsn, { source: 'QC' });
      });
    }

    // Fetch from INBOUND in bulk (only for WSNs not in PICKING or QC)
    const wsnsNotInPickingOrQC = wsns.filter(w => !sourceMap.has(w));
    if (wsnsNotInPickingOrQC.length > 0) {
      const inboundSql = `SELECT UPPER(TRIM(wsn)) as wsn FROM inbound WHERE UPPER(TRIM(wsn)) = ANY($1) AND warehouse_id = $2`;
      const inboundRes = await client.query(inboundSql, [wsnsNotInPickingOrQC, warehouseId]);
      inboundRes.rows.forEach((row: any) => {
        if (!sourceMap.has(row.wsn)) sourceMap.set(row.wsn, { source: 'INBOUND' });
      });
    }

    console.log(`📦 Found source data for ${sourceMap.size} WSNs`);

    // Prepare data for batch insert
    const validRows: any[] = [];
    const errors: any[] = [];

    for (const row of data) {
      const wsn = getValue(row, 'WSN', 'wsn', 'Wsn').toUpperCase().trim();

      if (!wsn) {
        errors.push({ row: data.indexOf(row) + 2, error: 'Missing WSN' });
        continue;
      }

      if (existingSet.has(wsn)) {
        errors.push({ wsn, error: 'Duplicate - Already dispatched' });
        continue;
      }

      const sourceInfo = sourceMap.get(wsn);
      if (!sourceInfo) {
        errors.push({ wsn, error: 'WSN not found in Picking/QC/Inbound' });
        continue;
      }

      // Use bulk dropdown values if provided, otherwise fall back to Excel columns
      const dispatchDate = bulkDispatchDate || parseDate(row, 'DISPATCHDATE', 'DISPATCH_DATE', 'dispatchdate', 'dispatch_date', 'DispatchDate');

      validRows.push({
        wsn,
        dispatch_date: dispatchDate,
        customer_name: bulkCustomerName || getValue(row, 'CUSTOMERNAME', 'CUSTOMER_NAME', 'customername', 'customer_name', 'CustomerName'),
        vehicle_no: getValue(row, 'VEHICLENO', 'VEHICLE_NO', 'vehicleno', 'vehicle_no', 'VehicleNo'),
        dispatch_remarks: getValue(row, 'DISPATCHREMARKS', 'DISPATCH_REMARKS', 'dispatchremarks', 'dispatch_remarks', 'DispatchRemarks'),
        other_remarks: getValue(row, 'OTHERREMARKS', 'OTHER_REMARKS', 'otherremarks', 'other_remarks', 'OtherRemarks'),
        source: sourceInfo.source
      });
    }

    console.log(`✅ Validated ${validRows.length} rows, ${errors.length} errors`);

    if (validRows.length === 0) {
      return res.json({
        batchId,
        totalRows: data.length,
        successCount: 0,
        errorCount: errors.length,
        errors: errors.slice(0, 50),
        timestamp: new Date().toISOString()
      });
    }

    // BATCH INSERT using PostgreSQL unnest for maximum performance
    const BATCH_SIZE = 1000; // Insert 1000 rows at a time
    let successCount = 0;

    console.log(`🚀 Starting batch insert of ${validRows.length} rows...`);

    for (let i = 0; i < validRows.length; i += BATCH_SIZE) {
      const batch = validRows.slice(i, i + BATCH_SIZE);

      // Build arrays for unnest
      const dispatchDates: string[] = [];
      const customerNames: string[] = [];
      const wsnList: string[] = [];
      const vehicleNos: string[] = [];
      const dispatchRemarks: string[] = [];
      const otherRemarks: string[] = [];
      const quantities: number[] = [];
      const sources: string[] = [];
      const warehouseIds: number[] = [];
      const warehouseNames: string[] = [];
      const batchIds: string[] = [];
      const userNames: string[] = [];

      for (const row of batch) {
        dispatchDates.push(row.dispatch_date);
        customerNames.push(row.customer_name);
        wsnList.push(row.wsn);
        vehicleNos.push(row.vehicle_no);
        dispatchRemarks.push(row.dispatch_remarks);
        otherRemarks.push(row.other_remarks);
        quantities.push(1);
        sources.push(row.source);
        warehouseIds.push(warehouseId);
        warehouseNames.push(warehouseName);
        batchIds.push(batchId);
        userNames.push(userName);
      }

      // Use unnest for bulk insert
      const insertSql = `
        INSERT INTO outbound (
          dispatch_date, customer_name, wsn, vehicle_no, dispatch_remarks, other_remarks,
          quantity, source, warehouse_id, warehouse_name, batch_id, created_user_name
        )
        SELECT * FROM unnest(
          $1::date[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
          $7::int[], $8::text[], $9::int[], $10::text[], $11::text[], $12::text[]
        )
      `;

      try {
        await client.query(insertSql, [
          dispatchDates, customerNames, wsnList, vehicleNos, dispatchRemarks, otherRemarks,
          quantities, sources, warehouseIds, warehouseNames, batchIds, userNames
        ]);
        successCount += batch.length;
      } catch (batchError: any) {
        console.error(`❌ Batch insert error at rows ${i}-${i + batch.length}:`, batchError.message);
        // Fall back to individual inserts for this batch
        for (const row of batch) {
          try {
            await client.query(`
              INSERT INTO outbound (
                dispatch_date, customer_name, wsn, vehicle_no, dispatch_remarks, other_remarks,
                quantity, source, warehouse_id, warehouse_name, batch_id, created_user_name
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            `, [
              row.dispatch_date, row.customer_name, row.wsn, row.vehicle_no,
              row.dispatch_remarks, row.other_remarks, 1, row.source,
              warehouseId, warehouseName, batchId, userName
            ]);
            successCount++;
          } catch (rowError: any) {
            errors.push({ wsn: row.wsn, error: rowError.message });
          }
        }
      }

      // Log progress for large uploads
      if (validRows.length > 5000 && (i + BATCH_SIZE) % 10000 === 0) {
        console.log(`📊 Progress: ${Math.min(i + BATCH_SIZE, validRows.length)}/${validRows.length} rows inserted`);
      }
    }

    const totalTime = Date.now() - startTime;
    console.log(`✅ Bulk upload completed: ${successCount}/${data.length} rows in ${totalTime}ms`);

    // 🗑️ Delete dispatched WSNs from picking table (no longer needed after dispatch)
    const dispatchedWSNs = validRows.map((r: any) => r.wsn);
    if (dispatchedWSNs.length > 0) {
      try {
        const delResult = await client.query(
          `DELETE FROM picking WHERE UPPER(wsn) = ANY($1) AND warehouse_id = $2`,
          [dispatchedWSNs, warehouseId]
        );
        if ((delResult.rowCount ?? 0) > 0) {
          console.log(`🗑️ Deleted ${delResult.rowCount} WSN(s) from picking (dispatched via bulk upload ${batchId})`);
        }
      } catch (delErr) {
        console.warn('⚠️ Failed to delete picking entries after bulk dispatch:', delErr);
        // Non-critical: don't fail the bulk upload
      }
    }

    await client.query('COMMIT');

    // Trigger event backup (fire-and-forget)
    backupScheduler.triggerEventBackup(`outbound bulk upload: ${successCount} entries`).catch(() => { });

    // 📡 Cross-page SSE: Notify picking page to refresh
    if (successCount > 0) {
      try {
        const { sseManager } = require('../services/sseManager');
        sseManager.broadcast(warehouseId, 'picking', 'data-submitted', {
          successCount,
          batchId,
          submittedWSNs: dispatchedWSNs,
          submittedBy: userName,
        }, '');
      } catch { /* SSE broadcast is best-effort */ }
    }

    res.json({
      batchId,
      totalRows: data.length,
      successCount,
      errorCount: errors.length,
      errors: errors.slice(0, 50),
      duration: `${totalTime}ms`,
      timestamp: new Date().toISOString()
    });
  } catch (error: any) {
    if (client) await client.query('ROLLBACK').catch(() => { });
    console.error('❌ Bulk upload error:', error);
    res.status(500).json({ error: safeError(error) });
  } finally {
    if (client) client.release();
    // 🧹 Clean up uploaded temp file
    if (req.file?.path) {
      try {
        await fsPromises.unlink(req.file.path);
      } catch (cleanupError) {
        console.warn('⚠️ Failed to clean up temp file:', req.file.path);
      }
    }
  }
};

// ====== GET OUTBOUND LIST - OPTIMIZED for 1M+ rows ======
export const getList = async (req: Request, res: Response) => {
  try {
    const {
      page = 1,
      limit = 100,
      search = '',
      warehouseId,
      source = '',
      customer = '',
      startDate = '',
      endDate = '',
      batchId = '',
      brand = '',
      category = ''
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

    const offset = (Number(page) - 1) * Number(limit);

    // Determine if we need master_data join
    const needsMasterJoin = Boolean(
      (search && search !== '') ||
      (brand && brand !== '') ||
      (category && category !== '')
    );

    let whereConditions: string[] = [];
    let countWhereConditions: string[] = [];
    const params: any[] = [];
    const countParams: any[] = [];
    let paramIndex = 1;
    let countParamIndex = 1;

    // Warehouse filter - apply restriction or requested ID
    if (accessibleWarehouses && accessibleWarehouses.length > 0) {
      if (warehouseId) {
        whereConditions.push(`o.warehouse_id = $${paramIndex}`);
        countWhereConditions.push(`o.warehouse_id = $${countParamIndex}`);
        params.push(warehouseId);
        countParams.push(warehouseId);
        paramIndex++;
        countParamIndex++;
      } else {
        // No specific warehouse requested, filter to user's accessible warehouses
        whereConditions.push(`o.warehouse_id = ANY($${paramIndex}::int[])`);
        countWhereConditions.push(`o.warehouse_id = ANY($${countParamIndex}::int[])`);
        params.push(accessibleWarehouses);
        countParams.push(accessibleWarehouses);
        paramIndex++;
        countParamIndex++;
      }
    } else if (warehouseId) {
      // No restrictions (super_admin/admin), but specific warehouse requested
      whereConditions.push(`o.warehouse_id = $${paramIndex}`);
      countWhereConditions.push(`o.warehouse_id = $${countParamIndex}`);
      params.push(warehouseId);
      countParams.push(warehouseId);
      paramIndex++;
      countParamIndex++;
    }

    if (search) {
      whereConditions.push(`(
        o.wsn ILIKE $${paramIndex} OR
        o.customer_name ILIKE $${paramIndex} OR
        o.vehicle_no ILIKE $${paramIndex} OR
        m.product_title ILIKE $${paramIndex} OR
        m.brand ILIKE $${paramIndex}
      )`);
      countWhereConditions.push(`(
        o.wsn ILIKE $${countParamIndex} OR
        o.customer_name ILIKE $${countParamIndex} OR
        o.vehicle_no ILIKE $${countParamIndex} OR
        m.product_title ILIKE $${countParamIndex} OR
        m.brand ILIKE $${countParamIndex}
      )`);
      params.push(`%${search}%`);
      countParams.push(`%${search}%`);
      paramIndex++;
      countParamIndex++;
    }

    if (source) {
      whereConditions.push(`o.source = $${paramIndex}`);
      countWhereConditions.push(`o.source = $${countParamIndex}`);
      params.push(source);
      countParams.push(source);
      paramIndex++;
      countParamIndex++;
    }

    if (customer) {
      whereConditions.push(`o.customer_name ILIKE $${paramIndex}`);
      countWhereConditions.push(`o.customer_name ILIKE $${countParamIndex}`);
      params.push(`%${customer}%`);
      countParams.push(`%${customer}%`);
      paramIndex++;
      countParamIndex++;
    }

    if (startDate) {
      whereConditions.push(`o.dispatch_date >= $${paramIndex}`);
      countWhereConditions.push(`o.dispatch_date >= $${countParamIndex}`);
      params.push(startDate);
      countParams.push(startDate);
      paramIndex++;
      countParamIndex++;
    }

    if (endDate) {
      whereConditions.push(`o.dispatch_date <= $${paramIndex}`);
      countWhereConditions.push(`o.dispatch_date <= $${countParamIndex}`);
      params.push(endDate);
      countParams.push(endDate);
      paramIndex++;
      countParamIndex++;
    }

    if (batchId) {
      whereConditions.push(`o.batch_id = $${paramIndex}`);
      countWhereConditions.push(`o.batch_id = $${countParamIndex}`);
      params.push(batchId);
      countParams.push(batchId);
      paramIndex++;
      countParamIndex++;
    }

    if (brand) {
      whereConditions.push(`m.brand ILIKE $${paramIndex}`);
      countWhereConditions.push(`m.brand ILIKE $${countParamIndex}`);
      params.push(`%${brand}%`);
      countParams.push(`%${brand}%`);
      paramIndex++;
      countParamIndex++;
    }

    if (category) {
      whereConditions.push(`m.cms_vertical ILIKE $${paramIndex}`);
      countWhereConditions.push(`m.cms_vertical ILIKE $${countParamIndex}`);
      params.push(`%${category}%`);
      countParams.push(`%${category}%`);
      paramIndex++;
      countParamIndex++;
    }

    const whereClause = whereConditions.length > 0
      ? `WHERE ${whereConditions.join(' AND ')}`
      : '';
    const countWhereClause = countWhereConditions.length > 0
      ? `WHERE ${countWhereConditions.join(' AND ')}`
      : '';

    // OPTIMIZED: Run count and ID queries in PARALLEL
    const countSql = needsMasterJoin
      ? `SELECT COUNT(*) as total FROM outbound o LEFT JOIN master_data m ON o.wsn = m.wsn AND m.deleted_at IS NULL ${countWhereClause}`
      : `SELECT COUNT(*) as total FROM outbound o ${countWhereClause}`;

    const idsSql = `
      SELECT o.id
      FROM outbound o
      ${needsMasterJoin ? 'LEFT JOIN master_data m ON o.wsn = m.wsn AND m.deleted_at IS NULL' : ''}
      ${whereClause}
      ORDER BY o.id DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
    params.push(Number(limit), offset);

    // Run both queries in parallel
    const [countResult, idsResult] = await Promise.all([
      query(countSql, countParams),
      query(idsSql, params)
    ]);

    const total = parseInt(countResult.rows[0].total);
    const ids = idsResult.rows.map((r: any) => r.id);

    // If no results, return empty
    if (ids.length === 0) {
      return res.json({
        data: [],
        total,
        page: Number(page),
        limit: Number(limit),
        totalPages: Math.ceil(total / Number(limit))
      });
    }

    // PHASE 2: Fetch full data for the IDs
    const dataSql = `
      SELECT 
        o.*,
        m.product_title, m.brand, m.cms_vertical, m.wid, m.fsn, m.order_id,
        m.fkqc_remark, m.fk_grade, m.hsn_sac, m.igst_rate, m.fsp, m.mrp,
        m.vrp, m.yield_value, m.invoice_date, m.fkt_link, m.wh_location,
        m.p_type, m.p_size
      FROM outbound o
      LEFT JOIN master_data m ON o.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE o.id = ANY($1)
      ORDER BY o.id DESC
    `;
    const result = await query(dataSql, [ids]);

    res.json({
      data: result.rows,
      total,
      page: Number(page),
      limit: Number(limit),
      totalPages: Math.ceil(total / Number(limit))
    });
  } catch (error: any) {
    console.error('❌ Get outbound list error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== GET CUSTOMERS ======
export const getCustomers = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;

    //console.log('===== GET CUSTOMERS REQUEST =====');
    //console.log('Warehouse ID:', warehouseId);

    if (!warehouseId) {
      console.log('ERROR: Warehouse ID missing');
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    // Return only customers that appear in outbound entries for this warehouse
    const sql = `
      SELECT DISTINCT customer_name
      FROM outbound
      WHERE warehouse_id = $1 AND customer_name IS NOT NULL AND customer_name != ''
      ORDER BY customer_name ASC
      LIMIT 1000
    `;

    const result = await query(sql, [warehouseId]);

    const customerNames = result.rows.map((r: any) => r.customer_name);

    res.json(customerNames);
  } catch (error: any) {
    console.error('===== ERROR IN GET CUSTOMERS =====');
    console.error('Error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== GET EXISTING WSNs ======
export const getExistingWSNs = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;
    const sql = `SELECT DISTINCT wsn FROM outbound WHERE warehouse_id = $1 LIMIT 10000`;
    const result = await query(sql, [warehouseId]);
    res.json(result.rows.map((r: any) => r.wsn));
  } catch (error: any) {
    console.error('❌ Get existing WSNs error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== GET BATCHES ======
export const getBatches = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;
    // Get accessible warehouses from middleware (user's allowed warehouses)
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;

    // Note: outbound table doesn't have created_at column
    // batch_id format: OUT_BULK_YYYYMMDD_HHMMSS - extract date from batch_id for display
    // Use MAX(id) for ordering (most recent inserts have higher IDs)
    let sql = `
      SELECT
        batch_id,
        COUNT(*) as count,
        MAX(dispatch_date) as last_updated,
        array_agg(DISTINCT warehouse_id) as warehouse_ids,
        MAX(created_user_name) as uploaded_by
      FROM outbound
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
    }

    sql += ` GROUP BY batch_id ORDER BY MAX(id) DESC LIMIT 500`;

    const result = await query(sql, params);

    // Fetch all warehouse names in one query for lookup
    const allWarehouseIds = [...new Set(result.rows.flatMap((r: any) => r.warehouse_ids || []))];
    let warehouseNameMap: Record<number, string> = {};
    if (allWarehouseIds.length > 0) {
      const whResult = await query(
        `SELECT id, name FROM warehouses WHERE id = ANY($1::int[])`,
        [allWarehouseIds]
      );
      warehouseNameMap = Object.fromEntries(whResult.rows.map((w: any) => [w.id, w.name]));
    }

    // Parse batch_id to extract upload date for display
    // batch_id format: OUT_BULK_YYYYMMDD_HHMMSS or similar patterns
    const batchesWithParsedDate = result.rows.map((row: any) => {
      let uploadDate = row.last_updated; // fallback to dispatch_date

      // Try to extract date from batch_id (e.g., OUT_BULK_20260121_134717)
      const match = row.batch_id?.match(/(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})/);
      if (match) {
        const [, year, month, day, hour, min, sec] = match;
        uploadDate = new Date(`${year}-${month}-${day}T${hour}:${min}:${sec}`).toISOString();
      }

      // Resolve warehouse names from IDs
      const warehouseNames = (row.warehouse_ids || [])
        .map((id: number) => warehouseNameMap[id] || `WH-${id}`)
        .join(', ');

      return {
        ...row,
        last_updated: uploadDate,
        uploaded_by: row.uploaded_by || 'Unknown',
        warehouse_names: warehouseNames
      };
    });

    res.json(batchesWithParsedDate);
  } catch (error: any) {
    console.error('❌ Get batches error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== DELETE BATCH ======
export const deleteBatch = async (req: Request, res: Response) => {
  try {
    const { batchId } = req.params;
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;

    // First check if the batch belongs to accessible warehouses
    if (accessibleWarehouses && accessibleWarehouses.length > 0) {
      const checkResult = await query(
        'SELECT DISTINCT warehouse_id FROM outbound WHERE batch_id = $1',
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

    // Log deleted data for CCTV-style tracking
    const beforeDelete = await query(
      'SELECT id, wsn, warehouse_id FROM outbound WHERE batch_id = $1',
      [batchId]
    );

    await query('DELETE FROM outbound WHERE batch_id = $1', [batchId]);

    // Log deletions asynchronously
    if (beforeDelete.rows.length > 0) {
      const userId = (req as any).user?.id;
      const userName = (req as any).user?.full_name || (req as any).user?.username || 'Unknown';
      const warehouseId = beforeDelete.rows[0]?.warehouse_id;
      Promise.resolve().then(async () => {
        await logChangeSimpleBatch('outbound', 'DELETE',
          beforeDelete.rows.map(row => ({ id: row.id, wsn: row.wsn, oldData: row })),
          { batchId, userId, userName, warehouseId }
        );
      }).catch(() => { });
    }

    // Trigger event backup for batch delete (fire-and-forget)
    backupScheduler.triggerEventBackup(`outbound batch delete`).catch(() => { });

    res.json({ message: 'Batch deleted successfully' });
  } catch (error: any) {
    console.error('❌ Delete batch error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== EXPORT TO EXCEL ======
export const exportToExcel = async (req: Request, res: Response) => {
  try {
    const {
      warehouseId,
      source = '',
      customer = '',
      startDate = '',
      endDate = '',
      batchId = ''
    } = req.query;

    let whereConditions: string[] = [];
    const params: any[] = [];
    let paramIndex = 1;

    if (warehouseId) {
      whereConditions.push(`o.warehouse_id = $${paramIndex}`);
      params.push(warehouseId);
      paramIndex++;
    }

    if (source) {
      whereConditions.push(`o.source = $${paramIndex}`);
      params.push(source);
      paramIndex++;
    }

    if (customer) {
      whereConditions.push(`o.customer_name ILIKE $${paramIndex}`);
      params.push(`%${customer}%`);
      paramIndex++;
    }

    if (startDate) {
      whereConditions.push(`o.dispatch_date >= $${paramIndex}`);
      params.push(startDate);
      paramIndex++;
    }

    if (endDate) {
      whereConditions.push(`o.dispatch_date <= $${paramIndex}`);
      params.push(endDate);
      paramIndex++;
    }

    if (batchId) {
      whereConditions.push(`o.batch_id = $${paramIndex}`);
      params.push(batchId);
      paramIndex++;
    }

    const whereClause = whereConditions.length > 0
      ? `WHERE ${whereConditions.join(' AND ')}`
      : '';

    const sql = `
      SELECT 
        o.wsn,
        o.customer_name,
        o.dispatch_date,
        o.vehicle_no,
        o.source,
        o.batch_id,
        o.dispatch_remarks,
        o.other_remarks,
        o.quantity,
        o.warehouse_id,
        o.created_user_name,
        m.product_title,
        m.brand,
        m.cms_vertical,
        m.wid,
        m.fsn,
        m.order_id,
        m.fkqc_remark,
        m.fk_grade,
        m.hsn_sac,
        m.igst_rate,
        m.fsp,
        m.mrp,
        m.vrp,
        m.yield_value,
        m.invoice_date,
        m.fkt_link,
        m.wh_location,
        m.p_type,
        m.p_size
      FROM outbound o
      LEFT JOIN master_data m ON o.wsn = m.wsn AND m.deleted_at IS NULL
      ${whereClause}
      ORDER BY o.dispatch_date DESC, o.id DESC
      LIMIT 50000
    `;

    const result = await query(sql, params);

    // Format rows for Excel: Capital headers, proper date format, numeric columns
    const HEADER_MAP: Record<string, string> = {
      wsn: 'WSN',
      customer_name: 'CUSTOMER NAME',
      dispatch_date: 'DISPATCH DATE',
      vehicle_no: 'VEHICLE NO',
      source: 'SOURCE',
      batch_id: 'BATCH ID',
      dispatch_remarks: 'DISPATCH REMARKS',
      other_remarks: 'OTHER REMARKS',
      quantity: 'QUANTITY',
      warehouse_id: 'WAREHOUSE ID',
      created_user_name: 'CREATED BY',
      product_title: 'PRODUCT TITLE',
      brand: 'BRAND',
      cms_vertical: 'CMS VERTICAL',
      wid: 'WID',
      fsn: 'FSN',
      order_id: 'ORDER ID',
      fkqc_remark: 'FKQC REMARK',
      fk_grade: 'FK GRADE',
      hsn_sac: 'HSN SAC',
      igst_rate: 'IGST RATE',
      fsp: 'FSP',
      mrp: 'MRP',
      vrp: 'VRP',
      yield_value: 'YIELD VALUE',
      invoice_date: 'INVOICE DATE',
      fkt_link: 'FKT LINK',
      wh_location: 'WH LOCATION',
      p_type: 'P TYPE',
      p_size: 'P SIZE',
    };

    // Numeric columns that should be exported as numbers, not text
    const NUMERIC_COLUMNS = ['hsn_sac', 'igst_rate', 'fsp', 'mrp', 'vrp', 'yield_value', 'quantity', 'warehouse_id'];

    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    const formattedRows = result.rows.map((row: any) => {
      const formatted: Record<string, any> = {};
      for (const [key, value] of Object.entries(row)) {
        const header = HEADER_MAP[key] || key.replace(/_/g, ' ').toUpperCase();

        // Format dispatch_date as DD-MMM-YYYY
        if (key === 'dispatch_date' && value) {
          const d = new Date(value as string);
          if (!isNaN(d.getTime())) {
            const day = String(d.getDate()).padStart(2, '0');
            const mon = months[d.getMonth()];
            const yyyy = d.getFullYear();
            formatted[header] = `${day}-${mon}-${yyyy}`;
          } else {
            formatted[header] = value;
          }
        }
        // Convert numeric columns to actual numbers
        else if (NUMERIC_COLUMNS.includes(key) && value !== null && value !== undefined && value !== '') {
          const num = Number(value);
          formatted[header] = isNaN(num) ? value : num;
        }
        else {
          formatted[header] = value;
        }
      }
      return formatted;
    });

    // Create Excel file with formatted data
    const worksheet = XLSX.utils.json_to_sheet(formattedRows);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, 'Outbound');

    const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });

    res.setHeader('Content-Disposition', 'attachment; filename=outbound_export.xlsx');
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.send(buffer);
  } catch (error: any) {
    console.error('❌ Export error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// GET BRANDS - from master_data
// ============================================
export const getBrands = async (req: Request, res: Response) => {
  try {
    const { warehouse_id } = req.query;

    let sql = `
      SELECT DISTINCT m.brand 
      FROM outbound o
      LEFT JOIN master_data m ON o.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE m.brand IS NOT NULL AND m.brand != ''
    `;

    const params: any[] = [];

    if (warehouse_id) {
      sql += ` AND o.warehouse_id = $1`;
      params.push(warehouse_id);
    }

    sql += ` ORDER BY m.brand LIMIT 1000`;

    const result = await query(sql, params);
    res.json(result.rows.map((r: any) => r.brand));
  } catch (error: any) {
    console.error('❌ Get brands error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// GET CATEGORIES - from master_data
// ============================================
export const getCategories = async (req: Request, res: Response) => {
  try {
    const { warehouse_id } = req.query;

    let sql = `
      SELECT DISTINCT m.cms_vertical 
      FROM outbound o
      LEFT JOIN master_data m ON o.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE m.cms_vertical IS NOT NULL AND m.cms_vertical != ''
    `;

    const params: any[] = [];

    if (warehouse_id) {
      sql += ` AND o.warehouse_id = $1`;
      params.push(warehouse_id);
    }

    sql += ` ORDER BY m.cms_vertical LIMIT 1000`;

    const result = await query(sql, params);
    res.json(result.rows.map((r: any) => r.cms_vertical));
  } catch (error: any) {
    console.error('❌ Get categories error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// GET SOURCES - from outbound table
// ============================================
export const getSources = async (req: Request, res: Response) => {
  try {
    const { warehouse_id } = req.query;

    let sql = `
      SELECT DISTINCT o.source
      FROM outbound o
      WHERE o.source IS NOT NULL AND o.source != ''
    `;

    const params: any[] = [];

    if (warehouse_id) {
      sql += ` AND o.warehouse_id = $1`;
      params.push(warehouse_id);
    }

    sql += ` ORDER BY o.source LIMIT 1000`;

    const result = await query(sql, params);
    res.json(result.rows.map((r: any) => r.source));
  } catch (error: any) {
    console.error('❌ Get sources error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// SAVE DRAFT - Save multi-entry draft to database
// ============================================
export const saveOutboundDraft = async (req: Request, res: Response) => {
  try {
    const { draft_data, warehouse_id, customer_name, dispatch_mode, common_date, draft_source } = req.body;
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
      INSERT INTO outbound_multi_entry_drafts (user_id, warehouse_id, draft_data, customer_name, dispatch_mode, common_date, row_count, draft_source, saved_at, updated_at)
      VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, $8, NOW(), NOW())
      ON CONFLICT (user_id, warehouse_id, draft_source)
      DO UPDATE SET
        draft_data = $3::jsonb,
        customer_name = $4,
        dispatch_mode = $5,
        common_date = $6,
        row_count = $7,
        saved_at = NOW(),
        updated_at = NOW()
      RETURNING id, saved_at, row_count
    `;

    const result = await query(sql, [
      userId,
      warehouse_id,
      JSON.stringify(draft_data),
      customer_name || '',
      dispatch_mode || '',
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
      sseManager.broadcast(warehouse_id, 'outbound', 'draft-updated', { userId, rowCount }, deviceId);
    } catch { /* best-effort */ }
  } catch (error: any) {
    console.error('❌ Save outbound draft error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// LOAD DRAFT - Load multi-entry draft from database
// ============================================
export const loadOutboundDraft = async (req: Request, res: Response) => {
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
      SELECT draft_data, customer_name, dispatch_mode, common_date, row_count, saved_at, updated_at
      FROM outbound_multi_entry_drafts
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
        dispatch_mode: draft.dispatch_mode || '',
        common_date: draft.common_date || '',
        row_count: draft.row_count || 0,
        saved_at: draft.saved_at,
        updated_at: draft.updated_at,
      },
    });
  } catch (error: any) {
    console.error('❌ Load outbound draft error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// CLEAR DRAFT - Delete multi-entry draft from database
// ============================================
export const clearOutboundDraft = async (req: Request, res: Response) => {
  try {
    const { warehouse_id } = req.query;
    const userId = (req as any).user?.userId || (req as any).user?.id;

    if (!userId) {
      return res.status(400).json({ error: 'User ID required' });
    }

    const source = req.query.draft_source === 'mobile' ? 'mobile' : 'desktop';

    let sql = `DELETE FROM outbound_multi_entry_drafts WHERE user_id = $1 AND draft_source = $3`;
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
        sseManager.broadcast(Number(warehouse_id), 'outbound', 'draft-cleared', { userId }, deviceId);
      }
    } catch { /* best-effort */ }
  } catch (error: any) {
    console.error('❌ Clear outbound draft error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// SYNC ROWS - Relay multi-entry row changes to same user's other devices via SSE
// No DB write — pure SSE relay for real-time cross-device sync
// ============================================
export const syncOutboundRows = async (req: Request, res: Response) => {
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
      console.log(`[SYNC-ROWS] outbound: userId=${userId}(type=${typeof userId}) warehouseId=${warehouseId} deviceId=${deviceId} rows=${rows.length}`);
      sseManager.broadcastToUser(Number(warehouseId), 'outbound', userId, 'entry-synced', {
        rows,
        userId,
      }, deviceId);
    } catch (err: any) { console.error('[SYNC-ROWS] outbound broadcast error:', err?.message); }

    res.json({ success: true });
  } catch (error: any) {
    console.error('❌ Sync outbound rows error:', error);
    res.status(500).json({ error: 'Failed to sync rows' });
  }
};

// ============================================
// SYNC HEADER - Relay multi-entry header fields (date, customer, vehicle) to same user's other devices via SSE
// No DB write — pure SSE relay for real-time cross-device sync
// ============================================
export const syncOutboundHeader = async (req: Request, res: Response) => {
  try {
    const { warehouseId, commonDate, selectedCustomer, commonVehicle } = req.body;
    const userId = (req as any).user?.userId || (req as any).user?.id;
    const deviceId = req.headers['x-device-id'] as string || '';

    if (!warehouseId) {
      return res.status(400).json({ error: 'warehouseId is required' });
    }

    // 📡 SSE: Relay header changes to same user's other devices only
    try {
      const { sseManager } = require('../services/sseManager');
      sseManager.broadcastToUser(Number(warehouseId), 'outbound', userId, 'header-updated', {
        commonDate,
        selectedCustomer,
        commonVehicle,
        userId,
      }, deviceId);
    } catch (err: any) { console.error('[SYNC-HEADER] outbound broadcast error:', err?.message); }

    res.json({ success: true });
  } catch (error: any) {
    console.error('❌ Sync outbound header error:', error);
    res.status(500).json({ error: 'Failed to sync header' });
  }
};

// ============================================
// SYNC DISPATCHING WSNS - Sync WSNs being entered in outbound multi-entry grid
// Purpose: Shows "Outbound in Process" status in inbound list
// ============================================
export const syncDispatchingWSNs = async (req: Request, res: Response) => {
  const client = await getPool().connect();
  try {
    const { wsns, warehouse_id, warehouseId } = req.body;
    const whId = warehouse_id || warehouseId;
    const userId = (req as any).user?.userId || (req as any).user?.id;

    if (!userId || !whId) {
      client.release();
      return res.status(400).json({ error: 'User ID and warehouse ID required' });
    }

    if (!Array.isArray(wsns)) {
      client.release();
      return res.status(400).json({ error: 'WSNs must be an array' });
    }

    // Filter out empty WSNs, normalize, and DEDUPLICATE
    const validWSNs = [...new Set(
      wsns
        .filter((w: string) => w && w.trim())
        .map((w: string) => w.trim().toUpperCase())
    )];

    await client.query('BEGIN');

    try {
      // Clear all WSNs for this user in this warehouse first
      await client.query(
        `DELETE FROM dispatching_wsns WHERE user_id = $1 AND warehouse_id = $2`,
        [userId, whId]
      );

      // Insert valid WSNs in chunks
      if (validWSNs.length > 0) {
        const CHUNK_SIZE = 200;
        for (let i = 0; i < validWSNs.length; i += CHUNK_SIZE) {
          const chunk = validWSNs.slice(i, i + CHUNK_SIZE);
          const values = chunk.map((_wsn: string, idx: number) => {
            const baseIdx = idx * 3;
            return `($${baseIdx + 1}, $${baseIdx + 2}, $${baseIdx + 3})`;
          }).join(', ');

          const params = chunk.flatMap((wsn: string) => [wsn, userId, whId]);

          await client.query(
            `INSERT INTO dispatching_wsns (wsn, user_id, warehouse_id) 
             VALUES ${values}
             ON CONFLICT (wsn, warehouse_id) DO UPDATE SET 
               user_id = EXCLUDED.user_id,
               updated_at = NOW()`,
            params
          );
        }
      }

      await client.query('COMMIT');

      res.json({
        success: true,
        synced: validWSNs.length,
        message: `Synced ${validWSNs.length} WSNs to dispatching state`
      });
    } catch (insertError) {
      await client.query('ROLLBACK');
      throw insertError;
    }
  } catch (error: any) {
    console.error('❌ Sync dispatching WSNs error:', error);
    res.status(500).json({ error: safeError(error) });
  } finally {
    client.release();
  }
};

// ============================================
// CLEAR DISPATCHING WSNS - Clear all WSNs for a user's session
// ============================================
export const clearDispatchingWSNs = async (req: Request, res: Response) => {
  try {
    const { warehouse_id, warehouseId } = req.body;
    const whId = warehouse_id || warehouseId;
    const userId = (req as any).user?.userId || (req as any).user?.id;

    if (!userId) {
      return res.status(400).json({ error: 'User ID required' });
    }

    let deleteSql = `DELETE FROM dispatching_wsns WHERE user_id = $1`;
    const params: any[] = [userId];

    if (whId) {
      deleteSql += ` AND warehouse_id = $2`;
      params.push(whId);
    }

    const result = await query(deleteSql, params);

    res.json({
      success: true,
      cleared: result.rowCount,
      message: `Cleared ${result.rowCount} WSNs from dispatching state`
    });
  } catch (error: any) {
    console.error('❌ Clear dispatching WSNs error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// GET DISPATCHING WSNS - Get all WSNs currently in dispatching state
// ============================================
export const getDispatchingWSNs = async (req: Request, res: Response) => {
  try {
    const { warehouse_id, warehouseId } = req.query;
    const whId = warehouse_id || warehouseId;

    let sql = `SELECT DISTINCT wsn FROM dispatching_wsns`;
    const params: any[] = [];

    if (whId) {
      sql += ` WHERE warehouse_id = $1`;
      params.push(whId);
    }

    sql += ' LIMIT 10000';
    const result = await query(sql, params);
    const wsns = result.rows.map((r: any) => r.wsn);

    res.json(wsns);
  } catch (error: any) {
    console.error('❌ Get dispatching WSNs error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};
