// File Path = warehouse-backend/src/controllers/inbound.controller.ts
import { Request, Response } from 'express';
import { query, getPool } from '../config/database';
import { generateBatchId } from '../utils/helpers';
import * as XLSX from 'xlsx';
import ExcelJS from 'exceljs';
import fs from 'fs';
import { safeError } from '../utils/sanitizeError';
import { logChange, logChanges, logChangeSimple, logChangeSimpleBatch } from '../utils/changeLogger';
import { backupScheduler } from '../services/backupScheduler';

// ⚡ OPTIMIZED: LRU-style in-memory cache for count queries with size limit
const MAX_CACHE_SIZE = 500; // Prevent unbounded memory growth
const countCache = new Map<string, { count: number; timestamp: number }>();
const COUNT_CACHE_TTL = 5000; // 5 seconds TTL - balances freshness with speed

function getCachedCount(key: string): number | null {
  const cached = countCache.get(key);
  if (cached && Date.now() - cached.timestamp < COUNT_CACHE_TTL) {
    return cached.count;
  }
  // Clean up expired entry
  if (cached) {
    countCache.delete(key);
  }
  return null;
}

function setCachedCount(key: string, count: number): void {
  // ⚡ LRU eviction: Remove oldest entries when cache is full
  if (countCache.size >= MAX_CACHE_SIZE) {
    // Delete first 10% of entries (oldest due to Map insertion order)
    const deleteCount = Math.floor(MAX_CACHE_SIZE * 0.1);
    const keysToDelete = Array.from(countCache.keys()).slice(0, deleteCount);
    keysToDelete.forEach(k => countCache.delete(k));
  }
  countCache.set(key, { count, timestamp: Date.now() });
}

// ⚡ Periodic cache cleanup (every 60 seconds)
setInterval(() => {
  const now = Date.now();
  for (const [key, value] of countCache.entries()) {
    if (now - value.timestamp > COUNT_CACHE_TTL * 2) {
      countCache.delete(key);
    }
  }
}, 60000);

// SINGLE ENTRY 
export const createInboundEntry = async (req: Request, res: Response) => {
  const pool = getPool();
  const client = await pool.connect();

  try {
    const {
      wsn: rawWsn,
      inbound_date,
      vehicle_no,
      product_serial_number,
      rack_no,
      unload_remarks,
      warehouse_id,
      update_existing,
    } = req.body;

    // Convert WSN to uppercase for consistent matching with master_data
    const wsn = rawWsn?.trim()?.toUpperCase();

    const userId = (req as any).user?.id;
    const userName = (req as any).user?.full_name ||
      (req as any).user?.name ||
      (req as any).user?.username ||
      'Unknown';

    console.log('📦 Creating single inbound entry:', { wsn, warehouse_id });

    // Start transaction for atomicity
    await client.query('BEGIN');

    // Check if WSN exists in ANY warehouse - case insensitive (with row lock to prevent race conditions)
    const checkAnySql = `SELECT id, warehouse_id FROM inbound WHERE UPPER(wsn) = $1 LIMIT 1 FOR UPDATE`;
    const checkAnyResult = await client.query(checkAnySql, [wsn]);

    if (checkAnyResult.rows.length > 0) {
      const existingWarehouse = checkAnyResult.rows[0].warehouse_id;

      if (existingWarehouse !== Number(warehouse_id)) {
        await client.query('ROLLBACK');
        return res.status(403).json({
          error: 'WSN already inbound in different warehouse.',
          existingWarehouseId: existingWarehouse,
        });
      }

      if (!update_existing) {
        await client.query('ROLLBACK');
        return res.status(409).json({
          error: 'Duplicate WSN in same warehouse',
          existingId: checkAnyResult.rows[0].id,
        });
      }

      // Update existing - inbound fields only
      const updateSql = `
        UPDATE inbound 
        SET inbound_date = $1,
            vehicle_no = $2,
            product_serial_number = $3,
            rack_no = $4,
            unload_remarks = $5,
            updated_at = NOW()
        WHERE id = $6
        RETURNING *
      `;
      const updateResult = await client.query(updateSql, [
        inbound_date,
        vehicle_no,
        product_serial_number,
        rack_no,
        unload_remarks,
        checkAnyResult.rows[0].id,
      ]);

      // Log change for CCTV-style tracking
      await logChange(client, 'inbound', 'UPDATE', {
        id: checkAnyResult.rows[0].id,
        wsn,
        newData: updateResult.rows[0]
      }, { userId, userName, warehouseId: warehouse_id });

      await client.query('COMMIT');
      console.log('✅ Inbound entry updated');
      return res.json({ ...updateResult.rows[0], action: 'updated' });
    }

    // Get warehouse name
    const whSql = `SELECT name FROM warehouses WHERE id = $1`;
    const whResult = await client.query(whSql, [warehouse_id]);
    const warehouseName = whResult.rows[0]?.name || '';

    // ✅ Check if WSN exists in master_data (product must be uploaded first)
    const masterCheckSql = `SELECT wsn FROM master_data WHERE UPPER(wsn) = $1 AND warehouse_id = $2 AND deleted_at IS NULL LIMIT 1`;
    const masterCheckResult = await client.query(masterCheckSql, [wsn, warehouse_id]);
    if (masterCheckResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({
        error: `WSN "${wsn}" not found in master data. Please upload product data first before inbounding.`,
      });
    }

    // ✅ Insert ONLY inbound-specific fields
    const sql = `
      INSERT INTO inbound (
        wsn,
        inbound_date,
        vehicle_no,
        product_serial_number,
        rack_no,
        unload_remarks,
        warehouse_id,
        warehouse_name,
        created_by,
        created_user_name
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10
      )
      RETURNING *
    `;

    const result = await client.query(sql, [
      wsn,
      inbound_date,
      vehicle_no || null,
      product_serial_number || null,
      rack_no || null,
      unload_remarks || null,
      warehouse_id,
      warehouseName,
      userId,
      userName,
    ]);

    // Log change for CCTV-style tracking
    await logChange(client, 'inbound', 'INSERT', {
      id: result.rows[0].id,
      wsn,
      newData: result.rows[0]
    }, { userId, userName, warehouseId: warehouse_id });

    await client.query('COMMIT');
    console.log('✅ Single inbound entry created');
    res.status(201).json({ ...result.rows[0], action: 'created' });
  } catch (error: any) {
    await client.query('ROLLBACK').catch(() => { });
    console.error('❌ Create inbound error:', error);
    res.status(500).json({ error: safeError(error) });
  } finally {
    client.release();
  }
};


// GET MASTER DATA BY WSN
export const getMasterDataByWSN = async (req: Request, res: Response) => {
  try {
    let { wsn } = req.params;
    if (!wsn) return res.status(400).json({ error: 'WSN is required' });
    wsn = wsn.toUpperCase();
    console.log('🔍 Searching WSN:', wsn);

    // ⚡ EGRESS OPTIMIZATION: Select only needed columns instead of SELECT *
    const warehouseId = req.query.warehouseId;
    let sql = `SELECT wsn, wid, fsn, order_id, product_title, brand, mrp, fsp, 
                        hsn_sac, igst_rate, cms_vertical, fkt_link, p_type, p_size, 
                        vrp, yield_value, fk_grade, fkqc_remark, wh_location, batch_id
                 FROM master_data WHERE UPPER(wsn) = $1 AND deleted_at IS NULL`;
    const params: any[] = [wsn];
    if (warehouseId) {
      sql += ` AND warehouse_id = $2`;
      params.push(warehouseId);
    }
    sql += ` LIMIT 1`;
    const result = await query(sql, params);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'WSN not found in master data' });
    }

    res.json(result.rows[0]);
  } catch (error: any) {
    console.error('❌ Get master data error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};


// BULK UPLOAD - with date normalization + error tracking
export const bulkInboundUpload = async (req: Request, res: Response) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const { warehouse_id, inbound_date, rack_no } = req.body;
    const userId = (req as any).user?.id;
    const userName = (req as any).user?.full_name ||
      (req as any).user?.name ||
      (req as any).user?.username ||
      'Unknown';

    const filePath = req.file.path;

    // Use shared parser utility for safe Excel parsing
    const buffer = await fs.promises.readFile(filePath);
    const { parseExcelBuffer } = require('../utils/excelParser');
    const data: any[] = await parseExcelBuffer(Buffer.from(buffer));

    if (data.length === 0) {
      fs.unlinkSync(filePath);
      return res.status(400).json({ error: 'Excel file is empty' });
    }

    const batchId = generateBatchId('BULK');

    res.status(202).json({
      message: 'Upload started',
      batchId,
      totalRows: data.length,
      timestamp: new Date().toISOString(),
    });

    // Process in background
    processInboundBulk(data, batchId, warehouse_id, userId, userName, filePath, inbound_date, rack_no);
  } catch (error: any) {
    console.error('❌ Bulk upload error:', error);
    if (req.file?.path) {
      try {
        fs.unlinkSync(req.file.path);
      } catch (e) { }
    }
    res.status(500).json({ error: safeError(error) });
  }
};

// ✅ Date normalizer for any format No timezone issues
const normalizeDate = (val: any): string | null => {
  if (!val && val !== 0) return null;

  // ✅ Handle JS Date objects
  if (val instanceof Date) {
    // Convert JS Date to YYYY-MM-DD (UTC)
    const year = val.getUTCFullYear();
    const month = String(val.getUTCMonth() + 1).padStart(2, '0');
    const day = String(val.getUTCDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }


  // ✅ Excel SERIAL NUMBER - WITHOUT timezone conversion
  if (typeof val === 'number' && val > 0) {
    // Excel epoch: December 30, 1899
    // Use getUTCFullYear/Month/Date to avoid timezone issues
    const excelEpoch = new Date(Date.UTC(1899, 11, 30));
    const resultDate = new Date(excelEpoch.getTime() + val * 24 * 60 * 60 * 1000);

    const year = resultDate.getUTCFullYear();
    const month = String(resultDate.getUTCMonth() + 1).padStart(2, '0');
    const day = String(resultDate.getUTCDate()).padStart(2, '0');

    console.log(`📅 Excel Serial: ${val} → ${year}-${month}-${day}`);
    return `${year}-${month}-${day}`;
  }

  // ✅ STRING FORMAT
  if (typeof val === 'string') {
    const s = val.trim();
    if (!s) return null;

    // yyyy-mm-dd
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

    // dd-mm-yyyy
    if (/^\d{2}-\d{2}-\d{4}$/.test(s)) {
      const [d, m, y] = s.split('-');
      return `${y}-${m}-${d}`;
    }

    // dd/mm/yyyy
    if (/^\d{2}\/\d{2}\/\d{4}$/.test(s)) {
      const [d, m, y] = s.split('/');
      return `${y}-${m}-${d}`;
    }

    // ✅ dd-MMM-yy (e.g., "01-Dec-25") - IMPORTANT! Excel exports as text
    // Using case-insensitive and flexible regex
    const monthRegex = /^(\d{1,2})-([A-Za-z]{3})-(\d{2})$/;
    const monthMatch = s.match(monthRegex);
    if (monthMatch) {
      const months: any = {
        'jan': '01', 'january': '01',
        'feb': '02', 'february': '02',
        'mar': '03', 'march': '03',
        'apr': '04', 'april': '04',
        'may': '05',
        'jun': '06', 'june': '06',
        'jul': '07', 'july': '07',
        'aug': '08', 'august': '08',
        'sep': '09', 'september': '09',
        'oct': '10', 'october': '10',
        'nov': '11', 'november': '11',
        'dec': '12', 'december': '12'
      };

      const d = monthMatch[1].padStart(2, '0');
      const m = months[monthMatch[2].toLowerCase()];
      const yy = monthMatch[3];
      const y = yy.length === 2 ? `20${yy}` : yy;

      if (m) {
        console.log(`📅 Text Date: "${s}" → ${y}-${m}-${d}`);
        return `${y}-${m}-${d}`;
      }
    }

    // mm/dd/yyyy (US format)
    if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(s)) {
      const parts = s.split('/');
      if (parts[0].length <= 2 && parts[1].length <= 2) {
        const m = String(parts[0]).padStart(2, '0');
        const d = String(parts[1]).padStart(2, '0');
        const y = parts[2];
        return `${y}-${m}-${d}`;
      }
    }

    // ✅ Fallback: Try JavaScript Date parsing with UTC
    // Use UTC methods to avoid timezone conversion
    const parsed = new Date(s);
    if (!isNaN(parsed.getTime())) {
      const year = parsed.getUTCFullYear();
      const month = String(parsed.getUTCMonth() + 1).padStart(2, '0');
      const day = String(parsed.getUTCDate()).padStart(2, '0');
      console.log(`📅 Parsed Date: "${s}" → ${year}-${month}-${day}`);
      return `${year}-${month}-${day}`;
    }
  }

  return null;
};

const normalizeKey = (key: string): string => {
  return key
    .toLowerCase()
    .trim()
    .replace(/\s+/g, '_')
    .replace(/[^a-z0-9_]/g, '');
};

async function processInboundBulk(
  data: any[],
  batchId: string,
  warehouseId: string,
  userId: number,
  userName: string,
  filePath: string,
  bulkInboundDate?: string,
  bulkRackNo?: string
) {
  const CHUNK_SIZE = 500;
  let successCount = 0;
  let duplicateCount = 0;
  let crossWarehouseCount = 0;
  let errorCount = 0;
  const errorDetails: any[] = [];
  const duplicateWSNs: string[] = [];
  const crossWarehouseWSNs: string[] = [];

  const client = await getPool().connect();
  try {
    await client.query('BEGIN');
    // Get warehouse name
    const whSql = `SELECT name FROM warehouses WHERE id = $1`;
    const whResult = await client.query(whSql, [warehouseId]);
    const warehouseName = whResult.rows[0]?.name || '';

    // Collect WSN list - Convert to UPPERCASE for consistency
    const wsns = data
      .map((row: any) => row['WSN'] || row['wsn'])
      .filter(Boolean)
      .map((v: any) => String(v).trim().toUpperCase());

    // Check existing inbound - use case-insensitive comparison
    const existingMap = new Map<string, number>();
    if (wsns.length > 0) {
      const existingSql = `SELECT UPPER(wsn) as wsn, warehouse_id FROM inbound WHERE UPPER(wsn) = ANY($1)`;
      const existingResult = await client.query(existingSql, [wsns]);
      existingResult.rows.forEach((row: any) => {
        existingMap.set(row.wsn, row.warehouse_id);
      });
    }

    const validRows: any[] = [];

    for (let idx = 0; idx < data.length; idx++) {
      const row = data[idx];
      // Convert WSN to UPPERCASE for consistent storage and matching with master_data
      const wsn = String(row['WSN'] || row['wsn'] || '').trim().toUpperCase();

      if (!wsn) {
        errorCount++;
        errorDetails.push({
          row: idx + 2,
          wsn: 'EMPTY',
          error: 'WSN is required',
        });
        continue;
      }

      // Duplicate check
      if (existingMap.has(wsn)) {
        const existingWarehouseId = existingMap.get(wsn);
        if (existingWarehouseId !== Number(warehouseId)) {
          crossWarehouseWSNs.push(wsn);
          crossWarehouseCount++;
          errorDetails.push({
            row: idx + 2,
            wsn,
            error: `Already inbound in warehouse ${existingWarehouseId}`,
          });
          continue;
        } else {
          duplicateWSNs.push(wsn);
          duplicateCount++;
          errorDetails.push({
            row: idx + 2,
            wsn,
            error: 'Duplicate in same warehouse',
          });
          continue;
        }
      }

      const getColumnValue = (row: any, possibleNames: string[]): any => {
        const normalizedRow: any = {};
        Object.keys(row).forEach(key => {
          normalizedRow[normalizeKey(key)] = row[key];
        });

        for (const name of possibleNames) {
          const normalized = normalizeKey(name);
          if (normalizedRow[normalized] !== undefined && normalizedRow[normalized] !== null) {
            return normalizedRow[normalized];
          }
        }
        return null;
      };

      // Use bulk dropdown values if provided, otherwise fall back to Excel columns
      const inboundDateRaw = bulkInboundDate || getColumnValue(row, [
        'INBOUND_DATE',
        'Inbound Date',
        'inbound_date',
        'inbound date',
      ]);

      // normalizeDate will handle both NUMBER and STRING formats


      const vehicleNo = getColumnValue(row, [
        'VEHICLE_NO',
        'Vehicle No',
        'vehicle_no',
        'vehicle no',
      ]);


      const productSerialNumber = getColumnValue(row, [
        'PRODUCT_SERIAL_NUMBER',
        'Product Serial Number',
        'product_serial_number',
        'product serial number'
      ]);

      // Use bulk dropdown rack_no if provided, otherwise fall back to Excel column
      const rackNo = bulkRackNo || getColumnValue(row, [
        'RACK_NO',
        'Rack No',
        'rack_no',
        'rack no'
      ]);

      const unloadRemarks = getColumnValue(row, [
        'UNLOAD_REMARKS',
        'Unload Remarks',
        'unload_remarks',
        'unload remarks'
      ]);

      const normalizedDate = normalizeDate(inboundDateRaw);

      validRows.push({
        wsn,
        inbound_date: normalizedDate,
        vehicle_no: vehicleNo,
        product_serial_number: productSerialNumber,
        rack_no: rackNo,
        unload_remarks: unloadRemarks,
      });
    }

    console.log(
      `📊 Valid: ${validRows.length}, Duplicates: ${duplicateCount}, Cross-warehouse: ${crossWarehouseCount}, Errors: ${errorCount}`
    );

    // Insert in chunks
    for (let i = 0; i < validRows.length; i += CHUNK_SIZE) {
      const chunk = validRows.slice(i, i + CHUNK_SIZE);

      try {
        const valuesClauses: string[] = [];
        const params: any[] = [];
        let paramIndex = 1;

        for (const row of chunk) {
          const rowParams = [
            row.wsn,
            row.inbound_date || new Date().toISOString().slice(0, 10),
            row.vehicle_no,
            row.product_serial_number,
            row.rack_no,
            row.unload_remarks,
            warehouseId,
            warehouseName,
            batchId,
            userId,
            userName,
            new Date().toISOString(), // created_at
          ];

          const placeholders = rowParams
            .map(() => `$${paramIndex++}`)
            .join(', ');
          valuesClauses.push(`(${placeholders})`);
          params.push(...rowParams);
        }

        const sql = `
          INSERT INTO inbound (
            wsn,
            inbound_date,
            vehicle_no,
            product_serial_number,
            rack_no,
            unload_remarks,
            warehouse_id,
            warehouse_name,
            batch_id,
            created_by,
            created_user_name,
            created_at
          ) VALUES ${valuesClauses.join(', ')}
        `;

        const result = await client.query(sql, params);
        successCount += result.rowCount || 0;
      } catch (chunkError: any) {
        console.error('❌ Chunk error:', chunkError.message);
        throw chunkError; // Bubble up so transaction can ROLLBACK
      }
    }

    await client.query('COMMIT');
    console.log(
      `🎉 Batch ${batchId}: ${successCount} success, ${duplicateCount} duplicates, ${crossWarehouseCount} cross-warehouse, ${errorCount} errors`
    );
  } catch (error: any) {
    await client.query('ROLLBACK').catch(() => { });
    console.error('❌ Process bulk error:', error);
  } finally {
    client.release();
    try {
      fs.unlinkSync(filePath);
    } catch (e) { }
  }
}



// MULTI-ENTRY - inbound fields only (OPTIMIZED for large batches)
export const multiInboundEntry = async (req: Request, res: Response) => {
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

    // Begin transaction early so checks + inserts are atomic
    await client.query('BEGIN');

    // Get warehouse name
    const whSql = `SELECT name FROM warehouses WHERE id = $1`;
    const whResult = await client.query(whSql, [warehouse_id]);
    const warehouseName = whResult.rows[0]?.name || '';

    const wsns = entries
      .map((e: any) => e.wsn)
      .filter(Boolean)
      .map((v: any) => String(v).trim().toUpperCase());

    // Existing WSN check - single query for all WSNs
    const existingMap = new Map<string, number>();
    if (wsns.length > 0) {
      const checkSql = `SELECT UPPER(wsn) as wsn, warehouse_id FROM inbound WHERE UPPER(wsn) = ANY($1)`;
      const checkRes = await client.query(checkSql, [wsns]);

      checkRes.rows.forEach((row: any) => {
        existingMap.set(row.wsn, row.warehouse_id);
      });
    }

    const batchId = generateBatchId('MULTI');

    // ✅ Batch check: which WSNs exist in master_data?
    const masterDataExistSet = new Set<string>();
    if (wsns.length > 0) {
      const masterCheckSql = `SELECT UPPER(wsn) as wsn FROM master_data WHERE UPPER(wsn) = ANY($1) AND warehouse_id = $2 AND deleted_at IS NULL`;
      const masterCheckRes = await client.query(masterCheckSql, [wsns, warehouse_id]);
      masterCheckRes.rows.forEach((row: any) => {
        masterDataExistSet.add(row.wsn);
      });
    }

    const results: any[] = [];
    const validEntries: any[] = [];

    // ⚡ First pass: validate all entries and collect valid ones for bulk insert
    for (const entry of entries) {
      const wsn = entry.wsn?.trim()?.toUpperCase();
      if (!wsn) {
        results.push({
          wsn: 'EMPTY',
          status: 'ERROR',
          message: 'WSN required',
        });
        continue;
      }

      // ✅ Master data existence check — reject WSNs without product data
      if (!masterDataExistSet.has(wsn)) {
        results.push({
          wsn,
          status: 'ERROR',
          message: 'WSN not found in master data. Upload product data first.',
        });
        continue;
      }

      // Duplicate check
      if (existingMap.has(wsn)) {
        const existingWarehouseId = existingMap.get(wsn);
        if (existingWarehouseId !== Number(warehouse_id)) {
          results.push({
            wsn,
            status: 'DUPLICATE',
            message: `Already inbound in warehouse ${existingWarehouseId}`,
          });
        } else {
          results.push({
            wsn,
            status: 'DUPLICATE',
            message: 'WSN already inbound in this warehouse',
          });
        }
        continue;
      }

      // Check for duplicates within the batch itself
      const isDuplicateInBatch = validEntries.some(ve => ve.wsn === wsn);
      if (isDuplicateInBatch) {
        results.push({
          wsn,
          status: 'DUPLICATE',
          message: 'Duplicate WSN within batch',
        });
        continue;
      }

      const inboundDateRaw = entry.inbound_date || new Date().toISOString().slice(0, 10);

      validEntries.push({
        wsn,
        inbound_date: inboundDateRaw,
        vehicle_no: entry.vehicle_no || null,
        product_serial_number: entry.product_serial_number || null,
        rack_no: entry.rack_no || null,
        unload_remarks: entry.unload_remarks || null,
        warehouse_id,
        warehouse_name: warehouseName,
        batch_id: batchId,
        created_by: userId,
        created_user_name: userName,
      });
    }

    let successCount = 0;

    // ⚡ BULK INSERT: Insert all valid entries in batches of 100 for optimal performance
    if (validEntries.length > 0) {
      const BATCH_SIZE = 100;

      for (let i = 0; i < validEntries.length; i += BATCH_SIZE) {
        const batch = validEntries.slice(i, i + BATCH_SIZE);

        // Build bulk insert query with multiple VALUES
        const values: any[] = [];
        const valuePlaceholders: string[] = [];

        batch.forEach((entry, idx) => {
          const offset = idx * 12;
          valuePlaceholders.push(
            `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12})`
          );
          values.push(
            entry.wsn,
            entry.inbound_date,
            entry.vehicle_no,
            entry.product_serial_number,
            entry.rack_no,
            entry.unload_remarks,
            entry.warehouse_id,
            entry.warehouse_name,
            entry.batch_id,
            entry.created_by,
            entry.created_user_name,
            new Date().toISOString() // created_at
          );
        });

        const bulkSql = `
          INSERT INTO inbound (
            wsn,
            inbound_date,
            vehicle_no,
            product_serial_number,
            rack_no,
            unload_remarks,
            warehouse_id,
            warehouse_name,
            batch_id,
            created_by,
            created_user_name,
            created_at
          )
          VALUES ${valuePlaceholders.join(', ')}
        `;

        try {
          // Use savepoint for batch-level rollback
          await client.query(`SAVEPOINT batch_${i}`);
          await client.query(bulkSql, values);

          // Mark all entries in this batch as successful
          batch.forEach((entry) => {
            results.push({ wsn: entry.wsn, status: 'SUCCESS', message: 'Created' });
            successCount++;
          });
        } catch (err: any) {
          // Rollback to savepoint and fall back to individual inserts for this batch
          await client.query(`ROLLBACK TO SAVEPOINT batch_${i}`);
          console.log(`Bulk insert failed for batch starting at ${i}, falling back to individual inserts`);
          for (const entry of batch) {
            try {
              await client.query(`SAVEPOINT entry_${entry.wsn.replace(/[^a-zA-Z0-9]/g, '_')}`);
              await client.query(`
                INSERT INTO inbound (
                  wsn, inbound_date, vehicle_no, product_serial_number, rack_no,
                  unload_remarks, warehouse_id, warehouse_name, batch_id, created_by, created_user_name, created_at
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
              `, [
                entry.wsn, entry.inbound_date, entry.vehicle_no, entry.product_serial_number,
                entry.rack_no, entry.unload_remarks, entry.warehouse_id, entry.warehouse_name,
                entry.batch_id, entry.created_by, entry.created_user_name, new Date().toISOString()
              ]);
              results.push({ wsn: entry.wsn, status: 'SUCCESS', message: 'Created' });
              successCount++;
            } catch (individualErr: any) {
              await client.query(`ROLLBACK TO SAVEPOINT entry_${entry.wsn.replace(/[^a-zA-Z0-9]/g, '_')}`);
              results.push({
                wsn: entry.wsn,
                status: (individualErr as any).code === '23505' ? 'DUPLICATE' : 'ERROR',
                message: (individualErr as any).code === '23505' ? 'WSN already inbound (concurrent entry)' : individualErr.message,
              });
            }
          }
        }
      }
    }

    // Log all successful inserts for CCTV-style tracking
    const successfulEntries = results.filter((r: any) => r.status === 'SUCCESS');
    if (successfulEntries.length > 0) {
      await logChanges(client, 'inbound', 'INSERT',
        successfulEntries.map((r: any) => ({ wsn: r.wsn, newData: { wsn: r.wsn, batch_id: batchId, warehouse_id } })),
        { batchId, userId, userName, warehouseId: warehouse_id }
      );
    }

    // Commit all successful inserts
    await client.query('COMMIT');

    // 📡 SSE: Broadcast to other devices on same warehouse+page
    if (successCount > 0) {
      try {
        const { sseManager } = require('../services/sseManager');
        const deviceId = req.headers['x-device-id'] as string || '';
        const submittedWSNs = results.filter((r: any) => r.status === 'SUCCESS').map((r: any) => r.wsn);
        sseManager.broadcast(warehouse_id, 'inbound', 'data-submitted', {
          successCount,
          totalCount: entries.length,
          batchId,
          submittedWSNs,
          submittedBy: userName,
        }, deviceId);
      } catch { /* SSE broadcast is best-effort */ }
    }

    // Trigger event backup (fire-and-forget)
    backupScheduler.triggerEventBackup(`inbound multi-entry: ${successCount} entries`).catch(() => { });

    res.json({
      batchId,
      totalCount: entries.length,
      successCount,
      results,
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


// GET INBOUND LIST - OPTIMIZED with conditional master_data JOIN
export const getInboundList = async (req: Request, res: Response) => {
  try {
    const {
      page = 1,
      limit = 100,
      search = '',
      warehouseId,
      dateFrom,
      dateTo,
      category,
      brand,
      batchId,
      statusFilter,
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

    // Determine if we need master_data join (only for search/brand/category filters)
    const needsMasterJoin = Boolean(
      (search && search !== '') ||
      (brand && brand !== '') ||
      (category && category !== '')
    );

    // Determine if we need dispatching_wsns / outbound joins for status filter
    const needsStatusJoin = Boolean(statusFilter && statusFilter !== '');

    let whereConditions: string[] = [];
    let countWhereConditions: string[] = [];
    const params: any[] = [];
    const countParams: any[] = [];
    let paramIndex = 1;
    let countParamIndex = 1;

    // Warehouse filter - apply restriction or requested ID
    if (accessibleWarehouses && accessibleWarehouses.length > 0) {
      if (warehouseId) {
        whereConditions.push(`i.warehouse_id = $${paramIndex}`);
        countWhereConditions.push(`i.warehouse_id = $${countParamIndex}`);
        params.push(warehouseId);
        countParams.push(warehouseId);
        paramIndex++;
        countParamIndex++;
      } else {
        // No specific warehouse requested, filter to user's accessible warehouses
        whereConditions.push(`i.warehouse_id = ANY($${paramIndex}::int[])`);
        countWhereConditions.push(`i.warehouse_id = ANY($${countParamIndex}::int[])`);
        params.push(accessibleWarehouses);
        countParams.push(accessibleWarehouses);
        paramIndex++;
        countParamIndex++;
      }
    } else if (warehouseId) {
      // No restrictions (super_admin/admin), but specific warehouse requested
      whereConditions.push(`i.warehouse_id = $${paramIndex}`);
      countWhereConditions.push(`i.warehouse_id = $${countParamIndex}`);
      params.push(warehouseId);
      countParams.push(warehouseId);
      paramIndex++;
      countParamIndex++;
    }

    // Search filter (WSN or product title)
    if (search && search !== '') {
      whereConditions.push(
        `(i.wsn ILIKE $${paramIndex} OR m.product_title ILIKE $${paramIndex})`
      );
      countWhereConditions.push(
        `(i.wsn ILIKE $${countParamIndex} OR m.product_title ILIKE $${countParamIndex})`
      );
      params.push(`%${search}%`);
      countParams.push(`%${search}%`);
      paramIndex++;
      countParamIndex++;
    }

    // Date range filter
    if (dateFrom && dateTo) {
      whereConditions.push(
        `i.inbound_date >= $${paramIndex}::date AND i.inbound_date <= $${paramIndex + 1}::date`
      );
      countWhereConditions.push(
        `i.inbound_date >= $${countParamIndex}::date AND i.inbound_date <= $${countParamIndex + 1}::date`
      );
      params.push(dateFrom, dateTo);
      countParams.push(dateFrom, dateTo);
      paramIndex += 2;
      countParamIndex += 2;
    } else if (dateFrom) {
      whereConditions.push(`i.inbound_date >= $${paramIndex}::date`);
      countWhereConditions.push(`i.inbound_date >= $${countParamIndex}::date`);
      params.push(dateFrom);
      countParams.push(dateFrom);
      paramIndex++;
      countParamIndex++;
    } else if (dateTo) {
      whereConditions.push(`i.inbound_date <= $${paramIndex}::date`);
      countWhereConditions.push(`i.inbound_date <= $${countParamIndex}::date`);
      params.push(dateTo);
      countParams.push(dateTo);
      paramIndex++;
      countParamIndex++;
    }

    // Brand filter - from master_data
    if (brand && brand !== '') {
      whereConditions.push(`m.brand = $${paramIndex}`);
      countWhereConditions.push(`m.brand = $${countParamIndex}`);
      params.push(brand);
      countParams.push(brand);
      paramIndex++;
      countParamIndex++;
    }

    // Category filter - from master_data
    if (category && category !== '') {
      whereConditions.push(`m.cms_vertical = $${paramIndex}`);
      countWhereConditions.push(`m.cms_vertical = $${countParamIndex}`);
      params.push(category);
      countParams.push(category);
      paramIndex++;
      countParamIndex++;
    }

    // Status filter - requires dispatching_wsns / outbound JOINs
    if (statusFilter === 'outbound_in_process') {
      whereConditions.push(`dw.wsn IS NOT NULL`);
      countWhereConditions.push(`dw.wsn IS NOT NULL`);
    } else if (statusFilter === 'dispatched') {
      whereConditions.push(`ob.wsn IS NOT NULL`);
      countWhereConditions.push(`ob.wsn IS NOT NULL`);
    } else if (statusFilter === 'available') {
      whereConditions.push(`dw.wsn IS NULL AND ob.wsn IS NULL`);
      countWhereConditions.push(`dw.wsn IS NULL AND ob.wsn IS NULL`);
    }

    // Batch ID filter - supports multiple batch IDs (comma-separated or array)
    if (batchId && batchId !== '') {
      // Parse batch IDs - handle both array and comma-separated string
      let batchIdArray: string[];
      if (Array.isArray(batchId)) {
        batchIdArray = batchId.map(id => String(id)).filter(Boolean);
      } else {
        batchIdArray = String(batchId).split(',').map(id => id.trim()).filter(Boolean);
      }

      if (batchIdArray.length === 1) {
        // Single batch ID - use simple equality
        whereConditions.push(`i.batch_id = $${paramIndex}`);
        countWhereConditions.push(`i.batch_id = $${countParamIndex}`);
        params.push(batchIdArray[0]);
        countParams.push(batchIdArray[0]);
        paramIndex++;
        countParamIndex++;
      } else if (batchIdArray.length > 1) {
        // Multiple batch IDs - use ANY with array
        whereConditions.push(`i.batch_id = ANY($${paramIndex}::text[])`);
        countWhereConditions.push(`i.batch_id = ANY($${countParamIndex}::text[])`);
        params.push(batchIdArray);
        countParams.push(batchIdArray);
        paramIndex++;
        countParamIndex++;
      }
    }

    const whereClause =
      whereConditions.length > 0 ? 'WHERE ' + whereConditions.join(' AND ') : '';
    const countWhereClause =
      countWhereConditions.length > 0 ? 'WHERE ' + countWhereConditions.join(' AND ') : '';

    // OPTIMIZED: Use cached count when available (reduces DB round trips)
    // Normalize batchId for cache key (handle arrays properly)
    const batchIdCacheKey = Array.isArray(batchId) ? batchId.sort().join(',') : batchId;
    const countCacheKey = `inbound_${warehouseId}_${search}_${brand}_${category}_${dateFrom}_${dateTo}_${batchIdCacheKey}_${statusFilter || ''}`;
    let total = getCachedCount(countCacheKey);

    // Build JOIN fragments for count/IDs queries
    const statusJoinSql = needsStatusJoin
      ? `LEFT JOIN dispatching_wsns dw ON UPPER(i.wsn) = UPPER(dw.wsn) AND dw.warehouse_id = i.warehouse_id
         LEFT JOIN outbound ob ON UPPER(i.wsn) = UPPER(ob.wsn) AND ob.warehouse_id = i.warehouse_id`
      : '';

    const countSql = `SELECT COUNT(*) FROM inbound i
      ${needsMasterJoin ? 'LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = i.warehouse_id' : ''}
      ${statusJoinSql}
      ${countWhereClause}`;

    const idsSql = `
      SELECT i.id
      FROM inbound i
      ${needsMasterJoin ? 'LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = i.warehouse_id' : ''}
      ${statusJoinSql}
      ${whereClause}
      ORDER BY i.created_at DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
    params.push(Number(limit), offset);

    // Run queries - use cached count if available, otherwise run both
    let idsResult;
    if (total !== null) {
      // Count is cached, only fetch IDs
      idsResult = await query(idsSql, params);
    } else {
      // Run both in parallel
      const [countResult, fetchedIds] = await Promise.all([
        query(countSql, countParams),
        query(idsSql, params)
      ]);
      total = parseInt(countResult.rows[0].count);
      setCachedCount(countCacheKey, total);
      idsResult = fetchedIds;
    }

    const ids = idsResult.rows.map((r: any) => r.id);

    // If no results, return empty
    if (ids.length === 0) {
      return res.json({
        data: [],
        total,
        page: Number(page),
        limit: Number(limit),
        totalPages: Math.ceil(total / Number(limit)),
      });
    }

    // Fetch full data for the IDs with master_data join
    const dataSql = `
      SELECT 
        i.id,
        i.wsn,
        i.inbound_date,
        i.vehicle_no,
        i.rack_no,
        i.product_serial_number,
        i.unload_remarks,
        i.quantity,
        i.batch_id,
        i.warehouse_id,
        i.warehouse_name,        
        i.created_user_name,
        i.created_at,
        -- Master data fields
        m.wid,
        m.fsn,
        m.order_id,
        m.product_title,
        m.brand,
        m.cms_vertical,
        m.fsp,
        m.mrp,
        m.hsn_sac,
        m.igst_rate,
        m.invoice_date,
        m.fkt_link,
        m.p_type,
        m.p_size,
        m.vrp,
        m.yield_value,
        m.fkqc_remark,
        m.fk_grade,
        -- Outbound in Process status (WSN is currently in someone's outbound multi-entry grid)
        CASE WHEN dw.wsn IS NOT NULL THEN true ELSE false END AS outbound_in_process,
        -- Dispatched status (WSN has been dispatched via outbound)
        CASE WHEN ob.wsn IS NOT NULL THEN true ELSE false END AS dispatched
      FROM inbound i
      LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = i.warehouse_id
      LEFT JOIN dispatching_wsns dw ON UPPER(i.wsn) = UPPER(dw.wsn) AND dw.warehouse_id = i.warehouse_id
      LEFT JOIN outbound ob ON UPPER(i.wsn) = UPPER(ob.wsn) AND ob.warehouse_id = i.warehouse_id
      WHERE i.id = ANY($1)
      ORDER BY i.created_at DESC
    `;

    const result = await query(dataSql, [ids]);

    res.json({
      data: result.rows,
      total,
      page: Number(page),
      limit: Number(limit),
      totalPages: Math.ceil(total / Number(limit)),
    });
  } catch (error: any) {
    console.error('❌ Get inbound list error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};


// GET BATCHES
export const getInboundBatches = async (req: Request, res: Response) => {
  try {
    // Accept both warehouseId and warehouse_id parameter names
    const warehouseId = req.query.warehouseId || req.query.warehouse_id;
    // Get accessible warehouses from middleware (user's allowed warehouses)
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;

    let sql = `
      SELECT 
        batch_id, 
        COUNT(*) as count, 
        COALESCE(MAX(created_at), MAX(inbound_date::timestamp), NOW()) as last_updated,
        STRING_AGG(DISTINCT warehouse_name, ', ') as warehouse_names,
        MAX(created_user_name) as uploaded_by
      FROM inbound
      WHERE batch_id IS NOT NULL
    `;

    const params: any[] = [];
    let paramIndex = 1;

    // Apply warehouse filter - prioritize middleware restriction, then query param
    if (accessibleWarehouses && accessibleWarehouses.length > 0) {
      // User has specific warehouse restrictions
      if (warehouseId) {
        // Requested warehouse must be in user's allowed list
        const requestedId = parseInt(warehouseId as string);
        if (!accessibleWarehouses.includes(requestedId)) {
          return res.status(403).json({ error: 'Access denied to this warehouse' });
        }
        sql += ` AND warehouse_id = $${paramIndex}`;
        params.push(requestedId);
        paramIndex++;
      } else {
        // Filter to only user's accessible warehouses
        sql += ` AND warehouse_id = ANY($${paramIndex}::int[])`;
        params.push(accessibleWarehouses);
        paramIndex++;
      }
    } else if (warehouseId) {
      // No restrictions, but specific warehouse requested
      sql += ` AND warehouse_id = $${paramIndex}`;
      params.push(warehouseId);
      paramIndex++;
    }
    // If no accessibleWarehouses and no warehouseId, show all (super_admin/admin)

    sql += `
      GROUP BY batch_id
      ORDER BY last_updated DESC
      LIMIT 500
    `;

    const result = await query(sql, params);

    // Normalize for frontend
    const rows = result.rows.map((r: any) => ({
      ...r,
      last_updated: r.last_updated ? new Date(r.last_updated).toISOString() : null,
      warehouse_names: r.warehouse_names || null,
      uploaded_by: r.uploaded_by || null
    }));

    res.json(rows);
  } catch (error: any) {
    console.error('❌ Get batches error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};


// DELETE BATCH
export const deleteInboundBatch = async (req: Request, res: Response) => {
  try {
    const { batchId } = req.params;
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;

    // First check if the batch belongs to accessible warehouses
    if (accessibleWarehouses && accessibleWarehouses.length > 0) {
      const checkResult = await query(
        'SELECT DISTINCT warehouse_id FROM inbound WHERE batch_id = $1',
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

    // Log deleted data before deletion for CCTV-style tracking
    const beforeDelete = await query(
      'SELECT id, wsn, warehouse_id FROM inbound WHERE batch_id = $1',
      [batchId]
    );

    const result = await query(
      'DELETE FROM inbound WHERE batch_id = $1',
      [batchId]
    );

    // Log deletions asynchronously (fire-and-forget, never blocks response)
    if (beforeDelete.rows.length > 0) {
      const userId = (req as any).user?.id;
      const userName = (req as any).user?.full_name || (req as any).user?.username || 'Unknown';
      const warehouseId = beforeDelete.rows[0]?.warehouse_id;
      Promise.resolve().then(async () => {
        await logChangeSimpleBatch('inbound', 'DELETE',
          beforeDelete.rows.map(row => ({ id: row.id, wsn: row.wsn, oldData: row })),
          { batchId, userId, userName, warehouseId }
        );
      }).catch(() => { });
    }

    // Trigger event backup for batch delete (fire-and-forget)
    backupScheduler.triggerEventBackup(`inbound batch delete: ${result.rowCount} entries`).catch(() => { });

    res.json({
      message: 'Batch deleted',
      count: result.rowCount,
    });
  } catch (error: any) {
    console.error('❌ Delete batch error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};


// GET WAREHOUSE RACKS
export const getWarehouseRacks = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.params;

    const sql = `
      SELECT id, rack_name, rack_type, capacity, location
      FROM racks
      WHERE warehouse_id = $1 AND is_active = true
      ORDER BY rack_name
      LIMIT 500
    `;

    const result = await query(sql, [warehouseId]);
    res.json(result.rows);
  } catch (error: any) {
    console.error('❌ Get racks error:', error);
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
      FROM inbound i
      LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = i.warehouse_id
      WHERE m.brand IS NOT NULL AND m.brand != ''
    `;

    const params: any[] = [];

    if (warehouse_id) {
      sql += ` AND i.warehouse_id = $1`;
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
      FROM inbound i
      LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL AND m.warehouse_id = i.warehouse_id
      WHERE m.cms_vertical IS NOT NULL AND m.cms_vertical != ''
    `;

    const params: any[] = [];

    if (warehouse_id) {
      sql += ` AND i.warehouse_id = $1`;
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
// BULK CHECK INBOUND WSNS - Check which WSNs are already inbounded (single query)
// ============================================
export const bulkCheckInboundWSNs = async (req: Request, res: Response) => {
  try {
    const { wsns, warehouse_id } = req.body;

    if (!Array.isArray(wsns) || wsns.length === 0) {
      return res.status(400).json({ error: 'WSNs array is required' });
    }

    // Normalize & deduplicate
    const normalizedWSNs = [...new Set(
      wsns.filter((w: string) => w && w.trim()).map((w: string) => w.trim().toUpperCase())
    )];

    if (normalizedWSNs.length === 0) {
      return res.json({ alreadyInbounded: [], crossWarehouse: [] });
    }

    // Single query: check all WSNs at once
    const sql = `SELECT UPPER(wsn) as wsn, warehouse_id FROM inbound WHERE UPPER(wsn) = ANY($1)`;
    const result = await query(sql, [normalizedWSNs]);

    const alreadyInbounded: string[] = [];
    const crossWarehouse: string[] = [];

    result.rows.forEach((row: any) => {
      if (warehouse_id && row.warehouse_id !== Number(warehouse_id)) {
        crossWarehouse.push(row.wsn);
      } else {
        alreadyInbounded.push(row.wsn);
      }
    });

    res.json({ alreadyInbounded, crossWarehouse });
  } catch (error: any) {
    console.error('❌ Bulk check inbound WSNs error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// GET ALL INBOUND WSNS
// ============================================
export const getAllInboundWSNs = async (req: Request, res: Response) => {
  try {
    const sql = `SELECT DISTINCT wsn FROM inbound ORDER BY wsn ASC LIMIT 10000`;
    const result = await query(sql);

    const wsns = result.rows.map((r: any) => r.wsn);

    res.json(wsns);
  } catch (error: any) {
    console.error('❌ Error fetching inbound WSNs:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// SYNC RECEIVING WSNS - Track WSNs being scanned in multi-entry grid
// ============================================
export const syncReceivingWSNs = async (req: Request, res: Response) => {
  // Use a dedicated client so all transaction statements run on the same
  // PgBouncer backend connection (required for transaction-mode pooling).
  const client = await getPool().connect();
  try {
    const { wsns, warehouse_id } = req.body;
    const userId = (req as any).user?.userId || (req as any).user?.id;

    if (!userId || !warehouse_id) {
      client.release();
      return res.status(400).json({ error: 'User ID and warehouse ID required' });
    }

    // Validate wsns is an array
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

    // Start transaction on the dedicated client
    await client.query('BEGIN');

    try {
      // First, clear all WSNs for this user in this warehouse
      await client.query(
        `DELETE FROM receiving_wsns WHERE user_id = $1 AND warehouse_id = $2`,
        [userId, warehouse_id]
      );

      // If there are valid WSNs, insert them in chunks to avoid parameter limits
      if (validWSNs.length > 0) {
        const CHUNK_SIZE = 200; // Safe chunk size (200 * 3 = 600 params, well under PG limit)
        for (let i = 0; i < validWSNs.length; i += CHUNK_SIZE) {
          const chunk = validWSNs.slice(i, i + CHUNK_SIZE);
          const values = chunk.map((wsn: string, idx: number) => {
            const baseIdx = idx * 3;
            return `($${baseIdx + 1}, $${baseIdx + 2}, $${baseIdx + 3})`;
          }).join(', ');

          const params = chunk.flatMap((wsn: string) => [wsn, userId, warehouse_id]);

          await client.query(
            `INSERT INTO receiving_wsns (wsn, user_id, warehouse_id) 
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
        message: `Synced ${validWSNs.length} WSNs to receiving state`
      });
    } catch (insertError) {
      await client.query('ROLLBACK');
      throw insertError;
    }
  } catch (error: any) {
    console.error('❌ Sync receiving WSNs error:', error);
    res.status(500).json({ error: safeError(error) });
  } finally {
    client.release();
  }
};

// ============================================
// CLEAR RECEIVING WSNS - Clear all WSNs for a user's session
// ============================================
export const clearReceivingWSNs = async (req: Request, res: Response) => {
  try {
    const { warehouse_id } = req.body;
    const userId = (req as any).user?.userId || (req as any).user?.id;

    if (!userId) {
      return res.status(400).json({ error: 'User ID required' });
    }

    let deleteSql = `DELETE FROM receiving_wsns WHERE user_id = $1`;
    const params: any[] = [userId];

    if (warehouse_id) {
      deleteSql += ` AND warehouse_id = $2`;
      params.push(warehouse_id);
    }

    const result = await query(deleteSql, params);

    res.json({
      success: true,
      cleared: result.rowCount,
      message: `Cleared ${result.rowCount} WSNs from receiving state`
    });
  } catch (error: any) {
    console.error('❌ Clear receiving WSNs error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// GET RECEIVING WSNS - Get all WSNs currently in receiving state
// ============================================
export const getReceivingWSNs = async (req: Request, res: Response) => {
  try {
    const { warehouse_id } = req.query;

    let sql = `SELECT DISTINCT wsn FROM receiving_wsns`;
    const params: any[] = [];

    if (warehouse_id) {
      sql += ` WHERE warehouse_id = $1`;
      params.push(warehouse_id);
    }

    sql += ' LIMIT 10000';
    const result = await query(sql, params);
    const wsns = result.rows.map((r: any) => r.wsn);

    res.json(wsns);
  } catch (error: any) {
    console.error('❌ Get receiving WSNs error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// SAVE DRAFT - Save/update multi-entry draft to database
// ============================================
export const saveDraft = async (req: Request, res: Response) => {
  try {
    const { draft_data, warehouse_id, vehicle_no, common_date, source } = req.body;
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

    // Validate source parameter
    const draftSource = source === 'mobile' ? 'mobile' : 'desktop';

    // Count rows that have actual WSN data
    const rowCount = draft_data.filter((r: any) => r.wsn && r.wsn.trim()).length;

    // Upsert: insert or update if exists (one draft per user per warehouse per source)
    const sql = `
      INSERT INTO inbound_multi_entry_drafts (user_id, warehouse_id, draft_data, vehicle_no, common_date, row_count, draft_source, saved_at, updated_at)
      VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, NOW(), NOW())
      ON CONFLICT (user_id, warehouse_id, draft_source)
      DO UPDATE SET
        draft_data = $3::jsonb,
        vehicle_no = $4,
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
      vehicle_no || '',
      common_date || '',
      rowCount,
      draftSource,
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
      sseManager.broadcast(warehouse_id, 'inbound', 'draft-updated', {
        userId,
        rowCount,
      }, deviceId);
    } catch { /* best-effort */ }
  } catch (error: any) {
    console.error('❌ Save draft error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// LOAD DRAFT - Load multi-entry draft from database
// ============================================
export const loadDraft = async (req: Request, res: Response) => {
  try {
    const { warehouse_id, source } = req.query;
    const userId = (req as any).user?.userId || (req as any).user?.id;

    if (!userId) {
      return res.status(400).json({ error: 'User ID required' });
    }

    if (!warehouse_id) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    const draftSource = source === 'mobile' ? 'mobile' : 'desktop';

    const sql = `
      SELECT draft_data, vehicle_no, common_date, row_count, saved_at, updated_at
      FROM inbound_multi_entry_drafts
      WHERE user_id = $1 AND warehouse_id = $2 AND draft_source = $3
    `;

    const result = await query(sql, [userId, warehouse_id, draftSource]);

    if (result.rows.length === 0) {
      return res.json({ exists: false, draft: null });
    }

    const draft = result.rows[0];
    res.json({
      exists: true,
      draft: {
        rows: draft.draft_data || [],
        vehicle_no: draft.vehicle_no || '',
        common_date: draft.common_date || '',
        row_count: draft.row_count || 0,
        saved_at: draft.saved_at,
        updated_at: draft.updated_at,
      },
    });
  } catch (error: any) {
    console.error('❌ Load draft error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// CLEAR DRAFT - Delete multi-entry draft from database
// ============================================
export const clearDraft = async (req: Request, res: Response) => {
  try {
    const { warehouse_id, source } = req.query;
    const userId = (req as any).user?.userId || (req as any).user?.id;

    if (!userId) {
      return res.status(400).json({ error: 'User ID required' });
    }

    const draftSource = source === 'mobile' ? 'mobile' : 'desktop';

    let sql = `DELETE FROM inbound_multi_entry_drafts WHERE user_id = $1 AND draft_source = $2`;
    const params: any[] = [userId, draftSource];

    if (warehouse_id) {
      sql += ` AND warehouse_id = $3`;
      params.push(warehouse_id);
    }

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
        sseManager.broadcast(Number(warehouse_id), 'inbound', 'draft-cleared', { userId }, deviceId);
      }
    } catch { /* best-effort */ }
  } catch (error: any) {
    console.error('❌ Clear draft error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// SYNC ROWS - Relay multi-entry row changes to same user's other devices via SSE
// No DB write — pure SSE relay for real-time cross-device sync
// ============================================
export const syncRows = async (req: Request, res: Response) => {
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
      console.log(`[SYNC-ROWS] inbound: userId=${userId}(type=${typeof userId}) warehouseId=${warehouseId} deviceId=${deviceId} rows=${rows.length}`);
      sseManager.broadcastToUser(Number(warehouseId), 'inbound', userId, 'entry-synced', {
        rows,
        userId,
      }, deviceId);
    } catch (err: any) { console.error('[SYNC-ROWS] inbound broadcast error:', err?.message); }

    res.json({ success: true });
  } catch (error: any) {
    console.error('❌ Sync rows error:', error);
    res.status(500).json({ error: 'Failed to sync rows' });
  }
};

// ==================== PRINT QUEUE (for mobile devices) ====================
// Mobile phones submit print jobs to the DB. The Print Agent on the laptop
// polls the backend for pending jobs and prints them locally.

// Track when the Print Agent last polled (in-memory, per-process)
let lastAgentPollAt: number = 0;

// Health check — tells the mobile UI whether the Print Agent is actively polling
export const printProxyHealth = async (_req: Request, res: Response) => {
  const agentActive = Date.now() - lastAgentPollAt < 30000; // polled in last 30s
  if (agentActive) {
    res.json({ status: 'ok', agentActive: true });
  } else {
    res.status(503).json({ status: 'offline', agentActive: false, error: 'Print Agent not polling' });
  }
};

// Submit a print job to the queue (called by mobile frontend)
export const printProxyLabel = async (req: Request, res: Response) => {
  try {
    const userId = (req as any).user?.id;
    const warehouseId = (req as any).user?.warehouse_id || req.body.warehouse_id;
    const { wsn, product_title, brand, mrp, fsp, fsn, wid, product_serial_number, copies } = req.body;
    if (!wsn) return res.status(400).json({ error: 'WSN required' });

    const payload = {
      wsn,
      product_title: product_title || '',
      brand: brand || '',
      mrp: String(mrp || ''),
      fsp: String(fsp || ''),
      fsn: fsn || '',
      wid: wid || '',
      product_serial_number: product_serial_number || '',
      copies: Math.max(1, Math.min(copies || 1, 10)),
    };

    const result = await query(
      `INSERT INTO print_queue (user_id, warehouse_id, wsn, payload, status)
       VALUES ($1, $2, $3, $4, 'pending')
       RETURNING id, created_at`,
      [userId, warehouseId, wsn, JSON.stringify(payload)]
    );

    res.json({ success: true, jobId: result.rows[0].id, status: 'queued' });
  } catch (err: any) {
    console.error('❌ Print queue submit error:', err);
    res.status(500).json({ error: 'Failed to queue print job' });
  }
};

// Print Agent polls for pending jobs (API key auth — no user JWT needed)
export const printQueuePending = async (req: Request, res: Response) => {
  try {
    lastAgentPollAt = Date.now();

    // Atomically grab pending jobs and DELETE them (zero storage — no data stays in DB)
    const result = await query(
      `DELETE FROM print_queue
       WHERE id IN (
         SELECT id FROM print_queue WHERE status = 'pending' ORDER BY created_at ASC LIMIT 10
       )
       RETURNING id, wsn, payload`,
      []
    );

    res.json({ jobs: result.rows });
  } catch (err: any) {
    console.error('❌ Print queue pending error:', err);
    res.status(500).json({ error: 'Failed to fetch pending jobs' });
  }
};

// Print Agent reports job completion (kept for error tracking, but job already deleted)
export const printQueueComplete = async (req: Request, res: Response) => {
  res.json({ success: true });
};

export const printProxyPrinters = async (_req: Request, res: Response) => {
  // Not available in queue mode — printers are local to the laptop
  res.json({ printers: [], note: 'Printer list only available from Print Agent directly' });
};