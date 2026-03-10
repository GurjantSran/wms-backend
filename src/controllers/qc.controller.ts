import { Request, Response } from 'express';
import { query, getPool } from '../config/database';
import { generateBatchId } from '../utils/helpers';
import * as XLSX from 'xlsx';
import ExcelJS from 'exceljs';
import fs from 'fs';
import { safeError } from '../utils/sanitizeError';
import { logChanges, logChangeSimple, logChangeSimpleBatch } from '../utils/changeLogger';
import { backupScheduler } from '../services/backupScheduler';

// ✅ GET ALL QC'D WSNs (for duplicate checking)
export const getAllQCWSNs = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;
    const params: any[] = [];
    let whereSql = `wsn IS NOT NULL AND wsn != ''`;
    if (warehouseId) {
      params.push(warehouseId);
      whereSql += ` AND warehouse_id = $${params.length}`;
    }
    const result = await query(
      `SELECT DISTINCT UPPER(TRIM(wsn)) as wsn, warehouse_id as warehouseid 
       FROM qc 
       WHERE ${whereSql}
       ORDER BY wsn LIMIT 10000`,
      params
    );

    console.log('✅ QC WSNs fetched:', result.rows.length); // Debug log
    res.json(result.rows);
  } catch (error: any) {
    console.error('❌ Error fetching all QC WSNs:', error);
    res.status(500).json({ error: 'Failed to fetch QC WSNs' });
  }
};


// ✅ GET PENDING INBOUND ITEMS
export const getPendingInboundForQC = async (req: Request, res: Response) => {
  try {
    const { warehouseId, search: rawSearch } = req.query;
    const search = typeof rawSearch === 'string' ? rawSearch.trim().toUpperCase() : rawSearch;
    let sql = `
      SELECT 
        i.id as inbound_id, 
        i.wsn, 
        i.inbound_date, 
        i.vehicle_no,
        i.rack_no, 
        i.product_serial_number, 
        
        -- ✅ MASTER DATA - ALL COLUMNS NEEDED FOR QC MULTI ENTRY
        m.product_title, 
        m.brand,
        m.cms_vertical, 
        m.mrp, 
        m.fsp, 
        m.fkt_link, 
        m.wid, 
        m.fsn,
        m.order_id,
        m.hsn_sac,          
        m.igst_rate,        
        m.invoice_date,
        m.p_type,
        m.p_size,
        m.vrp,
        m.yield_value,
        m.fkqc_remark,
        m.fk_grade,
        m.fkqc_remark,
        m.wh_location
      FROM inbound i
      LEFT JOIN master_data m ON i.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE NOT EXISTS (SELECT 1 FROM qc WHERE qc.wsn = i.wsn)
    `;

    const params: any[] = [];
    let paramIndex = 1;

    if (warehouseId) {
      sql += ` AND i.warehouse_id = $${paramIndex}`;
      params.push(warehouseId);
      paramIndex++;
    }

    if (search) {
      sql += ` AND i.wsn = $${paramIndex}`;
      params.push(search);
      paramIndex++;
    }

    sql += ` ORDER BY i.created_at DESC LIMIT 1000`;
    const result = await query(sql, params);
    res.json(result.rows);
  } catch (error: any) {
    console.error('❌ Pending inbound error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ✅ GET QC LIST - OPTIMIZED for 1M+ rows
export const getQCList = async (req: Request, res: Response) => {
  try {
    const {
      page = 1,
      limit = 100,
      search = '',
      warehouseId,
      qcStatus,
      qcGrade,
      dateFrom,
      dateTo,
      brand,
      category,
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

    // Determine if we need master_data join for filtering
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
        whereConditions.push(`q.warehouse_id = $${paramIndex}`);
        countWhereConditions.push(`q.warehouse_id = $${countParamIndex}`);
        params.push(warehouseId);
        countParams.push(warehouseId);
        paramIndex++;
        countParamIndex++;
      } else {
        // No specific warehouse requested, filter to user's accessible warehouses
        whereConditions.push(`q.warehouse_id = ANY($${paramIndex}::int[])`);
        countWhereConditions.push(`q.warehouse_id = ANY($${countParamIndex}::int[])`);
        params.push(accessibleWarehouses);
        countParams.push(accessibleWarehouses);
        paramIndex++;
        countParamIndex++;
      }
    } else if (warehouseId) {
      // No restrictions (super_admin/admin), but specific warehouse requested
      whereConditions.push(`q.warehouse_id = $${paramIndex}`);
      countWhereConditions.push(`q.warehouse_id = $${countParamIndex}`);
      params.push(warehouseId);
      countParams.push(warehouseId);
      paramIndex++;
      countParamIndex++;
    }

    if (search && search !== '') {
      whereConditions.push(`(
        q.wsn ILIKE $${paramIndex} OR
        m.product_title ILIKE $${paramIndex} OR
        m.brand ILIKE $${paramIndex}
      )`);
      countWhereConditions.push(`(
        q.wsn ILIKE $${countParamIndex} OR
        m.product_title ILIKE $${countParamIndex} OR
        m.brand ILIKE $${countParamIndex}
      )`);
      params.push(`%${search}%`);
      countParams.push(`%${search}%`);
      paramIndex++;
      countParamIndex++;
    }

    if (qcStatus && qcStatus !== '') {
      whereConditions.push(`q.qc_status = $${paramIndex}`);
      countWhereConditions.push(`q.qc_status = $${countParamIndex}`);
      params.push(qcStatus);
      countParams.push(qcStatus);
      paramIndex++;
      countParamIndex++;
    }

    if (qcGrade && qcGrade !== '') {
      whereConditions.push(`q.qc_grade = $${paramIndex}`);
      countWhereConditions.push(`q.qc_grade = $${countParamIndex}`);
      params.push(qcGrade);
      countParams.push(qcGrade);
      paramIndex++;
      countParamIndex++;
    }

    if (dateFrom) {
      whereConditions.push(`q.qc_date >= $${paramIndex}`);
      countWhereConditions.push(`q.qc_date >= $${countParamIndex}`);
      params.push(dateFrom);
      countParams.push(dateFrom);
      paramIndex++;
      countParamIndex++;
    }

    if (dateTo) {
      whereConditions.push(`q.qc_date <= $${paramIndex}`);
      countWhereConditions.push(`q.qc_date <= $${countParamIndex}`);
      params.push(dateTo);
      countParams.push(dateTo);
      paramIndex++;
      countParamIndex++;
    }

    if (brand && brand !== '') {
      whereConditions.push(`m.brand = $${paramIndex}`);
      countWhereConditions.push(`m.brand = $${countParamIndex}`);
      params.push(brand);
      countParams.push(brand);
      paramIndex++;
      countParamIndex++;
    }

    if (category && category !== '') {
      whereConditions.push(`m.cms_vertical = $${paramIndex}`);
      countWhereConditions.push(`m.cms_vertical = $${countParamIndex}`);
      params.push(category);
      countParams.push(category);
      paramIndex++;
      countParamIndex++;
    }

    const whereClause = whereConditions.length > 0 ? 'WHERE ' + whereConditions.join(' AND ') : '';
    const countWhereClause = countWhereConditions.length > 0 ? 'WHERE ' + countWhereConditions.join(' AND ') : '';

    // OPTIMIZED: Run count and ID queries in PARALLEL
    const countSql = needsMasterJoin
      ? `SELECT COUNT(*) FROM qc q LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL ${countWhereClause}`
      : `SELECT COUNT(*) FROM qc q ${countWhereClause}`;

    const idsSql = `
      SELECT q.id
      FROM qc q
      ${needsMasterJoin ? 'LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL' : ''}
      ${whereClause}
      ORDER BY q.created_at DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
    params.push(Number(limit), offset);

    // Run both queries in parallel
    const [countResult, idsResult] = await Promise.all([
      query(countSql, countParams),
      query(idsSql, params)
    ]);

    const total = parseInt(countResult.rows[0]?.count || '0');
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

    // PHASE 2: Fetch full data for the IDs
    const dataSql = `
      SELECT
        q.id, q.wsn, q.qc_date, q.qc_by, q.qc_by_name, q.qc_grade, q.qc_status,
        q.qc_remarks, q.other_remarks, q.product_serial_number, q.rack_no,
        q.batch_id, q.created_at, q.updated_at, q.updated_by_name,
        i.inbound_date, i.vehicle_no, i.rack_no AS inbound_rack_no,
        m.wid, m.fsn, m.order_id, m.fkqc_remark, m.fk_grade, m.product_title,
        m.hsn_sac, m.igst_rate, m.fsp, m.mrp, m.invoice_date, m.fkt_link,
        m.wh_location, m.brand, m.cms_vertical, m.vrp, m.yield_value,
        m.p_type, m.p_size, m.upload_date, m.batch_id AS master_batch_id,
        m.created_user_name
      FROM qc q
      LEFT JOIN inbound i ON q.wsn = i.wsn AND q.warehouse_id = i.warehouse_id
      LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE q.id = ANY($1)
      ORDER BY q.created_at DESC
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
    console.error('❌ Get QC list error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ✅ CREATE QC ENTRY
export const createQCEntry = async (req: Request, res: Response) => {
  const pool = getPool();
  const client = await pool.connect();

  try {
    const {
      wsn: rawWsn,
      qc_date,
      qc_grade,
      qc_remarks,
      other_remarks,
      product_serial_number,
      rack_no,
      warehouse_id,
      update_existing,
      qc_by_name,  // ← ADD THIS LINE
    } = req.body;

    // Normalize WSN to uppercase
    const wsn = typeof rawWsn === 'string' ? rawWsn.trim().toUpperCase() : rawWsn;

    const userId = (req as any).user?.userId;
    let currentUserName = (req as any).user?.full_name;

    // If full_name not in JWT, fetch from database
    if (!currentUserName) {
      try {
        const userResult = await client.query('SELECT full_name FROM users WHERE id = $1', [userId]);
        currentUserName = userResult.rows[0]?.full_name || 'Unknown';
      } catch (err) {
        console.error('Error fetching user name:', err);
        currentUserName = 'Unknown';
      }
    }
    // ✅ Prefer frontend-provided `qc_by_name` when it's a non-empty string, else use logged-in user
    const reqQcBy = typeof qc_by_name === 'string' ? qc_by_name.trim() : '';
    const qcByName = reqQcBy !== '' ? reqQcBy : currentUserName;

    // Start transaction for atomicity
    await client.query('BEGIN');

    // Check if QC already exists (with row lock to prevent race conditions)
    const checkSql = `SELECT id, warehouse_id FROM qc WHERE UPPER(wsn) = $1 FOR UPDATE`;
    const checkResult = await client.query(checkSql, [wsn]);

    if (checkResult.rows.length > 0) {
      const existing = checkResult.rows[0];
      if (existing.warehouse_id === warehouse_id) {
        // Same warehouse, allow update if update_existing
        if (!update_existing) {
          await client.query('ROLLBACK');
          return res.status(409).json({ error: 'QC already exists for this WSN in this warehouse', canUpdate: true });
        }
      } else {
        // Different warehouse, no update
        await client.query('ROLLBACK');
        return res.status(409).json({ error: 'WSN already exists in another warehouse', canUpdate: false });
      }

      // Update existing
      const updateSql = `
        UPDATE qc
        SET qc_date = $1,
            qc_grade = $2,
            qc_remarks = $3,
            other_remarks = $4,
            product_serial_number = $5,
            rack_no = $6,
            qc_by = $7,
            qc_by_name = $8,
            updated_by = $9,
            updated_by_name = $10,
            updated_at = NOW()
        WHERE id = $11
        RETURNING *
      `;

      const updateResult = await client.query(updateSql, [
        qc_date,
        qc_grade,
        qc_remarks,
        other_remarks,
        product_serial_number,
        rack_no,
        userId,
        qcByName,
        userId,
        currentUserName,
        checkResult.rows[0].id,
      ]);

      await client.query('COMMIT');
      return res.json({ ...updateResult.rows[0], action: 'updated' });
    }

    // Get inbound data
    const inboundSql = `SELECT id, warehouse_id FROM inbound WHERE UPPER(wsn) = $1 LIMIT 1`;
    const inboundResult = await client.query(inboundSql, [wsn]);

    if (inboundResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'WSN not found in inbound' });
    }

    // Insert QC entry
    const insertSql = `
   INSERT INTO qc (
    wsn,
    inbound_id,
    qc_date,
    qc_by,
    qc_by_name,
    qc_grade,
    qc_remarks,
    other_remarks,
    product_serial_number,
    rack_no,
    updated_by,
    updated_by_name,
    warehouse_id,
    qc_status
   ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'Done')
   RETURNING *
   `;


    const result = await client.query(insertSql, [
      wsn,
      inboundResult.rows[0].id,
      qc_date,
      userId,
      qcByName,
      qc_grade || null,
      qc_remarks || null,
      other_remarks || null,
      product_serial_number || null,
      rack_no || null,
      userId,
      currentUserName,
      warehouse_id,
    ]);

    await client.query('COMMIT');
    res.status(201).json({ ...result.rows[0], action: 'created' });
  } catch (error: any) {
    await client.query('ROLLBACK').catch(() => { });
    console.error('❌ Create QC error:', error);
    res.status(500).json({ error: safeError(error) });
  } finally {
    client.release();
  }
};

// ✅ BULK UPLOAD
export const bulkQCUpload = async (req: Request, res: Response) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const { warehouse_id, qc_date, qc_by_name } = req.body;
    const userId = (req as any).user?.userId;
    let userName = (req as any).user?.full_name;

    // If full_name not in JWT, fetch from database
    if (!userName) {
      try {
        const userResult = await query('SELECT full_name FROM users WHERE id = $1', [userId]);
        userName = userResult.rows[0]?.full_name || 'Unknown';
      } catch (err) {
        console.error('Error fetching user name:', err);
        userName = 'Unknown';
      }
    }
    const filePath = req.file.path;

    // Use shared parser utility for safer parsing
    const buffer = await fs.promises.readFile(filePath);
    const { parseExcelBuffer } = require('../utils/excelParser');
    const data: any[] = await parseExcelBuffer(Buffer.from(buffer));

    if (data.length === 0) {
      fs.unlinkSync(filePath);
      return res.status(400).json({ error: 'Excel file is empty' });
    }

    const batchId = generateBatchId('QC_BULK');

    res.status(202).json({
      message: 'Upload started',
      batchId,
      totalRows: data.length,
      timestamp: new Date().toISOString(),
    });

    // Process in background
    processBulkQC(data, batchId, warehouse_id, userId, userName, filePath, qc_date, qc_by_name);
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

async function processBulkQC(
  data: any[],
  batchId: string,
  warehouseId: string,
  userId: number,
  userName: string,
  filePath: string,
  bulkQcDate?: string,
  bulkQcByName?: string
) {
  const CHUNK_SIZE = 500;
  let successCount = 0;

  const client = await getPool().connect();
  try {
    await client.query('BEGIN');
    // Collect WSNs and check existing
    const wsns = data.map((row: any) => String(row['WSN'] || row['wsn'] || '').trim()).filter(Boolean);
    const existingMap = new Map();

    if (wsns.length > 0) {
      const existingSql = `SELECT wsn FROM qc WHERE wsn = ANY($1)`;
      const existingResult = await client.query(existingSql, [wsns]);
      existingResult.rows.forEach((row: any) => {
        existingMap.set(row.wsn, true);
      });
    }

    const validRows: any[] = [];

    // BATCH: Pre-fetch all inbound IDs in one query instead of N+1 SELECT per row
    const candidateWSNs = data
      .map((row: any) => String(row['WSN'] || row['wsn'] || '').trim())
      .filter((wsn: string) => wsn && !existingMap.has(wsn));

    const inboundMap = new Map<string, number>();
    if (candidateWSNs.length > 0) {
      const inboundResult = await client.query(
        `SELECT id, wsn FROM inbound WHERE wsn = ANY($1)`,
        [candidateWSNs]
      );
      inboundResult.rows.forEach((row: any) => {
        inboundMap.set(row.wsn, row.id);
      });
    }

    for (const row of data) {
      const wsn = String(row['WSN'] || row['wsn'] || '').trim();

      if (!wsn || existingMap.has(wsn)) continue;

      const inboundId = inboundMap.get(wsn);
      if (!inboundId) continue;

      validRows.push({
        wsn,
        qc_date: bulkQcDate || row['QC_DATE'] || row['QCDATE'] || new Date().toISOString().split('T')[0],
        qc_grade: row['GRADE'] || row['QCGRADE'] || null,
        qc_remarks: row['QC_REMARKS'] || row['QCREMARKS'] || null,
        other_remarks: row['OTHER_REMARKS'] || row['OTHERREMARKS'] || null,
        product_serial_number: row['PRODUCT_SERIAL_NUMBER'] || row['PRODUCTSERIALNUMBER'] || null,
        rack_no: row['RACK_NO'] || row['RACKNO'] || null,
        inbound_id: inboundId,
      });
    }

    // Insert in chunks
    for (let i = 0; i < validRows.length; i += CHUNK_SIZE) {
      const chunk = validRows.slice(i, i + CHUNK_SIZE);
      const valuesClauses: string[] = [];
      const params: any[] = [];
      let paramIndex = 1;

      for (const row of chunk) {
        // Use bulk qc_by_name from dropdown if provided, otherwise logged-in userName
        const effectiveQcByName = (bulkQcByName && bulkQcByName.trim() !== '') ? bulkQcByName.trim() : userName;

        const rowParams = [
          row.wsn,
          row.inbound_id,
          row.qc_date,
          userId,
          effectiveQcByName,  // qc_by_name from dropdown or logged-in user
          row.qc_grade,
          row.qc_remarks,
          row.other_remarks,
          row.product_serial_number,
          row.rack_no,
          userId,
          userName,  // updated_by_name (current user)
          warehouseId,
          batchId,
          'Done',
        ];



        const placeholders = rowParams.map(() => `$${paramIndex++}`).join(', ');
        valuesClauses.push(`(${placeholders})`);
        params.push(...rowParams);
      }

      const insertSql = `
  INSERT INTO qc (
    wsn, inbound_id, qc_date, qc_by, qc_by_name, qc_grade,
    qc_remarks, other_remarks, product_serial_number, rack_no,
    updated_by, updated_by_name, warehouse_id, batch_id, qc_status
  ) VALUES ${valuesClauses.join(', ')}
  `;
      const result = await client.query(insertSql, params);
      successCount += result.rowCount || 0;
    }

    await client.query('COMMIT');
    console.log(`✅ Batch ${batchId}: ${successCount} records inserted`);
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

// ✅ MULTI ENTRY
export const multiQCEntry = async (req: Request, res: Response) => {
  const pool = getPool();
  const client = await pool.connect();

  try {
    const { entries, warehouse_id } = req.body;
    const userId = (req as any).user?.userId;
    let userDefaultName = (req as any).user?.full_name;

    // If full_name not in JWT, fetch from database
    if (!userDefaultName) {
      try {
        const userResult = await client.query('SELECT full_name FROM users WHERE id = $1', [userId]);
        userDefaultName = userResult.rows[0]?.full_name || 'Unknown';
      } catch (err) {
        console.error('Error fetching user name:', err);
        userDefaultName = 'Unknown';
      }
    }

    // ✅ USE ENTRY'S QC_BY_NAME if provided (trim non-empty), otherwise user default
    const getQCByName = (entry: any) => {
      const raw = entry.qc_by_name || entry.qcByName || '';
      return (typeof raw === 'string' && raw.trim() !== '') ? raw.trim() : userDefaultName;
    };


    if (!entries || entries.length === 0) {
      client.release();
      return res.status(400).json({ error: 'No entries provided' });
    }

    const batchId = generateBatchId('QC_MULTI');
    const results: any[] = [];

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

    // ⚡ BULK CHECK: Get all existing QC records in ONE query
    const existingQCMap = new Map<string, boolean>();
    const qcCheckResult = await client.query(
      `SELECT UPPER(wsn) as wsn FROM qc WHERE UPPER(wsn) = ANY($1) AND warehouse_id = $2`,
      [allWSNs, warehouse_id]
    );
    qcCheckResult.rows.forEach((row: any) => existingQCMap.set(row.wsn, true));

    // ⚡ BULK CHECK: Get all inbound IDs in ONE query
    const inboundMap = new Map<string, number>();
    const inboundResult = await client.query(
      `SELECT UPPER(wsn) as wsn, id FROM inbound WHERE UPPER(wsn) = ANY($1) AND warehouse_id = $2`,
      [allWSNs, warehouse_id]
    );
    inboundResult.rows.forEach((row: any) => inboundMap.set(row.wsn, row.id));

    // ⚡ VALIDATE: Filter valid entries for bulk insert
    const validEntries: any[] = [];
    const processedWSNs = new Set<string>();

    for (const entry of entries) {
      const wsn = entry.wsn?.trim()?.toUpperCase();

      if (!wsn) {
        results.push({ wsn: 'EMPTY', status: 'ERROR', message: 'WSN required' });
        continue;
      }

      // Check batch duplicate
      if (processedWSNs.has(wsn)) {
        results.push({ wsn, status: 'DUPLICATE', message: 'Duplicate in batch' });
        continue;
      }
      processedWSNs.add(wsn);

      // Check existing QC
      if (existingQCMap.has(wsn)) {
        results.push({ wsn, status: 'DUPLICATE', message: 'QC already exists in this warehouse' });
        continue;
      }

      // Check inbound exists
      const inboundId = inboundMap.get(wsn);
      if (!inboundId) {
        results.push({ wsn, status: 'ERROR', message: 'WSN not found in inbound for this warehouse' });
        continue;
      }

      const qcDate = typeof entry.qc_date === 'string'
        ? entry.qc_date
        : new Date().toISOString().split('T')[0];

      validEntries.push({
        wsn,
        inbound_id: inboundId,
        qc_date: qcDate,
        qc_by: userId,
        qc_by_name: getQCByName(entry),
        qc_grade: entry.qc_grade || null,
        qc_remarks: entry.qc_remarks || null,
        other_remarks: entry.other_remarks || null,
        product_serial_number: entry.product_serial_number || null,
        rack_no: entry.rack_no || null,
        updated_by: userId,
        updated_by_name: userDefaultName,
        warehouse_id,
        batch_id: batchId,
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
          const offset = idx * 15;
          valuePlaceholders.push(
            `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12}, $${offset + 13}, $${offset + 14}, 'Done')`
          );
          values.push(
            entry.wsn,
            entry.inbound_id,
            entry.qc_date,
            entry.qc_by,
            entry.qc_by_name,
            entry.qc_grade,
            entry.qc_remarks,
            entry.other_remarks,
            entry.product_serial_number,
            entry.rack_no,
            entry.updated_by,
            entry.updated_by_name,
            entry.warehouse_id,
            entry.batch_id
          );
        });

        const bulkSql = `
          INSERT INTO qc (wsn, inbound_id, qc_date, qc_by, qc_by_name, qc_grade,
           qc_remarks, other_remarks, product_serial_number, rack_no,
           updated_by, updated_by_name, warehouse_id, batch_id, qc_status)
          VALUES ${valuePlaceholders.join(', ')}
        `;

        try {
          await client.query(`SAVEPOINT qc_batch_${i}`);
          await client.query(bulkSql, values);

          batch.forEach((entry) => {
            results.push({ wsn: entry.wsn, status: 'SUCCESS', message: 'Created' });
            successCount++;
          });
        } catch (err: any) {
          // Fallback to individual inserts for this batch
          await client.query(`ROLLBACK TO SAVEPOINT qc_batch_${i}`);
          console.log(`QC bulk insert failed for batch ${i}, falling back to individual`);

          for (const entry of batch) {
            try {
              await client.query(`SAVEPOINT qc_entry_${entry.wsn.replace(/[^a-zA-Z0-9]/g, '_')}`);
              await client.query(
                `INSERT INTO qc (wsn, inbound_id, qc_date, qc_by, qc_by_name, qc_grade,
                 qc_remarks, other_remarks, product_serial_number, rack_no,
                 updated_by, updated_by_name, warehouse_id, batch_id, qc_status)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'Done')`,
                [
                  entry.wsn, entry.inbound_id, entry.qc_date, entry.qc_by, entry.qc_by_name,
                  entry.qc_grade, entry.qc_remarks, entry.other_remarks, entry.product_serial_number,
                  entry.rack_no, entry.updated_by, entry.updated_by_name, entry.warehouse_id, entry.batch_id
                ]
              );
              results.push({ wsn: entry.wsn, status: 'SUCCESS', message: 'Created' });
              successCount++;
            } catch (individualErr: any) {
              await client.query(`ROLLBACK TO SAVEPOINT qc_entry_${entry.wsn.replace(/[^a-zA-Z0-9]/g, '_')}`);
              const msg = individualErr.code === '23505' ? 'QC already exists (concurrent entry)' : individualErr.message;
              results.push({ wsn: entry.wsn, status: individualErr.code === '23505' ? 'DUPLICATE' : 'ERROR', message: msg });
            }
          }
        }
      }
    }

    // Log all successful inserts for CCTV-style tracking
    const successfulQC = results.filter((r: any) => r.status === 'SUCCESS');
    if (successfulQC.length > 0) {
      await logChanges(client, 'qc', 'INSERT',
        successfulQC.map((r: any) => ({ wsn: r.wsn, newData: { wsn: r.wsn, batch_id: batchId, warehouse_id } })),
        { batchId, userId: (req as any).user?.id, userName: userDefaultName, warehouseId: warehouse_id }
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
        sseManager.broadcast(warehouse_id, 'qc', 'data-submitted', {
          successCount,
          totalCount: entries.length,
          batchId,
          submittedWSNs,
          submittedBy: userDefaultName,
        }, deviceId);
      } catch { /* SSE broadcast is best-effort */ }
    }

    // Trigger event backup (fire-and-forget)
    backupScheduler.triggerEventBackup(`qc multi-entry: ${successCount} entries`).catch(() => { });

    res.json({
      batchId,
      totalCount: entries.length,
      successCount,
      results,
    });
  } catch (error: any) {
    // Rollback entire transaction on unexpected error
    await client.query('ROLLBACK');
    console.error('❌ Multi entry error:', error);
    res.status(500).json({ error: safeError(error) });
  } finally {
    client.release();
  }
};

// ✅ GET STATS
export const getQCStats = async (req: Request, res: Response) => {
  try {
    const { warehouseId, dateFrom, dateTo } = req.query;

    let sql = `
      SELECT
        qc_status,
        COUNT(*) as count
      FROM qc
      WHERE 1=1
    `;

    const params: any[] = [];
    let paramIndex = 1;

    if (warehouseId) {
      sql += ` AND warehouse_id = $${paramIndex}`;
      params.push(warehouseId);
      paramIndex++;
    }

    if (dateFrom) {
      sql += ` AND DATE(qc_date) >= $${paramIndex}`;
      params.push(dateFrom);
      paramIndex++;
    }

    if (dateTo) {
      sql += ` AND DATE(qc_date) <= $${paramIndex}`;
      params.push(dateTo);
      paramIndex++;
    }

    sql += ` GROUP BY qc_status`;

    const result = await query(sql, params);

    const stats = {
      pending: 0,
      pass: 0,
      fail: 0,
      hold: 0,
      done: 0,
      total: 0,
    };

    result.rows.forEach((row: any) => {
      const status = (row.qc_status || 'pending')?.toLowerCase();
      if (status === 'pass') stats.pass += parseInt(row.count);
      else if (status === 'fail') stats.fail += parseInt(row.count);
      else if (status === 'hold') stats.hold += parseInt(row.count);
      else if (status === 'done') stats.done += parseInt(row.count);
      else stats.pending += parseInt(row.count);

      stats.total += parseInt(row.count);
    });

    res.json(stats);
  } catch (error: any) {
    console.error('❌ QC stats error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ✅ GET BATCHES
export const getQCBatches = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;
    // Get accessible warehouses from middleware (user's allowed warehouses)
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;

    let sql = `
      SELECT
        q.batch_id,
        COUNT(*) as count,
        MAX(q.created_at) as created_at,
        MIN(q.qc_status) as status,
        STRING_AGG(DISTINCT w.name, ', ') as warehouse_names,
        MAX(q.qc_by_name) as uploaded_by
      FROM qc q
      LEFT JOIN warehouses w ON q.warehouse_id = w.id
      WHERE q.batch_id IS NOT NULL
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
        sql += ` AND q.warehouse_id = $${paramIndex}`;
        params.push(requestedId);
        paramIndex++;
      } else {
        sql += ` AND q.warehouse_id = ANY($${paramIndex}::int[])`;
        params.push(accessibleWarehouses);
        paramIndex++;
      }
    } else if (warehouseId) {
      sql += ` AND q.warehouse_id = $${paramIndex}`;
      params.push(warehouseId);
      paramIndex++;
    }
    // If no accessibleWarehouses and no warehouseId, show all (super_admin/admin)

    sql += ` GROUP BY q.batch_id ORDER BY MAX(q.created_at) DESC LIMIT 500`;

    const result = await query(sql, params);

    // Normalize for frontend
    const rows = result.rows.map((r: any) => ({
      ...r,
      last_updated: r.created_at ? new Date(r.created_at).toISOString() : null,
      warehouse_names: r.warehouse_names || null,
      uploaded_by: r.uploaded_by || null
    }));

    res.json(rows);
  } catch (error: any) {
    console.error('❌ Get batches error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ✅ DELETE BATCH
export const deleteQCBatch = async (req: Request, res: Response) => {
  try {
    const { batchId } = req.params;
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;

    // First check if the batch belongs to accessible warehouses
    if (accessibleWarehouses && accessibleWarehouses.length > 0) {
      const checkResult = await query(
        'SELECT DISTINCT warehouse_id FROM qc WHERE batch_id = $1',
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
    const beforeDelete = await query('SELECT id, wsn, warehouse_id FROM qc WHERE batch_id = $1', [batchId]);

    const result = await query('DELETE FROM qc WHERE batch_id = $1', [batchId]);

    // Log deletions asynchronously
    if (beforeDelete.rows.length > 0) {
      const userId = (req as any).user?.id;
      const userName = (req as any).user?.full_name || (req as any).user?.username || 'Unknown';
      const warehouseId = beforeDelete.rows[0]?.warehouse_id;
      Promise.resolve().then(async () => {
        await logChangeSimpleBatch('qc', 'DELETE',
          beforeDelete.rows.map(row => ({ id: row.id, wsn: row.wsn, oldData: row })),
          { batchId, userId, userName, warehouseId }
        );
      }).catch(() => { });
    }

    res.json({
      message: 'Batch deleted',
      count: result.rowCount,
    });
  } catch (error: any) {
    console.error('❌ Delete batch error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ✅ GET BRANDS
export const getQCBrands = async (req: Request, res: Response) => {
  try {
    const { warehouse_id } = req.query;

    let sql = `
      SELECT DISTINCT m.brand
      FROM qc q
      LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE m.brand IS NOT NULL AND m.brand != ''
    `;

    const params: any[] = [];

    if (warehouse_id) {
      sql += ` AND q.warehouse_id = $1`;
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

// ✅ GET CATEGORIES
export const getQCCategories = async (req: Request, res: Response) => {
  try {
    const { warehouse_id } = req.query;

    let sql = `
      SELECT DISTINCT m.cms_vertical
      FROM qc q
      LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL
      WHERE m.cms_vertical IS NOT NULL AND m.cms_vertical != ''
    `;

    const params: any[] = [];

    if (warehouse_id) {
      sql += ` AND q.warehouse_id = $1`;
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

// ✅ EXPORT DATA
export const exportQCData = async (req: Request, res: Response) => {
  try {
    const { warehouseId, dateFrom, dateTo, qcStatus, brand, category, qcGrade } = req.query;

    let sql = `
      SELECT
  -- QC
  q.*,

  -- INBOUND
  i.inbound_date,
  i.vehicle_no,

  -- MASTER DATA (FULL)
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
  m.upload_date,
  m.batch_id AS master_batch_id,
  m.created_user_name
FROM qc q
LEFT JOIN inbound i ON q.wsn = i.wsn
LEFT JOIN master_data m ON q.wsn = m.wsn AND m.deleted_at IS NULL
WHERE 1=1

    `;

    const params: any[] = [];
    let paramIndex = 1;

    if (warehouseId) {
      sql += ` AND q.warehouse_id = $${paramIndex}`;
      params.push(warehouseId);
      paramIndex++;
    }

    if (dateFrom) {
      sql += ` AND DATE(q.qc_date) >= $${paramIndex}`;
      params.push(dateFrom);
      paramIndex++;
    }

    if (dateTo) {
      sql += ` AND DATE(q.qc_date) <= $${paramIndex}`;
      params.push(dateTo);
      paramIndex++;
    }

    if (qcStatus) {
      sql += ` AND q.qc_status = $${paramIndex}`;
      params.push(qcStatus);
      paramIndex++;
    }

    if (qcGrade && qcGrade !== '') {
      sql += ` AND q.qc_grade = $${paramIndex}`;
      params.push(qcGrade);
      paramIndex++;
    }

    if (brand) {
      sql += ` AND m.brand = $${paramIndex}`;
      params.push(brand);
      paramIndex++;
    }

    if (category) {
      sql += ` AND m.cms_vertical = $${paramIndex}`;
      params.push(category);
      paramIndex++;
    }

    sql += ` ORDER BY q.qc_date DESC LIMIT 10000`;

    const result = await query(sql, params);
    res.json({ data: result.rows });
  } catch (error: any) {
    console.error('❌ Export error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ✅ DELETE ENTRY
export const deleteQCEntry = async (req: Request, res: Response) => {
  try {
    const { qcId } = req.params;
    const result = await query('DELETE FROM qc WHERE id = $1', [qcId]);

    res.json({ message: 'QC entry deleted', count: result.rowCount });
  } catch (error: any) {
    console.error('❌ Delete error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ✅ GET TEMPLATE - with Excel dropdown validation for GRADE & RACKNO
export const getQCTemplate = async (req: Request, res: Response) => {
  try {
    const warehouseId = req.query.warehouse_id;

    // Fetch racks for this warehouse
    let rackNames: string[] = [];
    if (warehouseId) {
      try {
        const rackResult = await query(
          `SELECT DISTINCT rack_name FROM racks WHERE warehouse_id = $1 AND rack_name IS NOT NULL ORDER BY rack_name`,
          [warehouseId]
        );
        rackNames = rackResult.rows.map((r: any) => r.rack_name).filter(Boolean);
      } catch (e) {
        console.warn('Could not fetch racks for template:', e);
      }
    }

    // QC Grades
    const grades = ['A', 'B', 'C', 'D'];

    // Build template using ExcelJS for data validation support
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Template');

    // Define columns
    worksheet.columns = [
      { header: 'WSN', key: 'wsn', width: 20 },
      { header: 'GRADE', key: 'grade', width: 12 },
      { header: 'RACKNO', key: 'rackno', width: 15 },
      { header: 'QCREMARKS', key: 'qcremarks', width: 25 },
      { header: 'OTHERREMARKS', key: 'otherremarks', width: 25 },
      { header: 'PRODUCTSERIALNUMBER', key: 'productserialnumber', width: 25 },
    ];

    // Style header row
    const headerRow = worksheet.getRow(1);
    headerRow.font = { bold: true, color: { argb: 'FFFFFFFF' } };
    headerRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E40AF' } };
    headerRow.alignment = { horizontal: 'center' };

    // Add one sample row
    worksheet.addRow({
      wsn: 'ABC123A',
      grade: 'A',
      rackno: rackNames[0] || 'A-01',
      qcremarks: 'All checks passed',
      otherremarks: 'Package condition good',
      productserialnumber: 'SN12345',
    });

    // Add data validation for GRADE column (B2:B1000) - dropdown list
    const gradeFormula = `"${grades.join(',')}"`;
    for (let row = 2; row <= 1000; row++) {
      worksheet.getCell(`B${row}`).dataValidation = {
        type: 'list',
        allowBlank: false,
        formulae: [gradeFormula],
        showErrorMessage: true,
        errorTitle: 'Invalid Grade',
        error: `Grade must be one of: ${grades.join(', ')}`,
        showInputMessage: true,
        promptTitle: 'Select Grade',
        prompt: `Choose from: ${grades.join(', ')}`,
      };
    }

    // Add data validation for RACKNO column (C2:C1000) - dropdown list
    if (rackNames.length > 0) {
      const rackFormula = `"${rackNames.join(',')}"`;
      for (let row = 2; row <= 1000; row++) {
        worksheet.getCell(`C${row}`).dataValidation = {
          type: 'list',
          allowBlank: true,
          formulae: [rackFormula],
          showErrorMessage: true,
          errorTitle: 'Invalid Rack',
          error: `Rack must be one of the available racks for this warehouse`,
          showInputMessage: true,
          promptTitle: 'Select Rack',
          prompt: 'Choose from available racks',
        };
      }
    }

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="QC_Template.xlsx"');

    const buffer = await workbook.xlsx.writeBuffer();
    res.send(Buffer.from(buffer));
  } catch (error: any) {
    console.error('❌ Template error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// SAVE DRAFT - Save multi-entry draft to database
// ============================================
export const saveQCDraft = async (req: Request, res: Response) => {
  try {
    const { draft_data, warehouse_id, common_date, draft_source } = req.body;
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
      INSERT INTO qc_multi_entry_drafts (user_id, warehouse_id, draft_data, common_date, row_count, draft_source, saved_at, updated_at)
      VALUES ($1, $2, $3::jsonb, $4, $5, $6, NOW(), NOW())
      ON CONFLICT (user_id, warehouse_id, draft_source)
      DO UPDATE SET
        draft_data = $3::jsonb,
        common_date = $4,
        row_count = $5,
        saved_at = NOW(),
        updated_at = NOW()
      RETURNING id, saved_at, row_count
    `;

    const result = await query(sql, [
      userId,
      warehouse_id,
      JSON.stringify(draft_data),
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
      sseManager.broadcast(warehouse_id, 'qc', 'draft-updated', { userId, rowCount }, deviceId);
    } catch { /* best-effort */ }
  } catch (error: any) {
    console.error('❌ Save QC draft error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// LOAD DRAFT - Load multi-entry draft from database
// ============================================
export const loadQCDraft = async (req: Request, res: Response) => {
  try {
    const { warehouse_id } = req.query;
    const userId = (req as any).user?.userId || (req as any).user?.id;

    if (!userId) {
      return res.status(400).json({ error: 'User ID required' });
    }

    if (!warehouse_id) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    const source = req.query.draft_source === 'mobile' ? 'mobile' : 'desktop';

    const sql = `
      SELECT draft_data, common_date, row_count, saved_at, updated_at
      FROM qc_multi_entry_drafts
      WHERE user_id = $1 AND warehouse_id = $2 AND draft_source = $3
    `;

    const result = await query(sql, [userId, warehouse_id, source]);

    if (result.rows.length === 0) {
      return res.json({ exists: false, draft: null });
    }

    const draft = result.rows[0];
    res.json({
      exists: true,
      draft: {
        rows: draft.draft_data || [],
        common_date: draft.common_date || '',
        row_count: draft.row_count || 0,
        saved_at: draft.saved_at,
        updated_at: draft.updated_at,
      },
    });
  } catch (error: any) {
    console.error('❌ Load QC draft error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// CLEAR DRAFT - Delete multi-entry draft from database
// ============================================
export const clearQCDraft = async (req: Request, res: Response) => {
  try {
    const { warehouse_id } = req.query;
    const userId = (req as any).user?.userId || (req as any).user?.id;

    if (!userId) {
      return res.status(400).json({ error: 'User ID required' });
    }

    const source = req.query.draft_source === 'mobile' ? 'mobile' : 'desktop';

    let sql = `DELETE FROM qc_multi_entry_drafts WHERE user_id = $1 AND draft_source = $3`;
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
        sseManager.broadcast(Number(warehouse_id), 'qc', 'draft-cleared', { userId }, deviceId);
      }
    } catch { /* best-effort */ }
  } catch (error: any) {
    console.error('❌ Clear QC draft error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ============================================
// SYNC ROWS - Relay multi-entry row changes to same user's other devices via SSE
// No DB write — pure SSE relay for real-time cross-device sync
// ============================================
export const syncQCRows = async (req: Request, res: Response) => {
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
      console.log(`[SYNC-ROWS] qc: userId=${userId}(type=${typeof userId}) warehouseId=${warehouseId} deviceId=${deviceId} rows=${rows.length}`);
      sseManager.broadcastToUser(Number(warehouseId), 'qc', userId, 'entry-synced', {
        rows,
        userId,
      }, deviceId);
    } catch (err: any) { console.error('[SYNC-ROWS] qc broadcast error:', err?.message); }

    res.json({ success: true });
  } catch (error: any) {
    console.error('❌ Sync QC rows error:', error);
    res.status(500).json({ error: 'Failed to sync rows' });
  }
};