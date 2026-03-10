// File Path = warehouse-backend/src/controllers/master-data.controller.ts
import { Request, Response } from 'express';
import { query, withTransaction } from '../config/database';
import { generateBatchId } from '../utils/helpers';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import csv from 'csv-parser';
import { logChangeSimple } from '../utils/changeLogger';
import { createReadStream } from 'fs';
import ExcelJS from 'exceljs';
import { uploadToSupabase, isSupabaseStorageConfigured, ensureBucketExists, STORAGE_BUCKETS } from '../services/supabaseStorage';
import { safeError } from '../utils/sanitizeError';

// ✅ REQUIRED COLUMNS - EXACT order and EXACT names (Issue #4 fix)
const REQUIRED_COLUMNS = [
  'WSN', 'WID', 'FSN', 'Order_ID', 'FKQC_Remark', 'FK_Grade', 'Product_Title',
  'HSN/SAC', 'IGST_Rate', 'FSP', 'MRP', 'Invoice_Date', 'Fkt_Link',
  'Wh_Location', 'BRAND', 'cms_vertical', 'VRP', 'Yield_Value', 'P_Type', 'P_Size'
];

// ✅ MANDATORY DATA COLUMNS - These must have non-empty values for each row
// Rows missing any of these fields will be REJECTED during upload
const MANDATORY_DATA_COLUMNS = [
  'WSN', 'WID', 'FSN', 'Order_ID', 'Product_Title',
  'HSN/SAC', 'IGST_Rate', 'FSP', 'MRP', 'Fkt_Link',
  'Wh_Location', 'BRAND', 'cms_vertical', 'VRP'
];

// ✅ Helper: Extract plain text from any ExcelJS cell value type
// ExcelJS can return objects for rich text, hyperlinks, formulas, dates, etc.
// Using String() on these objects produces "[object Object]" — this function extracts the actual text.
function extractCellText(value: any): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'object') {
    // Rich text: { richText: [{ text: 'Hello' }, { font: {...}, text: ' World' }] }
    if ('richText' in value && Array.isArray(value.richText)) {
      return value.richText.map((part: any) => (part && part.text) || '').join('');
    }
    // Hyperlink: { text: 'link text', hyperlink: 'http://...' }
    if ('text' in value && typeof value.text === 'string') return value.text;
    // Formula: { formula: '=A1+B1', result: 5 } or { formula: '...', result: { richText: [...] } }
    if ('result' in value) return extractCellText(value.result);
    // Error value: { error: { message: '#VALUE!' } }
    if ('error' in value) return null;
  }
  // Fallback: only use String() if it won't produce [object Object]
  const str = String(value);
  return str === '[object Object]' ? null : str;
}

// ✅ Helper: Validate a row has all mandatory fields filled
// Returns array of missing field names (empty array = valid)
function validateRowMandatoryFields(row: Record<string, any>, isCSV: boolean): string[] {
  const missing: string[] = [];
  for (const col of MANDATORY_DATA_COLUMNS) {
    let value: any;
    if (isCSV) {
      // CSV uses column name keys directly
      value = row[col] || row[col.toLowerCase()];
    } else {
      // Excel uses index-based mapping
      value = row[col];
    }
    // For Excel values, extract text properly to avoid [object Object] passing validation
    const textValue = isCSV ? value : extractCellText(value);
    if (textValue === null || textValue === undefined || String(textValue).trim() === '') {
      missing.push(col);
    }
  }
  return missing;
}

// ========== PHASE 3: DUPLICATE STRATEGY TYPES ==========
type DuplicateStrategy = 'skip' | 'update' | 'replace';

interface InsertBatchResult {
  inserted: number;
  updated: number;
  skipped: number;
  duplicateWsns: string[];
}

// ========== DB-BASED PROGRESS TRACKING ==========

/**
 * Save/update upload progress in the database (upload_progress table)
 */
async function saveProgressDB(jobId: string, data: any): Promise<void> {
  try {
    await query(
      `INSERT INTO upload_progress (job_id, status, processed, total, success_count, error_count, duplicate_count, batch_id, error_message, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
       ON CONFLICT (job_id) DO UPDATE SET
         status = EXCLUDED.status,
         processed = EXCLUDED.processed,
         total = EXCLUDED.total,
         success_count = EXCLUDED.success_count,
         error_count = EXCLUDED.error_count,
         duplicate_count = EXCLUDED.duplicate_count,
         batch_id = EXCLUDED.batch_id,
         error_message = EXCLUDED.error_message,
         updated_at = NOW()`,
      [
        jobId,
        data.status || 'pending',
        data.processed || 0,
        data.total || 0,
        data.successCount || 0,
        data.errorCount || 0,
        data.duplicateCount || 0,
        data.batchId || null,
        data.error || null
      ]
    );
  } catch (err: any) {
    console.error('❌ Failed to save progress to DB:', err.message);
  }
}

/**
 * Get upload progress from the database
 */
async function getProgressDB(jobId: string): Promise<any | null> {
  try {
    const result = await query(
      `SELECT job_id, status, processed, total, success_count, error_count, duplicate_count, batch_id, error_message
       FROM upload_progress WHERE job_id = $1`,
      [jobId]
    );
    if (result.rows.length === 0) return null;
    const row = result.rows[0];
    return {
      status: row.status,
      processed: row.processed,
      total: row.total,
      successCount: row.success_count,
      errorCount: row.error_count,
      duplicateCount: row.duplicate_count,
      batchId: row.batch_id,
      error: row.error_message
    };
  } catch (err: any) {
    console.error('❌ Failed to get progress from DB:', err.message);
    return null;
  }
}

/**
 * Compute SHA-256 hash of a file
 */
function computeFileHash(filePath: string): string {
  const fileBuffer = fs.readFileSync(filePath);
  return crypto.createHash('sha256').update(fileBuffer).digest('hex');
}

/**
 * Create an upload_logs entry
 */
async function createUploadLog(data: {
  jobId: string; batchId: string; filename: string; fileSize: number;
  fileHash: string; fileType: string; storagePath?: string; uploadedBy?: number;
  duplicateStrategy?: DuplicateStrategy; warehouseId?: number;
}): Promise<void> {
  try {
    await query(
      `INSERT INTO upload_logs (job_id, batch_id, original_filename, file_size_bytes, file_hash_sha256, file_type, storage_path, status, uploaded_by, duplicate_strategy, warehouse_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'processing', $8, $9, $10)`,
      [data.jobId, data.batchId, data.filename, data.fileSize, data.fileHash, data.fileType, data.storagePath || null, data.uploadedBy || null, data.duplicateStrategy || 'skip', data.warehouseId || null]
    );
  } catch (err: any) {
    console.error('❌ Failed to create upload log:', err.message);
  }
}

/**
 * Update upload_logs on completion/failure
 */
async function finalizeUploadLog(jobId: string, data: {
  status: string; totalRows: number; successCount: number;
  duplicateCount?: number; skippedCount?: number; errorCount?: number;
  errorMessage?: string; duplicateWsns?: string[];
}): Promise<void> {
  try {
    // Limit duplicate_wsns JSONB to first 500 entries to avoid oversized rows
    const wsnsToStore = (data.duplicateWsns || []).slice(0, 500);
    await query(
      `UPDATE upload_logs SET
        status = $2, total_rows = $3, success_count = $4,
        duplicate_count = $5, skipped_count = $6, error_count = $7,
        error_message = $8, duplicate_wsns = $9::jsonb,
        completed_at = NOW()
       WHERE job_id = $1`,
      [jobId, data.status, data.totalRows, data.successCount,
        data.duplicateCount || 0, data.skippedCount || 0,
        data.errorCount || 0, data.errorMessage || null,
        JSON.stringify(wsnsToStore)]
    );
  } catch (err: any) {
    console.error('❌ Failed to finalize upload log:', err.message);
  }
}

// ✅ FIX #3: Helper to convert UTC to IST in database
const convertUTCtoIST = (utcDate: Date): Date => {
  const istDate = new Date(utcDate.getTime() + (5.5 * 60 * 60 * 1000));
  return istDate;
};

// ========== WAREHOUSE HELPER ==========
/**
 * Extract warehouse context from request.
 * Uses query param `warehouseId` or falls back to user's accessible warehouses.
 * Returns { warehouseId, accessibleWarehouses } for query building.
 */
function getWarehouseContext(req: Request): { warehouseId: number | null; accessibleWarehouses: number[] | null } {
  const warehouseId = req.query.warehouseId ? Number(req.query.warehouseId) : null;
  const accessible = (req as any).accessibleWarehouses as number[] | null;
  return { warehouseId, accessibleWarehouses: accessible };
}

/**
 * Build warehouse WHERE clause for master_data queries.
 * Returns { clause: string, params: any[], nextIndex: number }
 */
function buildWarehouseFilter(req: Request, startIndex: number = 1, alias: string = ''): { clause: string; params: any[]; nextIndex: number } {
  const prefix = alias ? `${alias}.` : '';
  const { warehouseId, accessibleWarehouses } = getWarehouseContext(req);

  if (warehouseId) {
    return { clause: `${prefix}warehouse_id = $${startIndex}`, params: [warehouseId], nextIndex: startIndex + 1 };
  }
  if (accessibleWarehouses && accessibleWarehouses.length > 0) {
    return { clause: `${prefix}warehouse_id = ANY($${startIndex}::int[])`, params: [accessibleWarehouses], nextIndex: startIndex + 1 };
  }
  // Admin/super_admin with no specific warehouse — show all
  return { clause: '', params: [], nextIndex: startIndex };
}

// ====== CACHE APIs for Frontend IndexedDB ======

/**
 * Get total count of master data records
 * Used by frontend to know how many records to sync
 */
export const getMasterDataCount = async (req: Request, res: Response) => {
  try {
    const wh = buildWarehouseFilter(req, 1);
    const whereClause = wh.clause
      ? `WHERE deleted_at IS NULL AND ${wh.clause}`
      : 'WHERE deleted_at IS NULL';
    const result = await query(`SELECT COUNT(*) FROM master_data ${whereClause}`, wh.params);
    res.json({ count: parseInt(result.rows[0].count) });
  } catch (error: any) {
    console.error('❌ Get master data count error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

/**
 * Get list of all batch IDs with counts and dates
 * For batch-specific caching feature
 */
export const getMasterDataBatchList = async (req: Request, res: Response) => {
  try {
    const wh = buildWarehouseFilter(req, 1);
    const whClause = wh.clause ? `AND ${wh.clause}` : '';
    const result = await query(`
      SELECT 
        batch_id,
        COUNT(*) as count,
        MIN(created_at) as created_at,
        MAX(created_at) as last_updated
      FROM master_data
      WHERE batch_id IS NOT NULL AND batch_id != '' AND deleted_at IS NULL ${whClause}
      GROUP BY batch_id
      ORDER BY MAX(created_at) DESC
      LIMIT 100
    `, wh.params);

    res.json({
      batches: result.rows.map((row: any) => ({
        batch_id: row.batch_id,
        count: parseInt(row.count),
        created_at: row.created_at,
        last_updated: row.last_updated
      }))
    });
  } catch (error: any) {
    console.error('❌ Get batch list error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

/**
 * Get all master data records for specific batch ID(s)
 * For batch-specific caching - downloads only selected batch data
 */
export const getMasterDataByBatchIds = async (req: Request, res: Response) => {
  try {
    const batchIds = req.query.batchIds as string;
    if (!batchIds) {
      return res.status(400).json({ error: 'batchIds parameter is required' });
    }

    // Support comma-separated batch IDs for multi-batch selection
    const batchIdArray = batchIds.split(',').map(id => id.trim()).filter(Boolean);

    if (batchIdArray.length === 0) {
      return res.status(400).json({ error: 'At least one batch ID is required' });
    }

    // Build parameterized query for multiple batch IDs
    const placeholders = batchIdArray.map((_, i) => `$${i + 1}`).join(', ');
    const wh = buildWarehouseFilter(req, batchIdArray.length + 1);
    const whClause = wh.clause ? `AND ${wh.clause}` : '';

    const result = await query(
      `SELECT wsn, wid, fsn, order_id, product_title, brand, mrp, fsp, 
              hsn_sac, igst_rate, cms_vertical, fkt_link, p_type, p_size, 
              vrp, yield_value, fk_grade, fkqc_remark, batch_id
       FROM master_data
       WHERE batch_id IN (${placeholders}) AND deleted_at IS NULL ${whClause}
       ORDER BY id
       LIMIT 50000`,
      [...batchIdArray, ...wh.params]
    );

    res.json({
      data: result.rows,
      batchIds: batchIdArray,
      count: result.rows.length
    });
  } catch (error: any) {
    console.error('❌ Get master data by batch error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

/**
 * Get master data in batches for frontend cache sync
 * Returns paginated data optimized for bulk caching
 */
export const getMasterDataBatch = async (req: Request, res: Response) => {
  try {
    const page = parseInt(req.query.page as string) || 1;
    const limit = Math.min(parseInt(req.query.limit as string) || 5000, 10000); // Cap at 10000 per request
    const offset = (page - 1) * limit;

    const wh = buildWarehouseFilter(req, 3);
    const whClause = wh.clause ? `AND ${wh.clause}` : '';

    const result = await query(
      `SELECT wsn, wid, fsn, order_id, product_title, brand, mrp, fsp, 
              hsn_sac, igst_rate, cms_vertical, fkt_link, p_type, p_size, 
              vrp, yield_value, fk_grade, fkqc_remark
       FROM master_data
       WHERE deleted_at IS NULL ${whClause}
       ORDER BY id
       LIMIT $1 OFFSET $2`,
      [limit, offset, ...wh.params]
    );

    res.json({
      data: result.rows,
      page,
      limit,
      count: result.rows.length
    });
  } catch (error: any) {
    console.error('❌ Get master data batch error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== END CACHE APIs ======

export const getMasterData = async (req: Request, res: Response) => {
  try {
    const { page = 1, limit = 100, search = '', batch_id = '', status = '', brand = '', category = '' } = req.query;
    // STABILITY: Cap limit at 500 server-side to prevent pool exhaustion
    // Heavy EXISTS subqueries × 1000 rows = connection held for 15-30s = pool starvation
    const safeLimitNum = Math.min(Number(limit) || 100, 500);
    const offset = ((Number(page) - 1) * safeLimitNum);
    let whereClause = '';
    const params: any[] = [];
    let paramIndex = 1;

    // Warehouse filter (always first)
    const wh = buildWarehouseFilter(req, paramIndex, 'md');
    if (wh.clause) {
      whereClause += wh.clause;
      params.push(...wh.params);
      paramIndex = wh.nextIndex;
    }

    // Build WHERE clause for search and batch_id
    if (search && search !== '') {
      whereClause += whereClause ? ` AND ` : '';
      whereClause += `(wsn ILIKE $${paramIndex} OR fsn ILIKE $${paramIndex} OR brand ILIKE $${paramIndex} OR product_title ILIKE $${paramIndex})`;
      params.push(`%${search}%`);
      paramIndex++;
    }

    if (batch_id && batch_id !== '') {
      whereClause += whereClause ? ` AND batch_id = $${paramIndex}` : `batch_id = $${paramIndex}`;
      params.push(batch_id);
      paramIndex++;
    }

    if (brand && brand !== '') {
      whereClause += whereClause ? ` AND brand = $${paramIndex}` : `brand = $${paramIndex}`;
      params.push(brand);
      paramIndex++;
    }

    if (category && category !== '') {
      whereClause += whereClause ? ` AND cms_vertical = $${paramIndex}` : `cms_vertical = $${paramIndex}`;
      params.push(category);
      paramIndex++;
    }

    // Status filter - warehouse-scoped subqueries
    let statusWhereClause = '';
    if (status && status !== '' && status !== 'All') {
      if (status === 'Received') {
        statusWhereClause = ` AND EXISTS(SELECT 1 FROM inbound WHERE inbound.wsn = md.wsn AND inbound.warehouse_id = md.warehouse_id)`;
      } else if (status === 'Receiving') {
        statusWhereClause = ` AND EXISTS(SELECT 1 FROM receiving_wsns WHERE UPPER(receiving_wsns.wsn) = UPPER(md.wsn) AND receiving_wsns.warehouse_id = md.warehouse_id) AND NOT EXISTS(SELECT 1 FROM inbound WHERE inbound.wsn = md.wsn AND inbound.warehouse_id = md.warehouse_id)`;
      } else if (status === 'Rejected') {
        statusWhereClause = ` AND EXISTS(SELECT 1 FROM rejections WHERE UPPER(rejections.wsn) = UPPER(md.wsn) AND rejections.warehouse_id = md.warehouse_id)`;
      } else if (status === 'Pending') {
        statusWhereClause = ` AND NOT EXISTS(SELECT 1 FROM inbound WHERE inbound.wsn = md.wsn AND inbound.warehouse_id = md.warehouse_id) AND NOT EXISTS(SELECT 1 FROM receiving_wsns WHERE UPPER(receiving_wsns.wsn) = UPPER(md.wsn) AND receiving_wsns.warehouse_id = md.warehouse_id) AND NOT EXISTS(SELECT 1 FROM rejections WHERE UPPER(rejections.wsn) = UPPER(md.wsn) AND rejections.warehouse_id = md.warehouse_id)`;
      }
    }

    // Add limit and offset at the end
    params.push(safeLimitNum);
    params.push(offset);

    const finalWhereClause = whereClause ? `WHERE ${whereClause} AND md.deleted_at IS NULL` : 'WHERE md.deleted_at IS NULL';

    // Build count query params (clone from data query but without limit/offset)
    const countParams = params.slice(0, -2); // Remove limit and offset
    const countFinalWhereClause = finalWhereClause; // Same WHERE clause

    // PERFORMANCE: Run data + count queries IN PARALLEL instead of sequentially
    // This halves the total connection hold time (was: ~30s sequential → ~15s parallel)
    const [result, countResult] = await Promise.all([
      // Data query with status CASE
      query(
        `SELECT md.*,
                CASE 
                  WHEN EXISTS(SELECT 1 FROM inbound WHERE inbound.wsn = md.wsn AND inbound.warehouse_id = md.warehouse_id) THEN 'Received'
                  WHEN EXISTS(SELECT 1 FROM receiving_wsns WHERE UPPER(receiving_wsns.wsn) = UPPER(md.wsn) AND receiving_wsns.warehouse_id = md.warehouse_id) THEN 'Receiving'
                  WHEN EXISTS(SELECT 1 FROM rejections WHERE UPPER(rejections.wsn) = UPPER(md.wsn) AND rejections.warehouse_id = md.warehouse_id) THEN 'Rejected'
                  ELSE 'Pending'
                END as actual_received
         FROM master_data md
         ${finalWhereClause}
         ${statusWhereClause}
         ORDER BY md.created_at DESC
         LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`,
        params
      ),
      // Count query - SIMPLIFIED: skip expensive status EXISTS when no status filter
      // (status filtering on COUNT is redundant when status='All' or empty)
      query(
        `SELECT COUNT(*) FROM master_data md
         ${countFinalWhereClause}
         ${statusWhereClause}`,
        countParams
      ),
    ]);

    res.json({
      data: result.rows,
      total: parseInt(countResult.rows[0].count),
      page: Number(page),
      limit: safeLimitNum,
    });

  } catch (error: any) {
    console.error('❌ Get master data error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// Download Excel Template 
export const downloadTemplate = async (req: Request, res: Response) => {
  try {
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Master Data');

    // Add header row with formatting
    worksheet.addRow(REQUIRED_COLUMNS);
    const headerRow = worksheet.getRow(1);
    headerRow.font = { bold: true, color: { argb: 'FFFFFFFF' } };
    headerRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF0070C0' } };
    // ✅ FIX: Use 'middle' instead of 'center' for vertical alignment
    headerRow.alignment = { horizontal: 'center', vertical: 'middle', wrapText: false };

    // Set column widths for clean view
    REQUIRED_COLUMNS.forEach((_, idx) => {
      worksheet.getColumn(idx + 1).width = 18;
    });

    // Freeze header row
    worksheet.views = [{ state: 'frozen', ySplit: 1 }];

    // Add sample row (light gray - just for reference)
    const sampleRow = worksheet.addRow([
      'WSN001', 'WID001', 'FSN001', 'ORD001', 'Remark', 'Grade-A', 'Product Name',
      '12345', '5', '100', '150', '2025-01-01', 'https://link.com',
      'Rack-A1', 'BrandName', 'Vertical', '120', '95', 'Type-A', 'Size-M'
    ]);
    sampleRow.font = { italic: true, color: { argb: 'FF808080' } };

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename=Master_Data_Template.xlsx');

    await workbook.xlsx.write(res);
    res.end();

  } catch (error: any) {
    console.error('❌ Template download error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

//✅ Upload - Stream processing (NO memory issues)
// export const uploadMasterData = async (req: Request, res: Response) => {
//   try {
//     if (!req.file) {
//       return res.status(400).json({ error: 'No file uploaded' });
//     }

//     const filePath = req.file.path;
//     const fileExt = path.extname(req.file.originalname).toLowerCase();
//     const fileSize = (req.file.size / 1024 / 1024).toFixed(2);

//     console.log(`📤 Upload: ${req.file.originalname} (${fileSize}MB)`);

//     const batchId = generateBatchId('BULK');
//     const jobId = `job_${Date.now()}`;
//     const uploadTimestampUTC = new Date().toISOString();  // ← FIX


//     // Immediate response
//     res.status(202).json({
//       message: 'Upload started',
//       jobId,
//       batchId,
//       fileSize
//     });

//     // Background processing
//     if (fileExt === '.csv') {
//       processCSVStream(filePath, batchId, jobId, uploadTimestampUTC);
//     } else if (fileExt === '.xlsx' || fileExt === '.xls') {
//       processExcelStream(filePath, batchId, jobId, uploadTimestampUTC);

//     } else {
//       // ✅ FIX: Reject invalid formats
//       saveProgress(jobId, {
//         status: 'failed',
//         error: `Invalid file format: ${fileExt}. Only .xlsx, .xls, .csv allowed`,
//         batchId
//       });
//       cleanup(filePath, jobId);
//     }

//   } catch (error: any) {
//     console.error('❌ Upload error:', error);
//     if (req.file?.path) {
//       try { fs.unlinkSync(req.file.path); } catch (e) { }
//     }
//     res.status(500).json({ error: safeError(error) });
//   }
// };
// ✅ Phase 2: Upload with Supabase Storage + DB logging
export const uploadMasterData = async (req: Request, res: Response) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const filePath = req.file.path;
    const fileExt = path.extname(req.file.originalname).toLowerCase();
    const fileSize = (req.file.size / 1024 / 1024).toFixed(2);

    console.log(`📤 Upload: ${req.file.originalname} (${fileSize}MB)`);

    // ✅ Validate file extension FIRST
    if (fileExt !== '.csv' && fileExt !== '.xlsx' && fileExt !== '.xls') {
      try { fs.unlinkSync(filePath); } catch (e) { }
      return res.status(400).json({
        error: `Invalid file format: ${fileExt}. Only .xlsx, .xls, and .csv files are allowed.`
      });
    }

    // ✅ Validate file size (max 50MB)
    const maxSizeMB = 50;
    if (req.file.size > maxSizeMB * 1024 * 1024) {
      try { fs.unlinkSync(filePath); } catch (e) { }
      return res.status(400).json({
        error: `File size exceeds ${maxSizeMB}MB limit. Please upload a smaller file.`
      });
    }

    const batchId = generateBatchId('BULK');
    const jobId = `job_${Date.now()}`;
    const uploadTimestampUTC = new Date().toISOString();

    // ✅ Read warehouse_id from form data (required)
    const warehouseId = req.body?.warehouse_id ? Number(req.body.warehouse_id) : null;
    if (!warehouseId) {
      try { fs.unlinkSync(filePath); } catch (e) { }
      return res.status(400).json({ error: 'warehouse_id is required for master data upload' });
    }

    // ✅ Phase 3: Read duplicate strategy from form data (default: skip)
    const rawStrategy = req.body?.duplicateStrategy || 'skip';
    const duplicateStrategy: DuplicateStrategy =
      (['skip', 'update', 'replace'].includes(rawStrategy)) ? rawStrategy as DuplicateStrategy : 'skip';
    console.log(`📋 Duplicate strategy: ${duplicateStrategy}`);

    // ✅ Phase 2: Compute SHA-256 file hash for duplicate detection
    let fileHash = '';
    try {
      fileHash = computeFileHash(filePath);
      console.log(`🔒 File hash: ${fileHash.substring(0, 16)}...`);

      // Check if same file was already uploaded recently (within 24h)
      const duplicateCheck = await query(
        `SELECT job_id, batch_id, status, uploaded_at FROM upload_logs
         WHERE file_hash_sha256 = $1 AND uploaded_at > NOW() - INTERVAL '24 hours'
         ORDER BY uploaded_at DESC LIMIT 1`,
        [fileHash]
      );

      if (duplicateCheck.rows.length > 0) {
        const prev = duplicateCheck.rows[0];
        console.log(`⚠️ Duplicate file detected! Previous upload: ${prev.job_id} (${prev.status})`);
        // Don't block - just warn. The user may intentionally re-upload.
      }
    } catch (hashErr: any) {
      console.error('⚠️ File hash computation failed (non-blocking):', hashErr.message);
    }

    // ✅ Phase 2: Upload file to Supabase Storage
    let storagePath = '';
    try {
      if (isSupabaseStorageConfigured()) {
        await ensureBucketExists(STORAGE_BUCKETS.UPLOADS, false);
        const storageFileName = `master-data/${batchId}/${req.file.originalname}`;
        const uploaded = await uploadToSupabase(filePath, storageFileName, STORAGE_BUCKETS.UPLOADS);
        if (uploaded) {
          storagePath = storageFileName;
          console.log(`☁️ File uploaded to Supabase: ${storageFileName}`);
        }
      }
    } catch (storageErr: any) {
      console.error('⚠️ Supabase upload failed (non-blocking):', storageErr.message);
    }

    // ✅ Phase 2: Create upload_logs entry
    const userId = req.user?.userId;
    await createUploadLog({
      jobId, batchId,
      filename: req.file.originalname,
      fileSize: req.file.size,
      fileHash,
      fileType: fileExt.replace('.', ''),
      storagePath,
      uploadedBy: userId,
      duplicateStrategy,
      warehouseId
    });

    // ✅ Phase 2: Initialize DB progress
    await saveProgressDB(jobId, {
      status: 'processing',
      processed: 0,
      total: 0,
      successCount: 0,
      batchId
    });

    // Immediate response
    res.status(202).json({
      message: 'Upload started',
      jobId,
      batchId,
      fileSize,
      fileHash: fileHash.substring(0, 16),
      duplicateStrategy
    });

    // Background processing
    if (fileExt === '.csv') {
      processCSVStream(filePath, batchId, jobId, uploadTimestampUTC, duplicateStrategy, warehouseId);
    } else if (fileExt === '.xlsx' || fileExt === '.xls') {
      processExcelStream(filePath, batchId, jobId, uploadTimestampUTC, duplicateStrategy, warehouseId);
    }

  } catch (error: any) {
    console.error('❌ Upload error:', error);
    if (req.file?.path) {
      try { fs.unlinkSync(req.file.path); } catch (e) { }
    }
    res.status(500).json({ error: safeError(error) });
  }
};


//✅ Phase 3: CSV Stream Processing with duplicate strategy support
async function processCSVStream(filePath: string, batchId: string, jobId: string, uploadTimestampUTC: string, duplicateStrategy: DuplicateStrategy = 'skip', warehouseId: number) {
  const CHUNK_SIZE = 2000;
  let rows: any[] = [];
  let total = 0;
  let success = 0;
  let duplicates = 0;
  let skipped = 0;
  const allDuplicateWsns: string[] = [];
  let headerValidated = false;
  let isFirstRow = true;

  try {
    saveProgressDB(jobId, {
      status: 'processing',
      processed: 0,
      total: 0,
      successCount: 0,
      duplicateCount: 0,
      batchId
    });

    const stream = createReadStream(filePath).pipe(csv());

    stream.on('data', async (row: any) => {
      // ✅ FIX: Skip header row in CSV properly
      if (isFirstRow) {
        isFirstRow = false;

        // Validate header on first row
        if (!headerValidated) {
          headerValidated = true;
          const headerRow = Object.keys(row);
          const normalizedHeader = headerRow.map(h => h.trim().toLowerCase());
          const normalizedRequired = REQUIRED_COLUMNS.map(c => c.trim().toLowerCase());

          const isValid = normalizedRequired.every(col =>
            normalizedHeader.includes(col)
          );

          if (!isValid) {
            console.warn('⚠️ CSV header mismatch!');
            console.warn('Expected:', REQUIRED_COLUMNS);
            console.warn('Got:', headerRow);
            // End stream - format invalid
            stream.destroy();
            saveProgressDB(jobId, {
              status: 'failed',
              error: 'CSV header format does not match required columns',
              batchId
            });
            finalizeUploadLog(jobId, { status: 'failed', totalRows: 0, successCount: 0, errorMessage: 'CSV header format does not match required columns' });
            cleanup(filePath, jobId);
            return;
          }
        }
        return; // Skip header row
      }

      const wsn = row['WSN'] || row['wsn'];
      if (!wsn || String(wsn).trim() === '') return;

      // ✅ Validate mandatory fields — reject rows with missing data
      const missingFields = validateRowMandatoryFields(row, true);
      if (missingFields.length > 0) {
        total++;
        skipped++;
        // Track first 100 validation errors for user feedback
        if (allDuplicateWsns.length < 100) {
          allDuplicateWsns.push(`ROW_INVALID:${String(wsn).trim()}:Missing ${missingFields.join(',')}`);
        }
        saveProgressDB(jobId, {
          status: 'processing',
          processed: total,
          total: total,
          successCount: success,
          duplicateCount: duplicates,
          batchId
        });
        return;
      }

      // ✅ FIX: Ensure all values are strings, not objects
      rows.push({
        wsn: String(wsn).trim().toUpperCase(),
        wid: row['WID'] ? String(row['WID']).trim() : null,
        fsn: row['FSN'] ? String(row['FSN']).trim() : null,
        order_id: row['Order_ID'] ? String(row['Order_ID']).trim() : null,
        fkqc_remark: row['FKQC_Remark'] ? String(row['FKQC_Remark']).trim() : null,
        fk_grade: row['FK_Grade'] ? String(row['FK_Grade']).trim() : null,
        product_title: row['Product_Title'] ? String(row['Product_Title']).trim() : null,
        hsn_sac: row['HSN/SAC'] ? String(row['HSN/SAC']).trim() : null,
        igst_rate: row['IGST_Rate'] ? String(row['IGST_Rate']).trim() : null,
        fsp: row['FSP'] ? String(row['FSP']).trim() : null,
        mrp: row['MRP'] ? String(row['MRP']).trim() : null,
        invoice_date: row['Invoice_Date'] ? String(row['Invoice_Date']).trim() : null,
        fkt_link: row['Fkt_Link'] ? String(row['Fkt_Link']).trim() : null,
        wh_location: row['Wh_Location'] ? String(row['Wh_Location']).trim() : null,
        brand: row['BRAND'] ? String(row['BRAND']).trim() : null,
        cms_vertical: row['cms_vertical'] ? String(row['cms_vertical']).trim() : null,
        vrp: row['VRP'] ? String(row['VRP']).trim() : null,
        yield_value: row['Yield_Value'] ? String(row['Yield_Value']).trim() : null,
        p_type: row['P_Type'] ? String(row['P_Type']).trim() : null,
        p_size: row['P_Size'] ? String(row['P_Size']).trim() : null,
        batchId,
        uploadTimestampUTC,
        warehouse_id: warehouseId
      });

      total++;

      if (rows.length >= CHUNK_SIZE) {
        stream.pause();
        try {
          const batchResult = await insertBatch(rows, duplicateStrategy);
          success += batchResult.inserted + batchResult.updated;
          duplicates += batchResult.duplicateWsns.length;
          skipped += batchResult.skipped;
          allDuplicateWsns.push(...batchResult.duplicateWsns);
          rows = [];
          saveProgressDB(jobId, {
            status: 'processing',
            processed: total,
            total: total,
            successCount: success,
            duplicateCount: duplicates,
            batchId
          });
          stream.resume();
        } catch (err) {
          console.error('❌ Batch insert error:', err);
          stream.resume();
        }
      }
    });

    stream.on('end', async () => {
      if (rows.length > 0) {
        try {
          const batchResult = await insertBatch(rows, duplicateStrategy);
          success += batchResult.inserted + batchResult.updated;
          duplicates += batchResult.duplicateWsns.length;
          skipped += batchResult.skipped;
          allDuplicateWsns.push(...batchResult.duplicateWsns);
        } catch (err) {
          console.error('❌ Final batch error:', err);
        }
      }

      // Count rows rejected due to missing mandatory fields
      const validationRejected = allDuplicateWsns.filter(w => w.startsWith('ROW_INVALID:')).length;
      const validationErrorMsg = validationRejected > 0
        ? `${validationRejected} row(s) rejected due to missing mandatory fields (WSN, WID, FSN, Order_ID, Product_Title, HSN/SAC, IGST_Rate, FSP, MRP, Fkt_Link, Wh_Location, BRAND, cms_vertical, VRP).`
        : null;

      saveProgressDB(jobId, {
        status: 'completed',
        processed: total,
        total: total,
        successCount: success,
        duplicateCount: duplicates,
        error: validationErrorMsg,
        batchId
      });

      console.log(`✅ CSV complete: ${success}/${total} rows (duplicates: ${duplicates}, skipped: ${skipped}, validationRejected: ${validationRejected}, strategy: ${duplicateStrategy})`);
      finalizeUploadLog(jobId, {
        status: 'completed', totalRows: total, successCount: success,
        duplicateCount: duplicates, skippedCount: skipped,
        errorMessage: validationErrorMsg || undefined,
        duplicateWsns: allDuplicateWsns
      });
      cleanup(filePath, jobId);
    });

    stream.on('error', (err) => {
      console.error('❌ Stream error:', err);
      saveProgressDB(jobId, { status: 'failed', error: err.message, batchId });
      finalizeUploadLog(jobId, {
        status: 'failed', totalRows: total, successCount: success,
        duplicateCount: duplicates, skippedCount: skipped,
        errorMessage: err.message, duplicateWsns: allDuplicateWsns
      });
      cleanup(filePath, jobId);
    });

  } catch (error: any) {
    console.error('❌ CSV processing error:', error);
    saveProgressDB(jobId, { status: 'failed', error: error.message, batchId });
    finalizeUploadLog(jobId, {
      status: 'failed', totalRows: total, successCount: success,
      duplicateCount: duplicates, skippedCount: skipped,
      errorMessage: error.message, duplicateWsns: allDuplicateWsns
    });
    cleanup(filePath, jobId);
  }
}

// ✅ Excel Stream Processing with validation 
// async function processExcelStream(filePath: string, batchId: string, jobId: string, uploadTimestampUTC: string) {
//   const CHUNK_SIZE = 1000;
//   let rows: any[] = [];
//   let total = 0;
//   let success = 0;
//   let isFirstRow = true;
//   let headerValidationDone = false;

//   try {
//     saveProgress(jobId, {
//       status: "processing",
//       processed: 0,
//       total: 0,
//       successCount: 0,
//       batchId,
//     });

//     const workbookReader = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {
//       entries: "emit",
//       sharedStrings: "cache",
//       worksheets: "emit",
//       styles: "cache",
//     });

//     let shouldStop = false;

//     for await (const worksheetReader of workbookReader) {
//       if (shouldStop) break;

//       for await (const row of worksheetReader) {
//         if (shouldStop) break;

//         const values = row.values as ExcelJS.CellValue[];

//         // ✅ FIX: Skip header row and validate
//         if (isFirstRow) {
//           isFirstRow = false;

//           if (!headerValidationDone) {
//             headerValidationDone = true;

//             // Get header cells (indices 1-20, skipping index 0)
//             const headerCells = values.slice(1, 21).map(v => {
//               if (v === null || v === undefined) return '';
//               return String(v).trim().toLowerCase();
//             });

//             const normalizedRequired = REQUIRED_COLUMNS.map(c => c.trim().toLowerCase());

//             // Check if all required columns are present
//             const isValidHeader = normalizedRequired.every((col, idx) => {
//               return (headerCells[idx] || '') === col;
//             });

//             if (!isValidHeader) {
//               console.warn('⚠️ Excel header validation failed!');
//               console.warn('Expected:', normalizedRequired);
//               console.warn('Got:', headerCells);

//               // Stop processing
//               saveProgress(jobId, {
//                 status: 'failed',
//                 error: 'Excel header format does not match required columns. Expected exact order: ' + REQUIRED_COLUMNS.join(', '),
//                 batchId
//               });

//               shouldStop = true;
//               break;
//             }
//           }
//           continue; // Skip header row
//         }

//         const wsn = values[1];
//         if (!wsn || String(wsn).trim() === '') continue;

//         // ✅ FIX: Convert all cell values to strings to prevent [object Object]
//         rows.push({
//           wsn: String(wsn || '').trim(),
//           wid: values[2] ? String(values[2]).trim() : null,
//           fsn: values[3] ? String(values[3]).trim() : null,
//           order_id: values[4] ? String(values[4]).trim() : null,
//           fkqc_remark: values[5] ? String(values[5]).trim() : null,
//           fk_grade: values[6] ? String(values[6]).trim() : null,
//           product_title: values[7] ? String(values[7]).trim() : null,
//           hsn_sac: values[8] ? String(values[8]).trim() : null,
//           igst_rate: values[9] ? String(values[9]).trim() : null,
//           fsp: values[10] ? String(values[10]).trim() : null,
//           mrp: values[11] ? String(values[11]).trim() : null,
//           invoice_date: values[12] ? String(values[12]).trim() : null,
//           fkt_link: values[13] ? String(values[13]).trim() : null,
//           wh_location: values[14] ? String(values[14]).trim() : null,
//           brand: values[15] ? String(values[15]).trim() : null,
//           cms_vertical: values[16] ? String(values[16]).trim() : null,
//           vrp: values[17] ? String(values[17]).trim() : null,
//           yield_value: values[18] ? String(values[18]).trim() : null,
//           p_type: values[19] ? String(values[19]).trim() : null,
//           p_size: values[20] ? String(values[20]).trim() : null,
//           batchId,
//           uploadTimestampUTC
//         });

//         total++;

//         if (rows.length >= CHUNK_SIZE) {
//           try {
//             await insertBatch(rows);
//             success += rows.length;
//             rows = [];
//             saveProgress(jobId, {
//               status: "processing",
//               processed: total,
//               total,
//               successCount: success,
//               batchId,
//             });

//             await new Promise((r) => setTimeout(r, 50));
//           } catch (err) {
//             console.error("❌ Batch insert error:", err);
//           }
//         }
//       }
//     }

//     if (shouldStop) {
//       cleanup(filePath, jobId);
//       return;
//     }

//     if (rows.length > 0) {
//       await insertBatch(rows);
//       success += rows.length;
//     }

//     saveProgress(jobId, {
//       status: "completed",
//       processed: total,
//       total,
//       successCount: success,
//       batchId,
//     });

//     console.log(`✅ Excel complete: ${success}/${total} rows`);
//     cleanup(filePath, jobId);

//   } catch (error: any) {
//     console.error("❌ Excel stream error:", error);
//     saveProgress(jobId, { status: "failed", error: error.message, batchId });
//     cleanup(filePath, jobId);
//   }
// }
// ✅ Phase 3: Excel Stream Processing with duplicate strategy support
async function processExcelStream(filePath: string, batchId: string, jobId: string, uploadTimestampUTC: string, duplicateStrategy: DuplicateStrategy = 'skip', warehouseId: number) {
  const CHUNK_SIZE = 1000;
  let rows: any[] = [];
  let total = 0;
  let success = 0;
  let duplicates = 0;
  let skipped = 0;
  const allDuplicateWsns: string[] = [];
  let isFirstRow = true;
  let headerValidationDone = false;
  let workbookReader: ExcelJS.stream.xlsx.WorkbookReader | null = null;

  try {
    saveProgressDB(jobId, {
      status: "processing",
      processed: 0,
      total: 0,
      successCount: 0,
      batchId,
    });

    // ✅ STEP 1: Validate file exists and is readable
    if (!fs.existsSync(filePath)) {
      throw new Error('Uploaded file not found');
    }

    const stats = fs.statSync(filePath);
    if (stats.size === 0) {
      throw new Error('Uploaded file is empty');
    }

    // ✅ STEP 2: Validate file is a valid Excel file (check magic bytes)
    const buffer = Buffer.alloc(8);
    const fd = fs.openSync(filePath, 'r');
    fs.readSync(fd, buffer, 0, 8, 0);
    fs.closeSync(fd);

    // Excel files start with PK (ZIP format) - magic bytes: 50 4B
    if (buffer[0] !== 0x50 || buffer[1] !== 0x4B) {
      throw new Error('Invalid Excel file format. File appears to be corrupted or not a valid .xlsx file');
    }

    // ✅ STEP 3: Try to initialize workbook reader with error handling
    try {
      workbookReader = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {
        entries: "emit",
        sharedStrings: "cache",
        worksheets: "emit",
        styles: "cache",
      });
    } catch (readerError: any) {
      throw new Error(`Failed to read Excel file: ${readerError.message || 'File may be corrupted'}`);
    }

    let shouldStop = false;
    let worksheetCount = 0;

    // ✅ STEP 4: Process worksheets with validation
    for await (const worksheetReader of workbookReader) {
      if (shouldStop) break;

      worksheetCount++;

      // Validate worksheet structure
      if (!worksheetReader || typeof worksheetReader !== 'object') {
        throw new Error('Invalid worksheet structure detected in Excel file');
      }

      for await (const row of worksheetReader) {
        if (shouldStop) break;

        // Validate row structure
        if (!row || !row.values || !Array.isArray(row.values)) {
          console.warn('⚠️ Skipping invalid row structure');
          continue;
        }

        const values = row.values as ExcelJS.CellValue[];

        // ✅ Header validation (first row only)
        if (isFirstRow) {
          isFirstRow = false;

          if (!headerValidationDone) {
            headerValidationDone = true;

            // Get header cells (indices 1-20, skipping index 0)
            const headerCells = values.slice(1, 21).map(v => {
              if (v === null || v === undefined) return '';
              return String(v).trim().toLowerCase();
            });

            const normalizedRequired = REQUIRED_COLUMNS.map(c => c.trim().toLowerCase());

            // Check if all required columns are present
            const isValidHeader = normalizedRequired.every((col, idx) => {
              return (headerCells[idx] || '') === col;
            });

            if (!isValidHeader) {
              console.warn('⚠️ Excel header validation failed!');
              console.warn('Expected:', normalizedRequired);
              console.warn('Got:', headerCells);

              // Create detailed error message
              const missingCols = normalizedRequired.filter((col, idx) =>
                (headerCells[idx] || '') !== col
              );

              const errorMsg = `Excel header format does not match template. 
                
Expected columns in exact order:
${REQUIRED_COLUMNS.join(', ')}

Please download the template file and ensure your Excel file matches the exact format.`;

              // Stop processing
              saveProgressDB(jobId, {
                status: 'failed',
                error: errorMsg,
                batchId
              });
              finalizeUploadLog(jobId, { status: 'failed', totalRows: 0, successCount: 0, errorMessage: errorMsg });

              shouldStop = true;
              break;
            }
          }
          continue; // Skip header row
        }

        const wsn = extractCellText(values[1]);
        if (!wsn || wsn.trim() === '') continue;

        // ✅ Validate mandatory fields — reject rows with missing data
        const excelRowData: Record<string, any> = {
          'WSN': values[1], 'WID': values[2], 'FSN': values[3], 'Order_ID': values[4],
          'FKQC_Remark': values[5], 'FK_Grade': values[6], 'Product_Title': values[7],
          'HSN/SAC': values[8], 'IGST_Rate': values[9], 'FSP': values[10], 'MRP': values[11],
          'Invoice_Date': values[12], 'Fkt_Link': values[13], 'Wh_Location': values[14],
          'BRAND': values[15], 'cms_vertical': values[16], 'VRP': values[17],
          'Yield_Value': values[18], 'P_Type': values[19], 'P_Size': values[20]
        };
        const missingFields = validateRowMandatoryFields(excelRowData, false);
        if (missingFields.length > 0) {
          total++;
          skipped++;
          if (allDuplicateWsns.length < 100) {
            allDuplicateWsns.push(`ROW_INVALID:${String(wsn).trim()}:Missing ${missingFields.join(',')}`);
          }
          saveProgressDB(jobId, {
            status: 'processing',
            processed: total,
            total,
            successCount: success,
            duplicateCount: duplicates,
            batchId,
          });
          continue;
        }

        // ✅ Convert all cell values to strings safely using extractCellText
        // This handles ExcelJS rich text, hyperlinks, formulas, dates properly
        rows.push({
          wsn: (extractCellText(wsn) || '').trim().toUpperCase(),
          wid: extractCellText(values[2])?.trim() || null,
          fsn: extractCellText(values[3])?.trim() || null,
          order_id: extractCellText(values[4])?.trim() || null,
          fkqc_remark: extractCellText(values[5])?.trim() || null,
          fk_grade: extractCellText(values[6])?.trim() || null,
          product_title: extractCellText(values[7])?.trim() || null,
          hsn_sac: extractCellText(values[8])?.trim() || null,
          igst_rate: extractCellText(values[9])?.trim() || null,
          fsp: extractCellText(values[10])?.trim() || null,
          mrp: extractCellText(values[11])?.trim() || null,
          invoice_date: extractCellText(values[12])?.trim() || null,
          fkt_link: extractCellText(values[13])?.trim() || null,
          wh_location: extractCellText(values[14])?.trim() || null,
          brand: extractCellText(values[15])?.trim() || null,
          cms_vertical: extractCellText(values[16])?.trim() || null,
          vrp: extractCellText(values[17])?.trim() || null,
          yield_value: extractCellText(values[18])?.trim() || null,
          p_type: extractCellText(values[19])?.trim() || null,
          p_size: extractCellText(values[20])?.trim() || null,
          batchId,
          uploadTimestampUTC,
          warehouse_id: warehouseId
        });

        total++;

        if (rows.length >= CHUNK_SIZE) {
          try {
            const batchResult = await insertBatch(rows, duplicateStrategy);
            success += batchResult.inserted + batchResult.updated;
            duplicates += batchResult.duplicateWsns.length;
            skipped += batchResult.skipped;
            allDuplicateWsns.push(...batchResult.duplicateWsns);
            rows = [];
            saveProgressDB(jobId, {
              status: "processing",
              processed: total,
              total,
              successCount: success,
              duplicateCount: duplicates,
              batchId,
            });

            await new Promise((r) => setTimeout(r, 50));
          } catch (err) {
            console.error("❌ Batch insert error:", err);
          }
        }
      }
    }

    // ✅ Validate that at least one worksheet was processed
    if (worksheetCount === 0) {
      throw new Error('Excel file contains no worksheets or is corrupted');
    }

    if (shouldStop) {
      cleanup(filePath, jobId);
      return;
    }

    // Process remaining rows
    if (rows.length > 0) {
      const batchResult = await insertBatch(rows, duplicateStrategy);
      success += batchResult.inserted + batchResult.updated;
      duplicates += batchResult.duplicateWsns.length;
      skipped += batchResult.skipped;
      allDuplicateWsns.push(...batchResult.duplicateWsns);
    }

    // Count rows rejected due to missing mandatory fields
    const validationRejected = allDuplicateWsns.filter(w => w.startsWith('ROW_INVALID:')).length;
    const validationErrorMsg = validationRejected > 0
      ? `${validationRejected} row(s) rejected due to missing mandatory fields (WSN, WID, FSN, Order_ID, Product_Title, HSN/SAC, IGST_Rate, FSP, MRP, Fkt_Link, Wh_Location, BRAND, cms_vertical, VRP).`
      : null;

    saveProgressDB(jobId, {
      status: "completed",
      processed: total,
      total,
      successCount: success,
      duplicateCount: duplicates,
      error: validationErrorMsg,
      batchId,
    });

    console.log(`✅ Excel complete: ${success}/${total} rows (duplicates: ${duplicates}, skipped: ${skipped}, validationRejected: ${validationRejected}, strategy: ${duplicateStrategy})`);
    finalizeUploadLog(jobId, {
      status: 'completed', totalRows: total, successCount: success,
      duplicateCount: duplicates, skippedCount: skipped,
      errorMessage: validationErrorMsg || undefined,
      duplicateWsns: allDuplicateWsns
    });
    cleanup(filePath, jobId);

  } catch (error: any) {
    console.error("❌ Excel stream error:", error);

    // Cleanup workbook reader if it exists
    if (workbookReader && typeof (workbookReader as any).destroy === 'function') {
      try {
        (workbookReader as any).destroy();
      } catch (e) {
        console.error('Error destroying workbook reader:', e);
      }
    }

    // Provide user-friendly error messages
    let errorMessage = error.message || 'Failed to process Excel file';

    // Specific error messages for common issues
    if (errorMessage.includes('sheets') || errorMessage.includes('undefined')) {
      errorMessage = 'Invalid or corrupted Excel file. Please ensure you are uploading a valid .xlsx file created from our template.';
    } else if (errorMessage.includes('magic bytes') || errorMessage.includes('PK')) {
      errorMessage = 'File is not a valid Excel format. Please upload only .xlsx files.';
    } else if (errorMessage.includes('header')) {
      // Keep the detailed header error message
      errorMessage = error.message;
    }

    saveProgressDB(jobId, {
      status: "failed",
      error: errorMessage,
      batchId
    });
    finalizeUploadLog(jobId, {
      status: 'failed', totalRows: total, successCount: success,
      duplicateCount: duplicates, skippedCount: skipped,
      errorMessage, duplicateWsns: allDuplicateWsns
    });

    cleanup(filePath, jobId);
  }
}


// ✅ Phase 3: Batch Insert with duplicate strategy support
async function insertBatch(rows: any[], duplicateStrategy: DuplicateStrategy = 'skip'): Promise<InsertBatchResult> {
  const result: InsertBatchResult = { inserted: 0, updated: 0, skipped: 0, duplicateWsns: [] };
  if (rows.length === 0) return result;

  // ✅ FIX: Remove duplicates within the batch to avoid "ON CONFLICT DO UPDATE command cannot affect row a second time"
  // Keep only the last occurrence of each WSN (latest data wins)
  const uniqueRows = new Map<string, any>();
  for (const row of rows) {
    if (row.wsn) {
      uniqueRows.set(row.wsn, row);
    }
  }
  const deduplicatedRows = Array.from(uniqueRows.values());

  if (deduplicatedRows.length === 0) return result;

  // Safety guard: PostgreSQL has a 65535 parameter limit
  // With 23 params per row (added warehouse_id), max safe batch is ~2800 rows
  const PARAMS_PER_ROW = 23;
  const MAX_PARAMS = 65000; // Leave some headroom below 65535
  const MAX_ROWS_PER_BATCH = Math.floor(MAX_PARAMS / PARAMS_PER_ROW);

  for (let batchStart = 0; batchStart < deduplicatedRows.length; batchStart += MAX_ROWS_PER_BATCH) {
    const batchRows = deduplicatedRows.slice(batchStart, batchStart + MAX_ROWS_PER_BATCH);

    // ✅ Phase 3: Pre-scan — find which WSNs already exist in DB (active records only, same warehouse)
    const wsns = batchRows.map(r => r.wsn);
    const whId = batchRows[0]?.warehouse_id;
    let existingWsns: Set<string> = new Set();
    try {
      const existingResult = await query(
        `SELECT wsn FROM master_data WHERE wsn = ANY($1) AND warehouse_id = $2 AND deleted_at IS NULL`,
        [wsns, whId]
      );
      existingWsns = new Set(existingResult.rows.map((r: any) => r.wsn));
    } catch (err: any) {
      console.error('⚠️ Pre-scan for duplicates failed (non-blocking):', err.message);
    }
    const batchDuplicateWsns = wsns.filter(w => existingWsns.has(w));
    result.duplicateWsns.push(...batchDuplicateWsns);

    // Build VALUES clause (shared across all strategies)
    const valuesSqlParts: string[] = [];
    const params: any[] = [];
    let idx = 1;

    for (const row of batchRows) {
      // Convert the upload timestamp ISO -> epoch seconds (float)
      let createdAtEpoch: number;
      try {
        const ms = Date.parse(row.uploadTimestampUTC);
        createdAtEpoch = isNaN(ms) ? Date.now() / 1000 : ms / 1000;
      } catch {
        createdAtEpoch = Date.now() / 1000;
      }

      const paramList = [
        row.wsn, row.wid, row.fsn, row.order_id, row.fkqc_remark, row.fk_grade,
        row.product_title, row.hsn_sac, row.igst_rate, row.fsp, row.mrp,
        row.invoice_date, row.fkt_link, row.wh_location, row.brand,
        row.cms_vertical, row.vrp, row.yield_value, row.p_type, row.p_size,
        row.batchId, row.warehouse_id, createdAtEpoch
      ];

      const placeholdersForRow = paramList.map((_, i) => {
        const placeholderIndex = idx + i;
        if (i === paramList.length - 1) {
          return `to_timestamp($${placeholderIndex})`;
        }
        return `$${placeholderIndex}`;
      }).join(', ');

      valuesSqlParts.push(`(${placeholdersForRow})`);
      params.push(...paramList);
      idx += paramList.length;
    }

    const insertColumns = `wsn, wid, fsn, order_id, fkqc_remark, fk_grade, product_title, hsn_sac,
    igst_rate, fsp, mrp, invoice_date, fkt_link, wh_location, brand, cms_vertical,
    vrp, yield_value, p_type, p_size, batch_id, warehouse_id, created_at`;
    const valuesClause = valuesSqlParts.join(', ');

    // ✅ Phase 3: Apply duplicate strategy
    if (duplicateStrategy === 'replace' && batchDuplicateWsns.length > 0) {
      // REPLACE: Soft-delete existing records, then insert fresh — atomic transaction
      await withTransaction(async (client) => {
        await client.query(
          `UPDATE master_data SET deleted_at = NOW(), deleted_by = NULL
           WHERE wsn = ANY($1) AND warehouse_id = $2 AND deleted_at IS NULL`,
          [batchDuplicateWsns, whId]
        );
        const insertSql = `INSERT INTO master_data (${insertColumns})
          VALUES ${valuesClause}
          ON CONFLICT (wsn, warehouse_id) WHERE deleted_at IS NULL DO NOTHING`;
        const insertRes = await client.query(insertSql, params);
        const actualInserted = insertRes.rowCount || 0;
        result.inserted += actualInserted;
        result.skipped += batchRows.length - actualInserted;
      });
    } else if (duplicateStrategy === 'update') {
      // UPDATE: Overwrite all data columns for duplicates
      const sql = `INSERT INTO master_data (${insertColumns})
        VALUES ${valuesClause}
        ON CONFLICT (wsn, warehouse_id) WHERE deleted_at IS NULL DO UPDATE SET
          wid = EXCLUDED.wid,
          fsn = EXCLUDED.fsn,
          order_id = EXCLUDED.order_id,
          fkqc_remark = EXCLUDED.fkqc_remark,
          fk_grade = EXCLUDED.fk_grade,
          product_title = EXCLUDED.product_title,
          hsn_sac = EXCLUDED.hsn_sac,
          igst_rate = EXCLUDED.igst_rate,
          fsp = EXCLUDED.fsp,
          mrp = EXCLUDED.mrp,
          invoice_date = EXCLUDED.invoice_date,
          fkt_link = EXCLUDED.fkt_link,
          wh_location = EXCLUDED.wh_location,
          brand = EXCLUDED.brand,
          cms_vertical = EXCLUDED.cms_vertical,
          vrp = EXCLUDED.vrp,
          yield_value = EXCLUDED.yield_value,
          p_type = EXCLUDED.p_type,
          p_size = EXCLUDED.p_size,
          batch_id = EXCLUDED.batch_id,
          created_at = EXCLUDED.created_at`;
      await query(sql, params);
      // For DO UPDATE: all rows are affected (insert + update)
      const updatedInBatch = batchDuplicateWsns.length;
      result.updated += updatedInBatch;
      result.inserted += batchRows.length - updatedInBatch;
    } else {
      // SKIP (default): Don't touch existing duplicates
      const sql = `INSERT INTO master_data (${insertColumns})
        VALUES ${valuesClause}
        ON CONFLICT (wsn, warehouse_id) WHERE deleted_at IS NULL DO NOTHING`;
      const insertRes = await query(sql, params);
      const actualInserted = insertRes.rowCount || 0;
      result.inserted += actualInserted;
      result.skipped += batchRows.length - actualInserted;
    }
  } // end sub-batch loop

  return result;
}



function cleanup(filePath: string, jobId: string, csvPath?: string) {
  try { fs.unlinkSync(filePath); } catch (e) { }
  if (csvPath) try { fs.unlinkSync(csvPath); } catch (e) { }
  // DB progress auto-cleaned by TTL — no file cleanup needed
}

export const getUploadProgress = async (req: Request, res: Response) => {
  const { jobId } = req.params;
  const progress = await getProgressDB(jobId);
  res.json(progress || { status: 'not_found' });
};

export const cancelUpload = async (req: Request, res: Response) => {
  const { jobId } = req.params;
  try {
    await saveProgressDB(jobId, { status: 'cancelled', error: 'Cancelled by user' });
    await finalizeUploadLog(jobId, { status: 'cancelled', totalRows: 0, successCount: 0, errorMessage: 'Cancelled by user' });
    res.json({ message: 'Cancelled' });
  } catch (err: any) {
    res.json({ message: 'Cancelled' });
  }
};

export const getActiveUploads = async (req: Request, res: Response) => {
  try {
    const result = await query(
      `SELECT job_id, status, processed, total, success_count, batch_id, updated_at
       FROM upload_progress
       WHERE status = 'processing'
       ORDER BY updated_at DESC`
    );
    const jobs = result.rows.map((row: any) => ({
      jobId: row.job_id,
      status: row.status,
      processed: row.processed,
      total: row.total,
      successCount: row.success_count,
      batchId: row.batch_id
    }));
    res.json(jobs);
  } catch (error) {
    res.json([]);
  }
};

// ✅ Create single master data record
export const createMasterData = async (req: Request, res: Response) => {
  try {
    const {
      wsn, wid, fsn, order_id, fkqc_remark, fk_grade, product_title,
      hsn_sac, igst_rate, fsp, mrp, invoice_date, fkt_link,
      wh_location, brand, cms_vertical, vrp, yield_value, p_type, p_size,
      warehouse_id
    } = req.body;

    // Validate required field
    if (!wsn || String(wsn).trim() === '') {
      return res.status(400).json({ error: 'WSN is required' });
    }

    // ✅ Validate all mandatory data fields for single entry
    const mandatoryCheck: Record<string, any> = {
      WSN: wsn, WID: wid, FSN: fsn, Order_ID: order_id, Product_Title: product_title,
      'HSN/SAC': hsn_sac, IGST_Rate: igst_rate, FSP: fsp, MRP: mrp, Fkt_Link: fkt_link,
      Wh_Location: wh_location, BRAND: brand, cms_vertical, VRP: vrp
    };
    const missingFields: string[] = [];
    for (const [field, value] of Object.entries(mandatoryCheck)) {
      if (!value || String(value).trim() === '') missingFields.push(field);
    }
    if (missingFields.length > 0) {
      return res.status(400).json({
        error: `Missing required fields: ${missingFields.join(', ')}. All mandatory fields must have values.`
      });
    }

    if (!warehouse_id) {
      return res.status(400).json({ error: 'warehouse_id is required' });
    }

    // Check if WSN already exists (only active records in same warehouse)
    const existing = await query('SELECT id FROM master_data WHERE wsn = $1 AND warehouse_id = $2 AND deleted_at IS NULL', [wsn.trim(), warehouse_id]);
    if (existing.rows.length > 0) {
      return res.status(400).json({ error: `WSN "${wsn}" already exists in master data for this warehouse` });
    }

    // Generate batch ID for single entries
    const batchId = generateBatchId('SINGLE');

    const result = await query(
      `INSERT INTO master_data (
        wsn, wid, fsn, order_id, fkqc_remark, fk_grade, product_title,
        hsn_sac, igst_rate, fsp, mrp, invoice_date, fkt_link,
        wh_location, brand, cms_vertical, vrp, yield_value, p_type, p_size, batch_id, warehouse_id, created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, NOW())
      RETURNING *`,
      [
        wsn?.trim() || null,
        wid?.trim() || null,
        fsn?.trim() || null,
        order_id?.trim() || null,
        fkqc_remark?.trim() || null,
        fk_grade?.trim() || null,
        product_title?.trim() || null,
        hsn_sac?.trim() || null,
        igst_rate?.trim() || null,
        fsp?.trim() || null,
        mrp?.trim() || null,
        invoice_date?.trim() || null,
        fkt_link?.trim() || null,
        wh_location?.trim() || null,
        brand?.trim() || null,
        cms_vertical?.trim() || null,
        vrp?.trim() || null,
        yield_value?.trim() || null,
        p_type?.trim() || null,
        p_size?.trim() || null,
        batchId,
        warehouse_id
      ]
    );

    console.log(`✅ Created master data: WSN=${wsn}, Batch=${batchId}`);

    // Log for CCTV-style tracking
    logChangeSimple('master_data', 'INSERT', {
      id: result.rows[0].id, wsn: wsn?.trim(), newData: result.rows[0]
    }, { batchId, warehouseId: warehouse_id }).catch(() => { });

    res.status(201).json({ message: 'Product created successfully', data: result.rows[0] });
  } catch (error: any) {
    if (error.code === '23505') {
      return res.status(409).json({ error: `WSN already exists in master data (concurrent entry)` });
    }
    console.error('❌ Create master data error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ✅ Update master data record
export const updateMasterData = async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const {
      wsn, wid, fsn, order_id, fkqc_remark, fk_grade, product_title,
      hsn_sac, igst_rate, fsp, mrp, invoice_date, fkt_link,
      wh_location, brand, cms_vertical, vrp, yield_value, p_type, p_size
    } = req.body;

    // Check if record exists (only active records)
    const existing = await query('SELECT id, wsn FROM master_data WHERE id = $1 AND deleted_at IS NULL', [id]);
    if (existing.rows.length === 0) {
      return res.status(404).json({ error: 'Record not found' });
    }

    // If WSN is being changed, check for duplicates
    if (wsn && wsn.trim() !== existing.rows[0].wsn) {
      const duplicate = await query('SELECT id FROM master_data WHERE wsn = $1 AND id != $2 AND deleted_at IS NULL', [wsn.trim(), id]);
      if (duplicate.rows.length > 0) {
        return res.status(400).json({ error: `WSN "${wsn}" already exists in another record` });
      }
    }

    const result = await query(
      `UPDATE master_data SET
        wsn = COALESCE($1, wsn),
        wid = $2,
        fsn = $3,
        order_id = $4,
        fkqc_remark = $5,
        fk_grade = $6,
        product_title = $7,
        hsn_sac = $8,
        igst_rate = $9,
        fsp = $10,
        mrp = $11,
        invoice_date = $12,
        fkt_link = $13,
        wh_location = $14,
        brand = $15,
        cms_vertical = $16,
        vrp = $17,
        yield_value = $18,
        p_type = $19,
        p_size = $20
      WHERE id = $21 AND deleted_at IS NULL
      RETURNING *`,
      [
        wsn?.trim() || null,
        wid?.trim() || null,
        fsn?.trim() || null,
        order_id?.trim() || null,
        fkqc_remark?.trim() || null,
        fk_grade?.trim() || null,
        product_title?.trim() || null,
        hsn_sac?.trim() || null,
        igst_rate?.trim() || null,
        fsp?.trim() || null,
        mrp?.trim() || null,
        invoice_date?.trim() || null,
        fkt_link?.trim() || null,
        wh_location?.trim() || null,
        brand?.trim() || null,
        cms_vertical?.trim() || null,
        vrp?.trim() || null,
        yield_value?.trim() || null,
        p_type?.trim() || null,
        p_size?.trim() || null,
        id
      ]
    );

    console.log(`✅ Updated master data: ID=${id}, WSN=${wsn}`);

    // Log for CCTV-style tracking
    logChangeSimple('master_data', 'UPDATE', {
      id: Number(id), wsn: wsn?.trim(), oldData: existing.rows[0], newData: result.rows[0]
    }, {}).catch(() => { });

    res.json({ message: 'Product updated successfully', data: result.rows[0] });
  } catch (error: any) {
    if (error.code === '23505') {
      return res.status(409).json({ error: `WSN already exists in another record (concurrent update)` });
    }
    console.error('❌ Update master data error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

export const deleteMasterData = async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const userId = req.user?.userId ?? null;

    // Check if record exists first (only active records)
    const existing = await query('SELECT wsn FROM master_data WHERE id = $1 AND deleted_at IS NULL', [id]);
    if (existing.rows.length === 0) {
      return res.status(404).json({ error: 'Record not found' });
    }

    // ✅ Phase 2: Soft delete instead of hard delete
    await query(
      'UPDATE master_data SET deleted_at = NOW(), deleted_by = $2 WHERE id = $1 AND deleted_at IS NULL',
      [id, userId]
    );

    // Log for CCTV-style tracking
    logChangeSimple('master_data', 'DELETE', {
      id: Number(id), wsn: existing.rows[0].wsn, oldData: existing.rows[0]
    }, { userId }).catch(() => { });

    console.log(`✅ Soft-deleted master data: ID=${id}, WSN=${existing.rows[0].wsn}`);
    res.json({ message: 'Deleted successfully' });
  } catch (error: any) {
    console.error('❌ Delete master data error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

export const deleteBatch = async (req: Request, res: Response) => {
  try {
    const { batchId } = req.params;
    const userId = req.user?.userId ?? null;

    // Warehouse scoping — restrict to accessible warehouses
    const wh = buildWarehouseFilter(req, 2);
    const whClause = wh.clause ? ` AND ${wh.clause}` : '';

    // ✅ Phase 2: Count records before delete (warehouse-scoped)
    const countResult = await query(
      `SELECT COUNT(*) as count FROM master_data WHERE batch_id = $1 AND deleted_at IS NULL${whClause}`,
      [batchId, ...wh.params]
    );
    const recordCount = parseInt(countResult.rows[0]?.count || '0');

    if (recordCount === 0) {
      return res.status(404).json({ error: 'No active records found for this batch' });
    }

    // ✅ Atomic: Snapshot + Soft-delete in a single transaction
    let snapshotWarehouseId: number | null = null;

    await withTransaction(async (client) => {
      // Step 1: Fetch all records for snapshot (warehouse-scoped)
      const snapshotData = await client.query(
        `SELECT * FROM master_data WHERE batch_id = $1 AND deleted_at IS NULL${whClause}`,
        [batchId, ...wh.params]
      );
      snapshotWarehouseId = snapshotData.rows[0]?.warehouse_id || null;

      // Step 2: Create snapshot (MUST succeed before delete)
      const jsonStr = JSON.stringify(snapshotData.rows);
      await client.query(
        `INSERT INTO batch_snapshots (batch_id, record_count, snapshot_data, snapshot_size_bytes, reason, created_by, warehouse_id)
         VALUES ($1, $2, $3::jsonb, $4, 'pre_delete', $5, $6)`,
        [
          batchId,
          recordCount,
          jsonStr,
          Buffer.byteLength(jsonStr),
          userId,
          snapshotWarehouseId
        ]
      );
      console.log(`📸 Batch snapshot created: ${batchId} (${recordCount} records, warehouse ${snapshotWarehouseId})`);

      // Step 3: Soft delete batch (warehouse-scoped) — only after snapshot succeeds
      await client.query(
        `UPDATE master_data SET deleted_at = NOW(), deleted_by = $2 WHERE batch_id = $1 AND deleted_at IS NULL${whClause}`,
        [batchId, userId, ...wh.params]
      );
    });

    console.log(`✅ Soft-deleted batch: ${batchId} (${recordCount} records)`);

    // Log for CCTV-style tracking (batch-level)
    logChangeSimple('master_data', 'DELETE', {
      oldData: { batch_id: batchId, count: recordCount, warehouse_id: snapshotWarehouseId }
    }, { batchId, userId: userId || undefined, warehouseId: snapshotWarehouseId || undefined }).catch(() => { });

    res.json({ message: 'Deleted', count: recordCount, snapshotCreated: true });
  } catch (error: any) {
    console.error('❌ Batch delete error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

export const getBatches = async (req: Request, res: Response) => {
  try {
    const wh = buildWarehouseFilter(req, 1, 'md');
    const whClause = wh.clause ? ` AND ${wh.clause}` : '';
    const result = await query(
      `SELECT md.batch_id, COUNT(*) as count, MAX(md.created_at) as lastupdated,
              COALESCE(ul.warehouse_name, w.name) as warehouse_name,
              COALESCE(ul.uploaded_by_name, 'Unknown') as uploaded_by_name
       FROM master_data md
       LEFT JOIN LATERAL (
         SELECT w2.name as warehouse_name, COALESCE(u.full_name, u.username) as uploaded_by_name
         FROM upload_logs lo
         LEFT JOIN users u ON lo.uploaded_by = u.id
         LEFT JOIN warehouses w2 ON lo.warehouse_id = w2.id
         WHERE lo.batch_id = md.batch_id
         ORDER BY lo.uploaded_at DESC LIMIT 1
       ) ul ON TRUE
       LEFT JOIN warehouses w ON md.warehouse_id = w.id
       WHERE md.deleted_at IS NULL${whClause}
       GROUP BY md.batch_id, ul.warehouse_name, ul.uploaded_by_name, w.name
       ORDER BY lastupdated DESC LIMIT 500`,
      wh.params
    );

    res.json(result.rows);
  } catch (error: any) {
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== GET BRANDS - unique brands from master_data (with optional category filter) ======
export const getBrands = async (req: Request, res: Response) => {
  try {
    const { category } = req.query;
    let paramIndex = 1;

    const wh = buildWarehouseFilter(req, paramIndex);
    paramIndex = wh.nextIndex;

    let sql = `
      SELECT DISTINCT brand 
      FROM master_data 
      WHERE brand IS NOT NULL AND brand != '' AND deleted_at IS NULL${wh.clause ? ` AND ${wh.clause}` : ''}
    `;
    const params: any[] = [...wh.params];

    // Filter by category if provided
    if (category && category !== '') {
      sql += ` AND cms_vertical = $${paramIndex}`;
      params.push(category);
      paramIndex++;
    }

    sql += ` ORDER BY brand LIMIT 1000`;

    const result = await query(sql, params);
    res.json(result.rows.map((r: any) => r.brand));
  } catch (error: any) {
    console.error('❌ Get master data brands error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== GET CATEGORIES - unique categories from master_data (with optional brand filter) ======
export const getCategories = async (req: Request, res: Response) => {
  try {
    const { brand } = req.query;
    let paramIndex = 1;

    const wh = buildWarehouseFilter(req, paramIndex);
    paramIndex = wh.nextIndex;

    let sql = `
      SELECT DISTINCT cms_vertical 
      FROM master_data 
      WHERE cms_vertical IS NOT NULL AND cms_vertical != '' AND deleted_at IS NULL${wh.clause ? ` AND ${wh.clause}` : ''}
    `;
    const params: any[] = [...wh.params];

    // Filter by brand if provided
    if (brand && brand !== '') {
      sql += ` AND brand = $${paramIndex}`;
      params.push(brand);
      paramIndex++;
    }

    sql += ` ORDER BY cms_vertical LIMIT 1000`;

    const result = await query(sql, params);
    res.json(result.rows.map((r: any) => r.cms_vertical));
  } catch (error: any) {
    console.error('❌ Get master data categories error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

export const exportMasterData = async (req: Request, res: Response) => {
  try {
    const { batchIds, dateFrom, dateTo, search, status, brand, category, batch_id } = req.query;
    let where: string[] = [];
    const params: any[] = [];

    let idx = 1;

    // Warehouse filter
    const wh = buildWarehouseFilter(req, idx, 'md');
    if (wh.clause) {
      where.push(wh.clause);
      params.push(...wh.params);
      idx = wh.nextIndex;
    }

    // Support both batchIds (comma-separated) and single batch_id
    if (batchIds && typeof batchIds === 'string') {
      const batches = batchIds.split(',');
      where.push(`md.batch_id IN (${batches.map(() => `$${idx++}`).join(',')})`);
      params.push(...batches);
    } else if (batch_id && typeof batch_id === 'string') {
      where.push(`md.batch_id = $${idx++}`);
      params.push(batch_id);
    }

    if (dateFrom && dateTo) {
      where.push(`md.created_at >= $${idx++}`);
      where.push(`md.created_at <= $${idx++}`);
      params.push(dateFrom);
      params.push(dateTo);
    }

    // Search filter - search in wsn, wid, fsn, order_id, product_title
    if (search && typeof search === 'string') {
      const searchTerm = `%${search}%`;
      where.push(`(md.wsn ILIKE $${idx} OR md.wid ILIKE $${idx} OR md.fsn ILIKE $${idx} OR md.order_id ILIKE $${idx} OR md.product_title ILIKE $${idx})`);
      params.push(searchTerm);
      idx++;
    }

    // Brand filter
    if (brand && typeof brand === 'string') {
      where.push(`md.brand ILIKE $${idx++}`);
      params.push(`%${brand}%`);
    }

    // Category filter
    if (category && typeof category === 'string') {
      where.push(`md.cms_vertical ILIKE $${idx++}`);
      params.push(`%${category}%`);
    }

    // Status filter - warehouse-scoped subqueries
    let statusWhereClause = '';
    if (status && typeof status === 'string' && status !== 'All') {
      if (status === 'Received') {
        statusWhereClause = ` AND EXISTS(SELECT 1 FROM inbound WHERE inbound.wsn = md.wsn AND inbound.warehouse_id = md.warehouse_id)`;
      } else if (status === 'Receiving') {
        statusWhereClause = ` AND EXISTS(SELECT 1 FROM receiving_wsns WHERE UPPER(receiving_wsns.wsn) = UPPER(md.wsn) AND receiving_wsns.warehouse_id = md.warehouse_id) AND NOT EXISTS(SELECT 1 FROM inbound WHERE inbound.wsn = md.wsn AND inbound.warehouse_id = md.warehouse_id)`;
      } else if (status === 'Rejected') {
        statusWhereClause = ` AND EXISTS(SELECT 1 FROM rejections WHERE UPPER(rejections.wsn) = UPPER(md.wsn) AND rejections.warehouse_id = md.warehouse_id)`;
      } else if (status === 'Pending') {
        statusWhereClause = ` AND NOT EXISTS(SELECT 1 FROM inbound WHERE inbound.wsn = md.wsn AND inbound.warehouse_id = md.warehouse_id) AND NOT EXISTS(SELECT 1 FROM receiving_wsns WHERE UPPER(receiving_wsns.wsn) = UPPER(md.wsn) AND receiving_wsns.warehouse_id = md.warehouse_id) AND NOT EXISTS(SELECT 1 FROM rejections WHERE UPPER(rejections.wsn) = UPPER(md.wsn) AND rejections.warehouse_id = md.warehouse_id)`;
      }
    }

    // Always filter out soft-deleted records
    where.push('md.deleted_at IS NULL');

    const whereClause = `WHERE ${where.join(' AND ')}`;

    // ⚡ actual_received is computed dynamically using CASE (warehouse-scoped)
    const sql = `SELECT md.wsn, md.wid, md.fsn, md.order_id, md.product_title, md.brand, md.mrp, md.fsp, 
                        md.hsn_sac, md.igst_rate, md.cms_vertical, md.fkt_link, md.p_type, md.p_size, 
                        md.vrp, md.yield_value, md.fk_grade, md.fkqc_remark, md.wh_location, md.batch_id, 
                        md.invoice_date, md.created_at,
                        CASE 
                          WHEN EXISTS(SELECT 1 FROM inbound WHERE inbound.wsn = md.wsn AND inbound.warehouse_id = md.warehouse_id) THEN 'Received'
                          WHEN EXISTS(SELECT 1 FROM receiving_wsns WHERE UPPER(receiving_wsns.wsn) = UPPER(md.wsn) AND receiving_wsns.warehouse_id = md.warehouse_id) THEN 'Receiving'
                          WHEN EXISTS(SELECT 1 FROM rejections WHERE UPPER(rejections.wsn) = UPPER(md.wsn) AND rejections.warehouse_id = md.warehouse_id) THEN 'Rejected'
                          ELSE 'Pending'
                        END as actual_received
                 FROM master_data md
                 ${whereClause}
                 ${statusWhereClause}
                 ORDER BY md.created_at DESC LIMIT 100000`;

    const result = await query(sql, params);

    res.json({ data: result.rows, count: result.rows.length });
  } catch (error: any) {
    res.status(500).json({ error: safeError(error) });
  }
};

// ========== PHASE 3: UPLOAD HISTORY API ==========

/**
 * Get paginated upload history from upload_logs
 * Supports filtering by status, date range, and search
 */
export const getUploadHistory = async (req: Request, res: Response) => {
  try {
    const { page = 1, limit = 20, status, dateFrom, dateTo, search } = req.query;
    const offset = ((Number(page) - 1) * Number(limit));
    const conditions: string[] = [];
    const params: any[] = [];
    let paramIndex = 1;

    // Warehouse filter
    const wh = buildWarehouseFilter(req, paramIndex);
    if (wh.clause) {
      conditions.push(wh.clause);
      params.push(...wh.params);
      paramIndex = wh.nextIndex;
    }

    if (status && status !== 'all') {
      conditions.push(`status = $${paramIndex}`);
      params.push(status);
      paramIndex++;
    }

    if (dateFrom) {
      conditions.push(`uploaded_at >= $${paramIndex}`);
      params.push(dateFrom);
      paramIndex++;
    }

    if (dateTo) {
      conditions.push(`uploaded_at <= $${paramIndex}`);
      params.push(dateTo);
      paramIndex++;
    }

    if (search && typeof search === 'string') {
      conditions.push(`(original_filename ILIKE $${paramIndex} OR batch_id ILIKE $${paramIndex} OR job_id ILIKE $${paramIndex})`);
      params.push(`%${search}%`);
      paramIndex++;
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    // Get paginated data
    const countParams = [...params]; // snapshot before adding limit/offset
    params.push(Number(limit));
    params.push(offset);

    const result = await query(
      `SELECT id, job_id, batch_id, original_filename, file_size_bytes, file_type,
              status, total_rows, success_count, duplicate_count, skipped_count,
              error_count, duplicate_strategy, error_message,
              uploaded_by, uploaded_at, completed_at
       FROM upload_logs
       ${whereClause}
       ORDER BY uploaded_at DESC
       LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`,
      params
    );

    const countResult = await query(
      `SELECT COUNT(*) FROM upload_logs ${whereClause}`,
      countParams
    );

    res.json({
      data: result.rows.map((row: any) => ({
        id: row.id,
        jobId: row.job_id,
        batchId: row.batch_id,
        filename: row.original_filename,
        fileSizeBytes: row.file_size_bytes,
        fileType: row.file_type,
        status: row.status,
        totalRows: row.total_rows,
        successCount: row.success_count,
        duplicateCount: row.duplicate_count,
        skippedCount: row.skipped_count,
        errorCount: row.error_count,
        duplicateStrategy: row.duplicate_strategy,
        errorMessage: row.error_message,
        uploadedBy: row.uploaded_by,
        uploadedAt: row.uploaded_at,
        completedAt: row.completed_at
      })),
      total: parseInt(countResult.rows[0].count),
      page: Number(page),
      limit: Number(limit)
    });
  } catch (error: any) {
    console.error('❌ Get upload history error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

/**
 * Delete a single upload history log entry.
 * Only allows deleting completed/failed/cancelled uploads (not active ones).
 */
export const deleteUploadLog = async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    // Warehouse scoping
    const wh = buildWarehouseFilter(req, 2);
    const whClause = wh.clause ? ` AND ${wh.clause}` : '';

    // Only allow deleting non-active upload logs (completed, failed, cancelled)
    const result = await query(
      `DELETE FROM upload_logs WHERE id = $1 AND status IN ('completed', 'failed', 'cancelled')${whClause} RETURNING job_id, original_filename`,
      [id, ...wh.params]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Upload log not found or still in progress' });
    }

    console.log(`🗑️ Deleted upload log: ID=${id}, File=${result.rows[0].original_filename}`);
    res.json({ message: 'Upload log deleted', jobId: result.rows[0].job_id, filename: result.rows[0].original_filename });
  } catch (error: any) {
    console.error('❌ Delete upload log error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

/**
 * Get duplicate WSNs for a specific upload job
 */
export const getUploadDuplicates = async (req: Request, res: Response) => {
  try {
    const { jobId } = req.params;
    const result = await query(
      `SELECT duplicate_wsns, duplicate_count, duplicate_strategy FROM upload_logs WHERE job_id = $1`,
      [jobId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Upload not found' });
    }

    const row = result.rows[0];
    res.json({
      jobId,
      duplicateCount: row.duplicate_count,
      duplicateStrategy: row.duplicate_strategy,
      duplicateWsns: row.duplicate_wsns || []
    });
  } catch (error: any) {
    console.error('❌ Get upload duplicates error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ========== PHASE 5: ADVANCED FEATURES ==========

/**
 * Restore a soft-deleted batch from its snapshot.
 * Re-inserts all records from snapshot_data JSONB, handling WSN conflicts with active records.
 * Marks snapshot as restored afterward.
 */
export const restoreBatch = async (req: Request, res: Response) => {
  try {
    const { batchId } = req.params;
    const userId = req.user?.userId ?? null;

    // Warehouse scoping
    const wh = buildWarehouseFilter(req, 2);
    const whClause = wh.clause ? ` AND ${wh.clause}` : '';

    // Find the most recent non-restored, non-expired snapshot for this batch (warehouse-scoped)
    const snapshotResult = await query(
      `SELECT id, batch_id, record_count, snapshot_data
       FROM batch_snapshots
       WHERE batch_id = $1 AND restored = FALSE AND expires_at > NOW()${whClause}
       ORDER BY created_at DESC LIMIT 1`,
      [batchId, ...wh.params]
    );

    if (snapshotResult.rows.length === 0) {
      return res.status(404).json({ error: 'No restorable snapshot found for this batch. It may have expired or already been restored.' });
    }

    const snapshot = snapshotResult.rows[0];
    const records: any[] = snapshot.snapshot_data;

    if (!Array.isArray(records) || records.length === 0) {
      return res.status(400).json({ error: 'Snapshot contains no records' });
    }

    let restoredCount = 0;
    let conflictCount = 0;
    const conflictWsns: string[] = [];
    const allWsns = records.map((r: any) => r.wsn);

    await withTransaction(async (client) => {
      // Step 1: Bulk un-soft-delete all matching records in one query
      const undeleteResult = await client.query(
        `UPDATE master_data SET deleted_at = NULL, deleted_by = NULL
         WHERE batch_id = $1 AND deleted_at IS NOT NULL AND wsn = ANY($2)
         RETURNING wsn`,
        [batchId, allWsns]
      );
      const undeletedWsns = new Set((undeleteResult.rows || []).map((r: any) => r.wsn));
      restoredCount += undeletedWsns.size;

      // Step 2: Find WSNs that weren't un-soft-deleted (need insert or conflict check)
      const remainingRecords = records.filter((r: any) => !undeletedWsns.has(r.wsn));

      if (remainingRecords.length > 0) {
        const remainingWsns = remainingRecords.map((r: any) => r.wsn);

        // Step 3: Bulk check which remaining WSNs have active conflicts
        const activeResult = await client.query(
          `SELECT wsn FROM master_data WHERE wsn = ANY($1) AND deleted_at IS NULL`,
          [remainingWsns]
        );
        const activeWsnSet = new Set((activeResult.rows || []).map((r: any) => r.wsn));

        // Separate conflicts from insertable records
        const toInsert = remainingRecords.filter((r: any) => !activeWsnSet.has(r.wsn));
        const conflicts = remainingRecords.filter((r: any) => activeWsnSet.has(r.wsn));
        conflictCount = conflicts.length;
        conflicts.slice(0, 100).forEach((r: any) => conflictWsns.push(r.wsn));

        // Step 4: Bulk insert remaining records in chunks of 200
        const CHUNK = 200;
        for (let i = 0; i < toInsert.length; i += CHUNK) {
          const chunk = toInsert.slice(i, i + CHUNK);
          const values: any[] = [];
          const placeholders: string[] = [];
          let idx = 1;

          for (const row of chunk) {
            placeholders.push(`($${idx},$${idx + 1},$${idx + 2},$${idx + 3},$${idx + 4},$${idx + 5},$${idx + 6},$${idx + 7},$${idx + 8},$${idx + 9},$${idx + 10},$${idx + 11},$${idx + 12},$${idx + 13},$${idx + 14},$${idx + 15},$${idx + 16},$${idx + 17},$${idx + 18},$${idx + 19},$${idx + 20},$${idx + 21})`);
            values.push(
              row.wsn, row.wid, row.fsn, row.order_id, row.product_title, row.brand,
              row.mrp, row.fsp, row.hsn_sac, row.igst_rate, row.cms_vertical, row.fkt_link,
              row.p_type, row.p_size, row.vrp, row.yield_value, row.fk_grade, row.fkqc_remark,
              row.wh_location, row.batch_id, row.invoice_date, row.warehouse_id
            );
            idx += 22;
          }

          const insertResult = await client.query(
            `INSERT INTO master_data (wsn, wid, fsn, order_id, product_title, brand, mrp, fsp,
              hsn_sac, igst_rate, cms_vertical, fkt_link, p_type, p_size,
              vrp, yield_value, fk_grade, fkqc_remark, wh_location, batch_id, invoice_date, warehouse_id)
             VALUES ${placeholders.join(', ')}
             ON CONFLICT (wsn, warehouse_id) WHERE deleted_at IS NULL DO NOTHING`,
            values
          );
          restoredCount += insertResult.rowCount || 0;
        }
      }

      // Step 5: Mark snapshot as restored
      await client.query(
        `UPDATE batch_snapshots SET restored = TRUE, restored_at = NOW(), restored_by = $2 WHERE id = $1`,
        [snapshot.id, userId]
      );
    });

    console.log(`✅ Batch restored: ${batchId} — ${restoredCount} records restored, ${conflictCount} conflicts skipped`);
    res.json({
      message: 'Batch restored successfully',
      batchId,
      restoredCount,
      conflictCount,
      conflictWsns: conflictWsns.slice(0, 50),
      snapshotId: snapshot.id
    });
  } catch (error: any) {
    console.error('❌ Restore batch error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

/**
 * List available batch snapshots (non-restored, non-expired).
 * Optionally filter by batchId query param.
 */
export const getSnapshots = async (req: Request, res: Response) => {
  try {
    const { batchId, page = 1, limit = 20, includeRestored } = req.query;
    const offset = (Number(page) - 1) * Number(limit);
    const conditions: string[] = ['expires_at > NOW()'];
    const params: any[] = [];
    let paramIndex = 1;

    // Warehouse filter
    const wh = buildWarehouseFilter(req, paramIndex);
    if (wh.clause) {
      conditions.push(wh.clause.replace('warehouse_id', 'bs.warehouse_id'));
      params.push(...wh.params);
      paramIndex = wh.nextIndex;
    }

    if (!includeRestored || includeRestored === 'false') {
      conditions.push('restored = FALSE');
    }

    if (batchId && typeof batchId === 'string') {
      conditions.push(`batch_id = $${paramIndex}`);
      params.push(batchId);
      paramIndex++;
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const countParams = [...params];

    params.push(Number(limit));
    params.push(offset);

    const result = await query(
      `SELECT bs.id, bs.batch_id, bs.record_count, bs.snapshot_size_bytes, bs.reason,
              bs.created_by, bs.created_at, bs.expires_at, bs.restored, bs.restored_at, bs.restored_by,
              u.username as created_by_name
       FROM batch_snapshots bs
       LEFT JOIN users u ON bs.created_by = u.id
       ${whereClause}
       ORDER BY bs.created_at DESC
       LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`,
      params
    );

    const countResult = await query(
      `SELECT COUNT(*) FROM batch_snapshots bs ${whereClause}`,
      countParams
    );

    res.json({
      data: result.rows.map((row: any) => ({
        id: row.id,
        batchId: row.batch_id,
        recordCount: row.record_count,
        snapshotSizeBytes: row.snapshot_size_bytes,
        reason: row.reason,
        createdBy: row.created_by,
        createdByName: row.created_by_name,
        createdAt: row.created_at,
        expiresAt: row.expires_at,
        restored: row.restored,
        restoredAt: row.restored_at,
        restoredBy: row.restored_by
      })),
      total: parseInt(countResult.rows[0].count),
      page: Number(page),
      limit: Number(limit)
    });
  } catch (error: any) {
    console.error('❌ Get snapshots error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

/**
 * Get soft-deleted records (trash view).
 * Supports pagination and search.
 */
export const getDeletedRecords = async (req: Request, res: Response) => {
  try {
    const { page = 1, limit = 50, search, batchId } = req.query;
    const offset = (Number(page) - 1) * Number(limit);
    const conditions: string[] = ['deleted_at IS NOT NULL'];
    const params: any[] = [];
    let paramIndex = 1;

    // Warehouse filter
    const wh = buildWarehouseFilter(req, paramIndex);
    if (wh.clause) {
      conditions.push(wh.clause);
      params.push(...wh.params);
      paramIndex = wh.nextIndex;
    }

    if (batchId && typeof batchId === 'string') {
      conditions.push(`batch_id = $${paramIndex}`);
      params.push(batchId);
      paramIndex++;
    }

    if (search && typeof search === 'string') {
      conditions.push(`(wsn ILIKE $${paramIndex} OR wid ILIKE $${paramIndex} OR product_title ILIKE $${paramIndex})`);
      params.push(`%${search}%`);
      paramIndex++;
    }

    const whereClause = `WHERE ${conditions.join(' AND ')}`;
    const countParams = [...params];

    params.push(Number(limit));
    params.push(offset);

    const result = await query(
      `SELECT id, wsn, wid, fsn, order_id, product_title, brand, batch_id,
              deleted_at, deleted_by, created_at
       FROM master_data
       ${whereClause}
       ORDER BY deleted_at DESC
       LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`,
      params
    );

    const countResult = await query(
      `SELECT COUNT(*) FROM master_data ${whereClause}`,
      countParams
    );

    res.json({
      data: result.rows,
      total: parseInt(countResult.rows[0].count),
      page: Number(page),
      limit: Number(limit)
    });
  } catch (error: any) {
    console.error('❌ Get deleted records error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

/**
 * Hard-delete (purge) a single soft-deleted record permanently.
 */
export const purgeDeletedRecord = async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    // Warehouse scoping — only allow purging records in accessible warehouses
    const wh = buildWarehouseFilter(req, 2);
    const whClause = wh.clause ? ` AND ${wh.clause}` : '';

    // Only allow purging records that are already soft-deleted (warehouse-scoped)
    const result = await query(
      `DELETE FROM master_data WHERE id = $1 AND deleted_at IS NOT NULL${whClause} RETURNING wsn`,
      [id, ...wh.params]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Soft-deleted record not found' });
    }

    console.log(`🗑️ Permanently purged record: ID=${id}, WSN=${result.rows[0].wsn}`);
    res.json({ message: 'Record permanently deleted', wsn: result.rows[0].wsn });
  } catch (error: any) {
    console.error('❌ Purge record error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

/**
 * Hard-delete ALL soft-deleted records permanently (bulk purge).
 */
export const purgeAllDeletedRecords = async (req: Request, res: Response) => {
  try {
    const wh = buildWarehouseFilter(req, 1);
    const whClause = wh.clause ? ` AND ${wh.clause}` : '';

    const result = await query(
      `DELETE FROM master_data WHERE deleted_at IS NOT NULL${whClause} RETURNING id`,
      wh.params
    );

    const count = result.rowCount || 0;
    console.log(`🗑️ Bulk purged ${count} soft-deleted records`);
    res.json({ message: `Permanently deleted ${count} records`, count });
  } catch (error: any) {
    console.error('❌ Bulk purge error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

/**
 * Cleanup stale data:
 * - Expired snapshots (expires_at < NOW() and not restored)
 * - Completed/failed upload_progress entries older than 7 days
 * - Hard-delete master_data records soft-deleted more than 90 days ago
 */
export const cleanupStaleData = async (req: Request, res: Response) => {
  try {
    const userId = req.user?.userId ?? null;
    const results: Record<string, number> = {};

    // Warehouse scoping
    const wh = buildWarehouseFilter(req, 1);
    const whClause = wh.clause ? ` AND ${wh.clause}` : '';
    const whClauseSnapshot = wh.clause ? ` AND ${wh.clause.replace('warehouse_id', 'warehouse_id')}` : '';

    // 1. Delete expired, non-restored snapshots (warehouse-scoped)
    const expiredSnapshots = await query(
      `DELETE FROM batch_snapshots WHERE expires_at < NOW() AND restored = FALSE${whClauseSnapshot} RETURNING id`,
      [...wh.params]
    );
    results.expiredSnapshots = expiredSnapshots.rowCount || 0;

    // 2. Clean up old upload_progress entries (completed/failed, older than 7 days)
    // upload_progress doesn't have warehouse_id — safe to clean globally
    const oldProgress = await query(
      `DELETE FROM upload_progress
       WHERE (status = 'completed' OR status = 'failed' OR status = 'cancelled')
         AND updated_at < NOW() - INTERVAL '7 days'
       RETURNING job_id`
    );
    results.oldProgressEntries = oldProgress.rowCount || 0;

    // 3. Hard-delete master_data records soft-deleted 90+ days ago (warehouse-scoped)
    const purgedRecords = await query(
      `DELETE FROM master_data WHERE deleted_at IS NOT NULL AND deleted_at < NOW() - INTERVAL '90 days'${whClause} RETURNING id`,
      [...wh.params]
    );
    results.purgedRecords = purgedRecords.rowCount || 0;

    console.log(`🧹 Cleanup complete: ${JSON.stringify(results)} (by user ${userId})`);
    res.json({ message: 'Cleanup complete', results });
  } catch (error: any) {
    console.error('❌ Cleanup error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};