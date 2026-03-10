// File Path = warehouse-backend/src/controllers/rejection.controller.ts
import { Request, Response } from 'express';
import { query, getPool } from '../config/database';
import { generateBatchId } from '../utils/helpers';
import ExcelJS from 'exceljs';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { safeError } from '../utils/sanitizeError';
import { logChanges, logChangeSimple } from '../utils/changeLogger';
import { backupScheduler } from '../services/backupScheduler';

// Multer setup for file uploads
const upload = multer({
    dest: path.join(__dirname, '../../uploads/rejections'),
    limits: { fileSize: 10 * 1024 * 1024 }, // 10MB limit
});

export const uploadMiddleware = upload.single('file');

// Valid rejection types
const VALID_REJECTION_TYPES = ['damaged', 'fraud', 'short', 'other'];

// Format date as DD-MMM-YYYY (e.g. 18-Feb-2026)
const formatDateDMY = (dateVal: any): string => {
    if (!dateVal) return '';
    const d = new Date(dateVal);
    if (isNaN(d.getTime())) return '';
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const day = String(d.getDate()).padStart(2, '0');
    return `${day}-${months[d.getMonth()]}-${d.getFullYear()}`;
};

/**
 * Upload rejection Excel file
 * POST /api/rejections/upload
 */
export const uploadRejections = async (req: Request, res: Response) => {
    const pool = getPool();
    const client = await pool.connect();

    try {
        const file = req.file;
        if (!file) {
            return res.status(400).json({ error: 'No file uploaded' });
        }

        const { warehouse_id, rejection_date, rejected_by_person } = req.body;
        if (!warehouse_id) {
            return res.status(400).json({ error: 'Warehouse ID is required' });
        }

        const userId = (req as any).user?.userId;
        const userName = (req as any).user?.full_name ||
            (req as any).user?.username ||
            'Unknown';

        if (!userId) {
            return res.status(401).json({ error: 'User authentication failed. Please re-login.' });
        }

        // Read Excel file
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.readFile(file.path);
        const worksheet = workbook.worksheets[0];

        if (!worksheet) {
            fs.unlinkSync(file.path);
            return res.status(400).json({ error: 'No worksheet found in Excel file' });
        }

        // Get header row
        const headerRow = worksheet.getRow(1);
        const headers: { [key: string]: number } = {};
        headerRow.eachCell((cell, colNumber) => {
            const value = cell.value?.toString().trim().toLowerCase().replace(/[^a-z0-9_]/g, '_');
            if (value) headers[value] = colNumber;
        });

        // Validate required columns
        const wsnCol = headers['wsn'];
        const typeCol = headers['rejection_type'] || headers['type'] || headers['reason'];
        const remarkCol = headers['remarks'] || headers['remark'] || headers['notes'];

        if (!wsnCol) {
            fs.unlinkSync(file.path);
            return res.status(400).json({ error: 'WSN column not found. Required columns: WSN, Rejection Type' });
        }

        if (!typeCol) {
            fs.unlinkSync(file.path);
            return res.status(400).json({ error: 'Rejection Type column not found. Required columns: WSN, Rejection Type' });
        }

        // Person and date come from dialog (req.body), not from Excel
        const uploadPerson = rejected_by_person ? rejected_by_person.trim() : null;
        const uploadDate = rejection_date ? new Date(rejection_date) : new Date();

        // Generate batch ID for this upload
        const batchId = generateBatchId('REJ');

        // Collect all rows
        const rows: any[] = [];
        const wsns: string[] = [];
        const errors: string[] = [];

        worksheet.eachRow((row, rowNumber) => {
            if (rowNumber === 1) return; // Skip header

            const wsn = row.getCell(wsnCol).value?.toString().trim().toUpperCase();
            let rejectionType = row.getCell(typeCol).value?.toString().trim().toLowerCase();
            const remarks = remarkCol ? row.getCell(remarkCol).value?.toString().trim() : null;

            if (!wsn) return; // Skip empty rows

            // Validate and normalize rejection type
            if (!rejectionType) {
                errors.push(`Row ${rowNumber}: Rejection type is required for WSN ${wsn}`);
                return;
            }

            // Strict validation — reject unknown types instead of auto-converting
            if (!VALID_REJECTION_TYPES.includes(rejectionType)) {
                errors.push(`Row ${rowNumber}: Invalid rejection type "${rejectionType}" for WSN ${wsn}. Allowed: ${VALID_REJECTION_TYPES.join(', ')}`);
                return;
            }

            rows.push({
                wsn,
                rejectionType,
                remarks,
                rejectedByPerson: uploadPerson,
                rejectionDate: uploadDate,
                rowNumber
            });
            wsns.push(wsn);
        });

        if (rows.length === 0) {
            fs.unlinkSync(file.path);
            return res.status(400).json({ error: 'No valid data rows found in Excel file' });
        }

        // Begin transaction early so existence checks + inserts are atomic
        await client.query('BEGIN');

        // Check which WSNs exist in master_data
        const existingMasterData = await client.query(
            `SELECT wsn, batch_id FROM master_data WHERE UPPER(wsn) = ANY($1) AND deleted_at IS NULL`,
            [wsns]
        );
        const masterDataMap = new Map(existingMasterData.rows.map((r: any) => [r.wsn.toUpperCase(), r.batch_id]));

        // Check which WSNs are already rejected (only active, not soft-deleted)
        const existingRejections = await client.query(
            `SELECT wsn FROM rejections WHERE UPPER(wsn) = ANY($1) AND deleted_at IS NULL`,
            [wsns]
        );
        const alreadyRejected = new Set(existingRejections.rows.map((r: any) => r.wsn.toUpperCase()));

        // Check which WSNs are already in inbound (received)
        const existingInbound = await client.query(
            `SELECT wsn FROM inbound WHERE UPPER(wsn) = ANY($1)`,
            [wsns]
        );
        const alreadyReceived = new Set(existingInbound.rows.map((r: any) => r.wsn.toUpperCase()));

        // Filter valid rows
        const validRows: any[] = [];
        const skipped: { wsn: string; reason: string }[] = [];

        for (const row of rows) {
            if (!masterDataMap.has(row.wsn)) {
                skipped.push({ wsn: row.wsn, reason: 'WSN not found in master data' });
                continue;
            }
            if (alreadyRejected.has(row.wsn)) {
                skipped.push({ wsn: row.wsn, reason: 'Already rejected' });
                continue;
            }
            if (alreadyReceived.has(row.wsn)) {
                skipped.push({ wsn: row.wsn, reason: 'Already received in inbound' });
                continue;
            }
            validRows.push({
                ...row,
                sourceBatchId: masterDataMap.get(row.wsn)
            });
        }

        if (validRows.length === 0) {
            await client.query('ROLLBACK');
            fs.unlinkSync(file.path);
            return res.status(400).json({
                error: 'No valid WSNs to reject',
                skipped,
                errors
            });
        }

        // Bulk insert inside the already-open transaction
        // Build bulk insert values - much faster than individual inserts
        const BATCH_SIZE = 500; // Insert 500 rows at a time
        let insertedCount = 0;

        for (let i = 0; i < validRows.length; i += BATCH_SIZE) {
            const batch = validRows.slice(i, i + BATCH_SIZE);
            const values: any[] = [];
            const placeholders: string[] = [];

            batch.forEach((row, idx) => {
                const offset = idx * 10;
                placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10})`);
                values.push(
                    row.wsn,
                    row.rejectionType,
                    row.remarks,
                    row.rejectedByPerson,
                    batchId,
                    row.sourceBatchId,
                    warehouse_id,
                    row.rejectionDate,
                    userId,
                    userName
                );
            });

            try {
                const result = await client.query(
                    `INSERT INTO rejections (
                        wsn, rejection_type, remarks, rejected_by_person,
                        batch_id, source_batch_id, warehouse_id, rejection_date,
                        uploaded_by, uploaded_by_name
                    ) VALUES ${placeholders.join(', ')}
                    ON CONFLICT (wsn) WHERE deleted_at IS NULL DO NOTHING
                    RETURNING id`,
                    values
                );
                insertedCount += result.rowCount || 0;
            } catch (err: any) {
                console.error('Batch insert error:', err.message);
                throw err;
            }
        }

        await client.query('COMMIT');

        // Log uploaded rejections for CCTV-style tracking (async, non-blocking)
        if (insertedCount > 0) {
            Promise.resolve().then(async () => {
                for (const row of validRows.slice(0, insertedCount)) {
                    await logChangeSimple('rejections', 'INSERT', {
                        wsn: row.wsn, newData: { wsn: row.wsn, rejection_type: row.rejectionType, batch_id: batchId }
                    }, { batchId, userId, userName, warehouseId: Number(warehouse_id) });
                }
            }).catch(() => { });
        }

        // Clean up uploaded file
        fs.unlinkSync(file.path);

        // Log upload to rejection_upload_logs (non-blocking, outside transaction)
        try {
            await query(
                `INSERT INTO rejection_upload_logs 
                    (batch_id, original_filename, file_size_bytes, total_rows, success_count, skipped_count, error_count, status, skipped_details, error_details, uploaded_by, uploaded_by_name, warehouse_id, uploaded_at, completed_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW())`,
                [batchId, file.originalname || 'unknown', file.size || 0, rows.length, insertedCount, skipped.length, errors.length, 'completed', JSON.stringify(skipped), JSON.stringify(errors), userId, userName, warehouse_id]
            );
        } catch (logErr) {
            console.log('⚠️ Failed to log upload history (non-critical):', logErr);
        }

        // Trigger event backup (fire-and-forget)
        backupScheduler.triggerEventBackup(`rejection upload: ${insertedCount} entries`).catch(() => {});

        res.json({
            success: true,
            message: `Successfully uploaded ${insertedCount} rejections`,
            batch_id: batchId,
            inserted: insertedCount,
            skipped: skipped.length,
            skippedDetails: skipped.slice(0, 20), // Limit details to first 20
            errors: errors.slice(0, 10)
        });

    } catch (error: any) {
        await client.query('ROLLBACK');
        console.error('❌ Upload rejections error:', error);

        // Log failed upload to rejection_upload_logs (non-blocking)
        try {
            const userId = (req as any).user?.userId;
            const userName = (req as any).user?.full_name || (req as any).user?.username || 'Unknown';
            await query(
                `INSERT INTO rejection_upload_logs 
                    (batch_id, original_filename, file_size_bytes, total_rows, success_count, skipped_count, error_count, status, error_message, skipped_details, error_details, uploaded_by, uploaded_by_name, warehouse_id, uploaded_at, completed_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW())`,
                ['FAILED', req.file?.originalname || 'unknown', req.file?.size || 0, 0, 0, 0, 0, 'failed', safeError(error), '[]', '[]', userId, userName, req.body?.warehouse_id || null]
            );
        } catch (logErr) {
            console.log('⚠️ Failed to log upload error (non-critical):', logErr);
        }

        // Clean up file if exists
        if (req.file?.path && fs.existsSync(req.file.path)) {
            fs.unlinkSync(req.file.path);
        }

        res.status(500).json({ error: safeError(error) });
    } finally {
        client.release();
    }
};

/**
 * Download rejection template
 * GET /api/rejections/template
 */
export const downloadTemplate = async (req: Request, res: Response) => {
    try {
        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet('Rejections');

        // Only 3 columns — Rejected By & Date are selected via dialog dropdowns
        worksheet.columns = [
            { header: 'WSN', key: 'wsn', width: 20 },
            { header: 'Rejection Type', key: 'rejection_type', width: 18 },
            { header: 'Remarks', key: 'remarks', width: 40 },
        ];

        // Style header row
        const headerRow = worksheet.getRow(1);
        headerRow.font = { bold: true };
        headerRow.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: 'FFE0E0E0' }
        };

        // Add sample data
        worksheet.addRow({ wsn: 'ABC123_X', rejection_type: 'damaged', remarks: 'Screen broken' });
        worksheet.addRow({ wsn: 'DEF456_Y', rejection_type: 'fraud', remarks: 'Wrong model' });
        worksheet.addRow({ wsn: 'GHI789_Z', rejection_type: 'short', remarks: 'Accessories missing' });

        // Add Excel Data Validation dropdown for Rejection Type column (rows 2-1000)
        const typeFormula = '"damaged,fraud,short,other"';
        for (let row = 2; row <= 1000; row++) {
            worksheet.getCell(`B${row}`).dataValidation = {
                type: 'list',
                allowBlank: false,
                formulae: [typeFormula],
                showErrorMessage: true,
                errorStyle: 'stop' as any,
                errorTitle: 'Invalid Type',
                error: 'Please select from: damaged, fraud, short, other',
            };
        }

        // Add notes
        worksheet.addRow([]);
        worksheet.addRow(['Note: Rejected By person and Rejection Date are selected in the upload dialog, not in this template.']);
        worksheet.getRow(worksheet.rowCount).font = { italic: true, color: { argb: 'FF666666' } };

        // Set response headers
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition', 'attachment; filename=Rejection_Template.xlsx');

        // Write to response
        await workbook.xlsx.write(res);
        res.end();

    } catch (error: any) {
        console.error('❌ Download template error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Get all rejections with filters
 * GET /api/rejections
 */
export const getRejections = async (req: Request, res: Response) => {
    try {
        const {
            page = 1,
            limit = 50,
            search,
            rejection_type,
            rejected_by_person,
            batch_id,
            warehouse_id,
            cn_status, // 'pending' | 'received' | 'all'
            source_batch_id
        } = req.query;

        const offset = (Number(page) - 1) * Number(limit);

        let whereClause = 'r.deleted_at IS NULL';
        const params: any[] = [];
        let paramIndex = 1;

        if (search) {
            whereClause += ` AND (r.wsn ILIKE $${paramIndex} OR m.product_title ILIKE $${paramIndex} OR m.brand ILIKE $${paramIndex})`;
            params.push(`%${search}%`);
            paramIndex++;
        }

        if (rejection_type && rejection_type !== 'all') {
            whereClause += ` AND r.rejection_type = $${paramIndex}`;
            params.push(rejection_type);
            paramIndex++;
        }

        if (rejected_by_person) {
            whereClause += ` AND r.rejected_by_person ILIKE $${paramIndex}`;
            params.push(`%${rejected_by_person}%`);
            paramIndex++;
        }

        if (batch_id) {
            whereClause += ` AND r.batch_id = $${paramIndex}`;
            params.push(batch_id);
            paramIndex++;
        }

        if (source_batch_id) {
            whereClause += ` AND r.source_batch_id = $${paramIndex}`;
            params.push(source_batch_id);
            paramIndex++;
        }

        if (warehouse_id) {
            whereClause += ` AND r.warehouse_id = $${paramIndex}`;
            params.push(warehouse_id);
            paramIndex++;
        }

        if (cn_status === 'pending') {
            whereClause += ` AND r.credit_note_no IS NULL`;
        } else if (cn_status === 'received') {
            whereClause += ` AND r.credit_note_no IS NOT NULL`;
        }

        // Data query with master_data join
        const dataQuery = `
      SELECT 
        r.*,
        m.product_title,
        m.brand,
        m.vrp,
        m.fsp,
        m.mrp,
        m.yield_value,
        m.cms_vertical,
        m.fsn
      FROM rejections r
      LEFT JOIN master_data m ON UPPER(r.wsn) = UPPER(m.wsn) AND m.deleted_at IS NULL
      WHERE ${whereClause}
      ORDER BY r.created_at DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
        params.push(Number(limit), offset);

        const result = await query(dataQuery, params);

        // Count query
        const countParams = params.slice(0, -2);
        const countResult = await query(
            `SELECT COUNT(*) FROM rejections r
       LEFT JOIN master_data m ON UPPER(r.wsn) = UPPER(m.wsn) AND m.deleted_at IS NULL
       WHERE ${whereClause}`,
            countParams
        );

        // Summary query
        const summaryResult = await query(
            `SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN credit_note_no IS NULL THEN 1 END) as cn_pending,
        COUNT(CASE WHEN credit_note_no IS NOT NULL THEN 1 END) as cn_received
       FROM rejections r
       WHERE r.deleted_at IS NULL ${warehouse_id ? 'AND r.warehouse_id = $1' : ''}`,
            warehouse_id ? [warehouse_id] : []
        );

        res.json({
            data: result.rows,
            total: parseInt(countResult.rows[0].count),
            page: Number(page),
            limit: Number(limit),
            summary: summaryResult.rows[0]
        });

    } catch (error: any) {
        console.error('❌ Get rejections error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Get rejection summary by batch
 * GET /api/rejections/summary
 */
export const getRejectionSummary = async (req: Request, res: Response) => {
    try {
        const { warehouse_id } = req.query;

        let whereClause = '1=1';
        const params: any[] = [];

        if (warehouse_id) {
            whereClause += ' AND r.warehouse_id = $1';
            params.push(warehouse_id);
        }

        // Only show active (non-deleted) rejections
        whereClause += ' AND r.deleted_at IS NULL';

        const result = await query(
            `SELECT 
        r.batch_id,
        r.rejection_date,
        COUNT(*) as item_count,
        SUM(COALESCE(NULLIF(m.fsp, '')::numeric, 0)) as total_fsp,
        SUM(COALESCE(NULLIF(m.mrp, '')::numeric, 0)) as total_mrp,
        COUNT(CASE WHEN r.credit_note_no IS NULL THEN 1 END) as cn_pending,
        COUNT(CASE WHEN r.credit_note_no IS NOT NULL THEN 1 END) as cn_received,
        MIN(r.created_at) as upload_date,
        r.uploaded_by_name
       FROM rejections r
       LEFT JOIN master_data m ON UPPER(r.wsn) = UPPER(m.wsn) AND m.deleted_at IS NULL
       WHERE ${whereClause}
       GROUP BY r.batch_id, r.rejection_date, r.uploaded_by_name
       ORDER BY MIN(r.created_at) DESC`,
            params
        );

        res.json({ data: result.rows });

    } catch (error: any) {
        console.error('❌ Get rejection summary error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Update credit note info for rejections
 * PUT /api/rejections/credit-note
 */
export const updateCreditNote = async (req: Request, res: Response) => {
    try {
        const { batch_id, credit_note_no, credit_note_date, credit_note_amount, rejection_ids } = req.body;

        if (!credit_note_no) {
            return res.status(400).json({ error: 'Credit note number is required' });
        }

        let result;
        if (batch_id) {
            // Update all rejections in a batch (only active)
            result = await query(
                `UPDATE rejections 
         SET credit_note_no = $1, credit_note_date = $2, credit_note_amount = $3
         WHERE batch_id = $4 AND deleted_at IS NULL
         RETURNING id`,
                [credit_note_no, credit_note_date || new Date(), credit_note_amount, batch_id]
            );
        } else if (rejection_ids && rejection_ids.length > 0) {
            // Update specific rejection IDs (only active)
            result = await query(
                `UPDATE rejections 
         SET credit_note_no = $1, credit_note_date = $2, credit_note_amount = $3
         WHERE id = ANY($4) AND deleted_at IS NULL
         RETURNING id`,
                [credit_note_no, credit_note_date || new Date(), credit_note_amount, rejection_ids]
            );
        } else {
            return res.status(400).json({ error: 'Either batch_id or rejection_ids is required' });
        }

        res.json({
            success: true,
            message: `Updated ${result.rows.length} rejections with credit note`,
            updated: result.rows.length
        });

    } catch (error: any) {
        console.error('❌ Update credit note error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Export rejections to Excel
 * GET /api/rejections/export
 */
export const exportRejections = async (req: Request, res: Response) => {
    try {
        const { batch_id, warehouse_id, cn_status } = req.query;

        let whereClause = '1=1';
        const params: any[] = [];
        let paramIndex = 1;

        if (batch_id) {
            whereClause += ` AND r.batch_id = $${paramIndex}`;
            params.push(batch_id);
            paramIndex++;
        }

        if (warehouse_id) {
            whereClause += ` AND r.warehouse_id = $${paramIndex}`;
            params.push(warehouse_id);
            paramIndex++;
        }

        if (cn_status === 'pending') {
            whereClause += ` AND r.credit_note_no IS NULL`;
        } else if (cn_status === 'received') {
            whereClause += ` AND r.credit_note_no IS NOT NULL`;
        }

        // Only export active (non-deleted) rejections
        whereClause += ' AND r.deleted_at IS NULL';

        const result = await query(
            `SELECT 
        r.wsn,
        m.fsn,
        m.product_title,
        m.brand,
        m.cms_vertical,
        m.vrp,
        m.fsp,
        m.mrp,
        m.yield_value,
        r.rejection_type,
        r.rejected_by_person,
        r.remarks,
        r.rejection_date,
        r.batch_id,
        r.source_batch_id,
        r.uploaded_by_name,
        r.created_at,
        r.credit_note_no,
        r.credit_note_date,
        r.credit_note_amount
       FROM rejections r
       LEFT JOIN master_data m ON UPPER(r.wsn) = UPPER(m.wsn) AND m.deleted_at IS NULL
       WHERE ${whereClause}
       ORDER BY r.created_at DESC
       LIMIT 50000`,
            params
        );

        // Create workbook
        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet('Rejections');

        // Add headers
        worksheet.columns = [
            { header: 'WSN', key: 'wsn', width: 18 },
            { header: 'FSN', key: 'fsn', width: 15 },
            { header: 'Product Title', key: 'product_title', width: 40 },
            { header: 'Brand', key: 'brand', width: 20 },
            { header: 'Category', key: 'cms_vertical', width: 18 },
            { header: 'Order ID', key: 'vrp', width: 12 },
            { header: 'FSP', key: 'fsp', width: 12 },
            { header: 'MRP', key: 'mrp', width: 12 },
            { header: 'Yield', key: 'yield_value', width: 12 },
            { header: 'Rejection Type', key: 'rejection_type', width: 15 },
            { header: 'Rejected By', key: 'rejected_by_person', width: 20 },
            { header: 'Remarks', key: 'remarks', width: 30 },
            { header: 'Rejection Date', key: 'rejection_date', width: 15 },
            { header: 'Batch ID', key: 'batch_id', width: 20 },
            { header: 'Source Batch', key: 'source_batch_id', width: 20 },
            { header: 'Uploaded By', key: 'uploaded_by_name', width: 18 },
            { header: 'Upload Date', key: 'created_at', width: 18 },
            { header: 'CN Number', key: 'credit_note_no', width: 15 },
            { header: 'CN Date', key: 'credit_note_date', width: 15 },
            { header: 'CN Amount', key: 'credit_note_amount', width: 12 },
        ];

        // Style header
        const headerRow = worksheet.getRow(1);
        headerRow.font = { bold: true };
        headerRow.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: 'FF4472C4' }
        };
        headerRow.font = { bold: true, color: { argb: 'FFFFFFFF' } };

        // Add data
        result.rows.forEach((row: any) => {
            worksheet.addRow({
                ...row,
                rejection_date: formatDateDMY(row.rejection_date),
                created_at: formatDateDMY(row.created_at),
                credit_note_date: formatDateDMY(row.credit_note_date)
            });
        });

        // Add summary row
        worksheet.addRow([]);
        const totalFSP = result.rows.reduce((sum: number, r: any) => sum + (parseFloat(r.fsp) || 0), 0);
        const totalMRP = result.rows.reduce((sum: number, r: any) => sum + (parseFloat(r.mrp) || 0), 0);
        const totalYield = result.rows.reduce((sum: number, r: any) => sum + (parseFloat(r.yield_value) || 0), 0);
        worksheet.addRow(['Total Items:', result.rows.length, '', '', '', '', 'Total FSP:', totalFSP, 'Total MRP:', totalMRP, 'Total Yield:', totalYield]);
        worksheet.getRow(worksheet.rowCount).font = { bold: true };

        // Set response headers
        const filename = `Rejections_${new Date().toISOString().split('T')[0]}.xlsx`;
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition', `attachment; filename=${filename}`);

        await workbook.xlsx.write(res);
        res.end();

    } catch (error: any) {
        console.error('❌ Export rejections error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Delete rejection
 * DELETE /api/rejections/:id
 */
export const deleteRejection = async (req: Request, res: Response) => {
    try {
        const { id } = req.params;
        const userName = (req as any).user?.full_name || (req as any).user?.username || 'Unknown';

        const result = await query(
            `UPDATE rejections SET deleted_at = NOW(), deleted_by_name = $2
             WHERE id = $1 AND deleted_at IS NULL RETURNING wsn, id, warehouse_id`,
            [id, userName]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Rejection not found' });
        }

        // Log deletion for CCTV-style tracking
        const userId = (req as any).user?.id || (req as any).user?.userId;
        logChangeSimple('rejections', 'DELETE', {
            id: result.rows[0].id, wsn: result.rows[0].wsn, oldData: result.rows[0]
        }, { userId, userName, warehouseId: result.rows[0].warehouse_id }).catch(() => { });

        res.json({
            success: true,
            message: `Deleted rejection for WSN: ${result.rows[0].wsn}`
        });

    } catch (error: any) {
        console.error('❌ Delete rejection error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Delete rejection batch
 * DELETE /api/rejections/batch/:batchId
 */
export const deleteRejectionBatch = async (req: Request, res: Response) => {
    try {
        const { batchId } = req.params;
        const userName = (req as any).user?.full_name || (req as any).user?.username || 'Unknown';

        const result = await query(
            `UPDATE rejections SET deleted_at = NOW(), deleted_by_name = $2
             WHERE batch_id = $1 AND deleted_at IS NULL RETURNING id, wsn, warehouse_id`,
            [batchId, userName]
        );

        // Log batch deletion for CCTV-style tracking
        if (result.rows.length > 0) {
            const userId = (req as any).user?.id || (req as any).user?.userId;
            const warehouseId = result.rows[0]?.warehouse_id;
            Promise.resolve().then(async () => {
                for (const row of result.rows) {
                    await logChangeSimple('rejections', 'DELETE', {
                        id: row.id, wsn: row.wsn, oldData: row
                    }, { batchId, userId, userName, warehouseId });
                }
            }).catch(() => { });
        }

        res.json({
            success: true,
            message: `Deleted ${result.rows.length} rejections from batch (can be restored)`,
            deleted: result.rows.length
        });

    } catch (error: any) {
        console.error('❌ Delete rejection batch error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Get unique rejected by persons for filter dropdown
 * GET /api/rejections/persons
 */
export const getRejectedByPersons = async (req: Request, res: Response) => {
    try {
        const { warehouse_id } = req.query;

        let whereClause = 'rejected_by_person IS NOT NULL AND deleted_at IS NULL';
        const params: any[] = [];

        if (warehouse_id) {
            whereClause += ' AND warehouse_id = $1';
            params.push(warehouse_id);
        }

        const result = await query(
            `SELECT DISTINCT rejected_by_person 
       FROM rejections 
       WHERE ${whereClause}
       ORDER BY rejected_by_person`,
            params
        );

        res.json({
            persons: result.rows.map((r: any) => r.rejected_by_person)
        });

    } catch (error: any) {
        console.error('❌ Get rejected by persons error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Get upload batches for filter dropdown
 * GET /api/rejections/batches
 */
export const getRejectionBatches = async (req: Request, res: Response) => {
    try {
        const { warehouse_id } = req.query;

        let whereClause = 'r.deleted_at IS NULL';
        const params: any[] = [];

        if (warehouse_id) {
            whereClause += ' AND r.warehouse_id = $1';
            params.push(warehouse_id);
        }

        const result = await query(
            `SELECT 
        r.batch_id,
        COUNT(*) as count,
        MIN(r.created_at) as created_at,
        MAX(r.created_at) as last_updated,
        r.uploaded_by_name,
        STRING_AGG(DISTINCT w.name, ', ') as warehouse_names
       FROM rejections r
       LEFT JOIN warehouses w ON r.warehouse_id = w.id
       WHERE ${whereClause}
       GROUP BY r.batch_id, r.uploaded_by_name
       ORDER BY MAX(r.created_at) DESC
       LIMIT 50`,
            params
        );

        res.json({
            batches: result.rows
        });

    } catch (error: any) {
        console.error('❌ Get rejection batches error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Rename a rejection batch
 * PUT /api/rejections/batch/:batchId/rename
 */
export const renameBatch = async (req: Request, res: Response) => {
    const client = await getPool().connect();
    try {
        const { batchId } = req.params;
        const { newBatchId } = req.body;

        if (!newBatchId || !newBatchId.trim()) {
            client.release();
            return res.status(400).json({ error: 'New batch ID is required' });
        }

        await client.query('BEGIN');

        // Check if new batch ID already exists among active records
        const existingCheck = await client.query(
            'SELECT batch_id FROM rejections WHERE batch_id = $1 AND deleted_at IS NULL LIMIT 1',
            [newBatchId.trim()]
        );

        if (existingCheck.rows.length > 0) {
            await client.query('ROLLBACK');
            client.release();
            return res.status(400).json({ error: 'Batch ID already exists' });
        }

        // Rename batch (only active records)
        const result = await client.query(
            'UPDATE rejections SET batch_id = $1 WHERE batch_id = $2 AND deleted_at IS NULL RETURNING id',
            [newBatchId.trim(), batchId]
        );

        if (result.rows.length === 0) {
            await client.query('ROLLBACK');
            client.release();
            return res.status(404).json({ error: 'Batch not found' });
        }

        await client.query('COMMIT');

        res.json({
            success: true,
            message: `Renamed batch to ${newBatchId}`,
            updated: result.rows.length
        });

    } catch (error: any) {
        await client.query('ROLLBACK').catch(() => { });
        console.error('❌ Rename batch error:', error);
        res.status(500).json({ error: safeError(error) });
    } finally {
        client.release();
    }
};

/**
 * Get upload history for rejections
 * GET /api/rejections/upload-history
 */
export const getUploadHistory = async (req: Request, res: Response) => {
    try {
        const { warehouse_id, page = '1', limit = '20', status, search } = req.query;

        const pageNum = parseInt(page as string) || 1;
        const limitNum = Math.min(parseInt(limit as string) || 20, 100);
        const offset = (pageNum - 1) * limitNum;

        let whereClause = '1=1';
        const params: any[] = [];
        let paramIndex = 1;

        if (warehouse_id) {
            whereClause += ` AND warehouse_id = $${paramIndex}`;
            params.push(warehouse_id);
            paramIndex++;
        }

        if (status && status !== 'all') {
            whereClause += ` AND status = $${paramIndex}`;
            params.push(status);
            paramIndex++;
        }

        if (search) {
            whereClause += ` AND (original_filename ILIKE $${paramIndex} OR batch_id ILIKE $${paramIndex} OR uploaded_by_name ILIKE $${paramIndex})`;
            params.push(`%${search}%`);
            paramIndex++;
        }

        const countResult = await query(
            `SELECT COUNT(*) FROM rejection_upload_logs WHERE ${whereClause}`,
            params
        );

        const dataResult = await query(
            `SELECT * FROM rejection_upload_logs WHERE ${whereClause} ORDER BY uploaded_at DESC LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`,
            [...params, limitNum, offset]
        );

        res.json({
            data: dataResult.rows,
            total: parseInt(countResult.rows[0].count),
            page: pageNum,
            limit: limitNum
        });

    } catch (error: any) {
        console.error('❌ Get upload history error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Delete upload history log entry
 * DELETE /api/rejections/upload-history/:id
 */
export const deleteUploadLog = async (req: Request, res: Response) => {
    try {
        const { id } = req.params;

        const result = await query(
            'DELETE FROM rejection_upload_logs WHERE id = $1 RETURNING id',
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Upload log not found' });
        }

        res.json({
            success: true,
            message: 'Upload log deleted'
        });

    } catch (error: any) {
        console.error('❌ Delete upload log error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ===================== MANAGED PERSONS =====================

/**
 * Get managed persons list for a warehouse
 * GET /api/rejections/managed-persons
 */
export const getManagedPersons = async (req: Request, res: Response) => {
    try {
        const { warehouse_id } = req.query;

        let whereClause = '1=1';
        const params: any[] = [];

        if (warehouse_id) {
            whereClause += ' AND warehouse_id = $1';
            params.push(warehouse_id);
        }

        const result = await query(
            `SELECT id, name, warehouse_id, created_at, created_by
             FROM rejection_persons
             WHERE ${whereClause}
             ORDER BY name ASC`,
            params
        );

        res.json({ persons: result.rows });
    } catch (error: any) {
        console.error('❌ Get managed persons error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Add a new managed person
 * POST /api/rejections/managed-persons
 */
export const addManagedPerson = async (req: Request, res: Response) => {
    try {
        const { name, warehouse_id } = req.body;

        if (!name || !name.trim()) {
            return res.status(400).json({ error: 'Person name is required' });
        }

        if (!warehouse_id) {
            return res.status(400).json({ error: 'Warehouse ID is required' });
        }

        // Title Case the name for consistency
        const titleCaseName = name.trim()
            .split(/\s+/)
            .map((w: string) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
            .join(' ');

        const userName = (req as any).user?.full_name || (req as any).user?.username || 'Unknown';

        const result = await query(
            `INSERT INTO rejection_persons (name, warehouse_id, created_by)
             VALUES ($1, $2, $3)
             ON CONFLICT (name, warehouse_id) DO NOTHING
             RETURNING id, name`,
            [titleCaseName, warehouse_id, userName]
        );

        if (result.rows.length === 0) {
            return res.status(409).json({ error: `Person "${titleCaseName}" already exists for this warehouse` });
        }

        res.json({
            success: true,
            person: result.rows[0],
            message: `Added "${titleCaseName}" to managed persons`
        });
    } catch (error: any) {
        console.error('❌ Add managed person error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Delete a managed person
 * DELETE /api/rejections/managed-persons/:id
 */
export const deleteManagedPerson = async (req: Request, res: Response) => {
    try {
        const { id } = req.params;

        const result = await query(
            'DELETE FROM rejection_persons WHERE id = $1 RETURNING name',
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Person not found' });
        }

        res.json({
            success: true,
            message: `Removed "${result.rows[0].name}" from managed persons`
        });
    } catch (error: any) {
        console.error('❌ Delete managed person error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ===================== SOFT DELETE RECOVERY =====================

/**
 * Get deleted (soft-deleted) batches
 * GET /api/rejections/deleted-batches
 */
export const getDeletedBatches = async (req: Request, res: Response) => {
    try {
        const { warehouse_id } = req.query;

        let whereClause = 'deleted_at IS NOT NULL';
        const params: any[] = [];

        if (warehouse_id) {
            whereClause += ' AND warehouse_id = $1';
            params.push(warehouse_id);
        }

        const result = await query(
            `SELECT 
                batch_id,
                COUNT(*) as count,
                MIN(created_at) as created_at,
                MAX(deleted_at) as deleted_at,
                MAX(deleted_by_name) as deleted_by_name,
                uploaded_by_name
             FROM rejections
             WHERE ${whereClause}
             GROUP BY batch_id, uploaded_by_name
             ORDER BY MAX(deleted_at) DESC
             LIMIT 50`,
            params
        );

        res.json({ batches: result.rows });
    } catch (error: any) {
        console.error('❌ Get deleted batches error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Restore a soft-deleted batch
 * PUT /api/rejections/batch/:batchId/restore
 */
export const restoreBatch = async (req: Request, res: Response) => {
    try {
        const { batchId } = req.params;

        const result = await query(
            `UPDATE rejections SET deleted_at = NULL, deleted_by_name = NULL
             WHERE batch_id = $1 AND deleted_at IS NOT NULL RETURNING id`,
            [batchId]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'No deleted batch found with this ID' });
        }

        res.json({
            success: true,
            message: `Restored ${result.rows.length} rejections from batch "${batchId}"`,
            restored: result.rows.length
        });
    } catch (error: any) {
        console.error('❌ Restore batch error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * Permanently delete a batch (from trash)
 * DELETE /api/rejections/batch/:batchId/permanent
 */
export const permanentDeleteBatch = async (req: Request, res: Response) => {
    try {
        const { batchId } = req.params;

        const result = await query(
            'DELETE FROM rejections WHERE batch_id = $1 AND deleted_at IS NOT NULL RETURNING id',
            [batchId]
        );

        res.json({
            success: true,
            message: `Permanently deleted ${result.rows.length} rejections from batch`,
            deleted: result.rows.length
        });
    } catch (error: any) {
        console.error('❌ Permanent delete batch error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};
