// File Path = warehouse-backend/src/controllers/backup.controller.ts
import { Request, Response } from 'express';
import { query, getPool } from '../config/database';
import fs from 'fs';
import fsPromises from 'fs/promises';
import path from 'path';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { createJSONBackup, exportTableAsCSV, getDatabaseStatistics } from '../utils/supabaseBackup';
import { backupScheduler } from '../services/backupScheduler';
import { uploadToSupabase, deleteFromSupabase, downloadFromSupabase, isSupabaseStorageConfigured, listFiles, STORAGE_BUCKETS } from '../services/supabaseStorage';
import readline from 'readline';
import zlib from 'zlib';
import { safeError } from '../utils/sanitizeError';
import { logAudit, getClientIp } from '../utils/auditLogger';

const execFilePromise = promisify(execFile);

const BACKUP_DIR = path.join(__dirname, '../../backups');

// Ensure backup directory exists
if (!fs.existsSync(BACKUP_DIR)) {
    fs.mkdirSync(BACKUP_DIR, { recursive: true });
}

// �️ Tables that should NEVER be overwritten during restore
// These are live session/auth tables — overwriting them logs out users and blocks login
const RESTORE_EXCLUDED_TABLES = new Set([
    'active_sessions',
    'login_history',
    'user_activity_logs',
]);

// �🔐 SECURITY: Whitelist of allowed table names to prevent SQL injection
const ALLOWED_RESTORE_TABLES = new Set([
    // Core business data
    'warehouses',
    'customers',
    'racks',
    'master_data',
    'inbound',
    'qc',
    'picking',
    'outbound',
    'rejections',
    'receiving_wsns',
    // User & auth
    'users',
    'roles',
    'permissions',
    'role_permissions',
    'user_permissions',
    'user_permission_overrides',
    'user_warehouses',
    'active_sessions',
    'login_history',
    'user_activity_logs',
    // UI access
    'role_ui_access',
    'user_ui_overrides',
    // Permission approval system
    'permission_change_requests',
    'permission_change_details',
    // Upload system
    'upload_logs',
    'upload_progress',
    'batch_snapshots',
    // Backup system
    'error_logs',
    'backups',
    'backup_schedules',
    'backup_restore_logs',
    'backup_health_stats',
    // Drafts
    'inbound_multi_entry_drafts',
    'outbound_multi_entry_drafts',
    'picking_multi_entry_drafts',
    'qc_multi_entry_drafts',
    // Live view
    'live_entry_sessions',
    'live_entries',
    // Change tracking
    'data_change_log'
]);

/**
 * Validate table name against whitelist to prevent SQL injection
 */
function isValidTableName(tableName: string): boolean {
    return ALLOWED_RESTORE_TABLES.has(tableName.toLowerCase());
}

/**
 * Get sanitized table name or throw error
 */
function getSafeTableName(tableName: string): string {
    const normalizedName = tableName.toLowerCase().trim();
    if (!ALLOWED_RESTORE_TABLES.has(normalizedName)) {
        throw new Error(`Invalid table name: ${tableName}`);
    }
    return normalizedName;
}

// Store backup/restore progress for async operations
const backupProgress = new Map<string, {
    status: string;
    progress: number;
    message: string;
    result?: any;
    details?: {
        currentTable?: string;
        tableProgress?: number;
        completedTables?: number;
        totalTables?: number;
        processedRows?: number;
        totalRows?: number;
    };
}>();

// ================= CREATE DATABASE BACKUP =================
export const createBackup = async (req: Request, res: Response) => {
    try {
        const { backup_type = 'full', description = '', use_json = true, async_mode = true } = req.body;
        const user = (req as any).user;

        // If JSON backup is requested (works on Supabase)
        if (use_json || backup_type === 'json') {
            console.log('🔄 Creating JSON backup (Supabase-friendly)...');

            // Generate a unique backup ID for tracking
            const backupId = `backup_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

            // For large data, use async mode to prevent timeout
            if (async_mode) {
                // Initialize progress tracking
                backupProgress.set(backupId, {
                    status: 'in_progress',
                    progress: 0,
                    message: 'Starting backup...'
                });

                // Send immediate response with backup ID
                res.json({
                    success: true,
                    message: 'Backup started in background',
                    backupId,
                    status: 'in_progress'
                });

                // Process backup in background
                processBackupAsync(backupId, backup_type, description, user?.id);
                return;
            }

            // Sync mode (for small data)
            const backupResult = await createJSONBackup({
                includeUsers: backup_type === 'full'
            });

            // Upload to Supabase Storage (if configured)
            if (isSupabaseStorageConfigured()) {
                await uploadToSupabase(backupResult.filePath, backupResult.fileName, STORAGE_BUCKETS.BACKUPS);
            }

            // Save backup metadata to database
            const result = await query(
                `INSERT INTO backups (
          file_name, 
          file_path, 
          file_size, 
          backup_type, 
          description, 
          created_by
        ) VALUES ($1, $2, $3, $4, $5, $6) 
        RETURNING *`,
                [
                    backupResult.fileName,
                    backupResult.filePath,
                    backupResult.fileSize,
                    'json',
                    description || 'JSON backup',
                    user?.id || null
                ]
            );

            return res.json({
                success: true,
                message: 'JSON backup created successfully',
                backup: {
                    ...result.rows[0],
                    file_size_mb: backupResult.fileSizeMB
                }
            });
        }

        // Original pg_dump backup (requires PostgreSQL tools)
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const backupFileName = `wms_backup_${backup_type}_${timestamp}.sql`;
        const backupFilePath = path.join(BACKUP_DIR, backupFileName);

        console.log('🔄 Starting pg_dump backup...');

        // Parse database URL
        const dbUrl = process.env.DATABASE_URL;
        if (!dbUrl) {
            return res.status(500).json({ error: 'Database URL not configured' });
        }

        // Parse connection string
        const urlParts = new URL(dbUrl);
        const host = urlParts.hostname;
        const port = urlParts.port || '5432';
        const database = urlParts.pathname.slice(1);
        const username = urlParts.username;
        const password = urlParts.password;

        // Build pg_dump arguments array (prevents shell injection)
        const pgDumpArgs: string[] = [
            '-h', host,
            '-p', port,
            '-U', username,
            '-d', database,
        ];

        if (backup_type === 'schema') {
            pgDumpArgs.push('--schema-only');
        } else if (backup_type === 'data') {
            pgDumpArgs.push('--data-only');
        }
        // Full backup: no extra flags needed

        pgDumpArgs.push('-f', backupFilePath);

        // Execute backup using execFile (no shell — prevents injection)
        await execFilePromise('pg_dump', pgDumpArgs, {
            env: { ...process.env, PGPASSWORD: password }
        });

        // Get file size
        const stats = fs.statSync(backupFilePath);
        const fileSizeInBytes = stats.size;
        const fileSizeInMB = (fileSizeInBytes / (1024 * 1024)).toFixed(2);

        // Save backup metadata to database
        const result = await query(
            `INSERT INTO backups (
        file_name, 
        file_path, 
        file_size, 
        backup_type, 
        description, 
        created_by
      ) VALUES ($1, $2, $3, $4, $5, $6) 
      RETURNING *`,
            [
                backupFileName,
                backupFilePath,
                fileSizeInBytes,
                backup_type,
                description,
                user?.id || null
            ]
        );

        console.log(`✅ Backup created successfully: ${backupFileName} (${fileSizeInMB} MB)`);

        res.json({
            success: true,
            message: 'Backup created successfully',
            backup: {
                ...result.rows[0],
                file_size_mb: fileSizeInMB
            }
        });

    } catch (error: any) {
        console.error('❌ Backup creation error:', error);
        res.status(500).json({
            error: 'Backup failed',
            details: safeError(error, 'pg_dump execution failed'),
            note: 'Make sure pg_dump is installed and accessible in PATH'
        });
    }
};

// ================= ASYNC BACKUP PROCESSOR =================
async function processBackupAsync(backupId: string, backup_type: string, description: string, userId: number | null) {
    try {
        backupProgress.set(backupId, {
            status: 'in_progress',
            progress: 10,
            message: 'Connecting to database...'
        });

        const backupResult = await createJSONBackup({
            includeUsers: backup_type === 'full',
            onProgress: (table, current, total) => {
                const percent = Math.round((current / total) * 100);
                backupProgress.set(backupId, {
                    status: 'in_progress',
                    progress: 10 + Math.round(percent * 0.8), // 10-90%
                    message: `Exporting ${table}: ${current}/${total} rows`
                });
            }
        });

        backupProgress.set(backupId, {
            status: 'in_progress',
            progress: 90,
            message: 'Saving backup metadata...'
        });

        // Upload to Supabase Storage (if configured)
        if (isSupabaseStorageConfigured()) {
            backupProgress.set(backupId, {
                status: 'in_progress',
                progress: 92,
                message: 'Uploading to cloud storage...'
            });
            await uploadToSupabase(backupResult.filePath, backupResult.fileName, STORAGE_BUCKETS.BACKUPS);
        }

        // Save backup metadata to database
        const result = await query(
            `INSERT INTO backups (
                file_name, file_path, file_size, backup_type, description, created_by
            ) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`,
            [
                backupResult.fileName,
                backupResult.filePath,
                backupResult.fileSize,
                'json',
                description || 'JSON backup',
                userId
            ]
        );

        backupProgress.set(backupId, {
            status: 'completed',
            progress: 100,
            message: 'Backup completed successfully!',
            result: {
                ...result.rows[0],
                file_size_mb: backupResult.fileSizeMB,
                tableStats: backupResult.tableStats
            }
        });

        console.log(`✅ Async backup completed: ${backupResult.fileName} (${backupResult.fileSizeMB} MB)`);

        // Clean up progress after 10 minutes
        setTimeout(() => backupProgress.delete(backupId), 10 * 60 * 1000);

    } catch (error: any) {
        console.error('❌ Async backup error:', error);
        backupProgress.set(backupId, {
            status: 'failed',
            progress: 0,
            message: `Backup failed: ${error.message}`
        });
    }
}

// ================= CHECK BACKUP PROGRESS =================
export const getBackupProgress = async (req: Request, res: Response) => {
    try {
        const { backupId } = req.params;

        const progress = backupProgress.get(backupId);

        if (!progress) {
            return res.status(404).json({
                error: 'Backup not found or expired',
                message: 'Please check the backup list for completed backups'
            });
        }

        res.json(progress);
    } catch (error: any) {
        res.status(500).json({ error: safeError(error) });
    }
};

// ================= SELECTIVE BACKUP (Specific Tables) =================
export const createSelectiveBackup = async (req: Request, res: Response) => {
    try {
        const { tables, description = '' } = req.body;
        const user = (req as any).user;

        if (!tables || !Array.isArray(tables) || tables.length === 0) {
            return res.status(400).json({
                error: 'Please select at least one table/module to backup'
            });
        }

        // Valid tables for selective backup
        const validTables = [
            'warehouses', 'customers', 'racks', 'master_data',
            'inbound', 'qc', 'picking', 'outbound', 'users'
        ];

        const invalidTables = tables.filter((t: string) => !validTables.includes(t));
        if (invalidTables.length > 0) {
            return res.status(400).json({
                error: `Invalid tables: ${invalidTables.join(', ')}`
            });
        }

        console.log(`🔄 Creating selective backup for tables: ${tables.join(', ')}`);

        // Generate backup ID for tracking
        const backupId = `backup_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

        // Initialize progress tracking
        backupProgress.set(backupId, {
            status: 'in_progress',
            progress: 0,
            message: 'Starting selective backup...'
        });

        // Send immediate response
        res.json({
            success: true,
            message: 'Selective backup started',
            backupId,
            tables,
            status: 'in_progress'
        });

        // Process backup in background
        processSelectiveBackupAsync(backupId, tables, description, user?.id);

    } catch (error: any) {
        console.error('❌ Selective backup error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// Async selective backup processor
async function processSelectiveBackupAsync(
    backupId: string,
    tables: string[],
    description: string,
    userId: number | null
) {
    try {
        backupProgress.set(backupId, {
            status: 'in_progress',
            progress: 5,
            message: 'Preparing selective backup...'
        });

        const backupResult = await createJSONBackup({
            tables,
            onProgress: (table, current, total) => {
                const percent = Math.round((current / total) * 100);
                backupProgress.set(backupId, {
                    status: 'in_progress',
                    progress: 5 + Math.round(percent * 0.85),
                    message: `Exporting ${table}: ${current}/${total} rows`
                });
            }
        });

        backupProgress.set(backupId, {
            status: 'in_progress',
            progress: 92,
            message: 'Saving backup...'
        });

        // Save backup metadata
        const result = await query(
            `INSERT INTO backups (
                file_name, file_path, file_size, backup_type, description, created_by
            ) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`,
            [
                backupResult.fileName,
                backupResult.filePath,
                backupResult.fileSize,
                'json',
                description || `Selective backup: ${tables.join(', ')}`,
                userId
            ]
        );

        backupProgress.set(backupId, {
            status: 'completed',
            progress: 100,
            message: 'Selective backup completed!',
            result: {
                ...result.rows[0],
                file_size_mb: backupResult.fileSizeMB,
                tables_backed_up: tables
            }
        });

        console.log(`✅ Selective backup completed: ${backupResult.fileName}`);
        setTimeout(() => backupProgress.delete(backupId), 10 * 60 * 1000);

    } catch (error: any) {
        console.error('❌ Selective backup error:', error);
        backupProgress.set(backupId, {
            status: 'failed',
            progress: 0,
            message: `Backup failed: ${error.message}`
        });
    }
}

// ================= GET ALL BACKUPS =================
export const getAllBackups = async (req: Request, res: Response) => {
    try {
        const result = await query(
            `SELECT 
        id,
        file_name,
        file_size,
        backup_type,
        description,
        created_by,
        created_at,
        ROUND(file_size / 1024.0 / 1024.0, 2) as file_size_mb
      FROM backups 
      ORDER BY created_at DESC`
        );

        res.json(result.rows);
    } catch (error: any) {
        console.error('❌ Get backups error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ================= DOWNLOAD BACKUP FILE =================
export const downloadBackup = async (req: Request, res: Response) => {
    try {
        const { id } = req.params;

        const result = await query(
            'SELECT file_name, file_path FROM backups WHERE id = $1',
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Backup not found' });
        }

        const backup = result.rows[0];
        const filePath = backup.file_path;

        // Check if file exists on local disk
        let fileExistsOnDisk = false;
        try {
            await fsPromises.access(filePath, fs.constants.R_OK);
            fileExistsOnDisk = true;
        } catch {
            // File not on disk (expected on Render's ephemeral filesystem after deploy/restart)
        }

        // If not on disk, try to recover from Supabase Storage
        if (!fileExistsOnDisk && isSupabaseStorageConfigured()) {
            console.log(`📥 File not on disk, downloading from Supabase Storage: ${backup.file_name}`);

            // Ensure backup directory exists
            const dir = path.dirname(filePath);
            if (!fs.existsSync(dir)) {
                fs.mkdirSync(dir, { recursive: true });
            }

            const downloaded = await downloadFromSupabase(backup.file_name, filePath, STORAGE_BUCKETS.BACKUPS);
            if (downloaded) {
                fileExistsOnDisk = true;
                console.log(`✅ Recovered backup from Supabase Storage: ${backup.file_name}`);
            } else {
                console.error(`❌ Failed to recover backup from Supabase Storage: ${backup.file_name}`);
            }
        }

        if (!fileExistsOnDisk) {
            return res.status(404).json({ error: 'Backup file not found on disk or cloud storage' });
        }

        logAudit({ action: 'BACKUP_DOWNLOAD', performedBy: (req as any).user?.username || 'unknown', details: `Downloaded backup: ${backup.file_name}`, ipAddress: getClientIp(req) });
        res.download(filePath, backup.file_name);
    } catch (error: any) {
        console.error('❌ Download backup error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ================= DELETE BACKUP =================
export const deleteBackup = async (req: Request, res: Response) => {
    try {
        const { id } = req.params;

        const result = await query(
            'SELECT file_path, file_name FROM backups WHERE id = $1',
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Backup not found' });
        }

        const backup = result.rows[0];
        const filePath = backup.file_path;
        const fileName = backup.file_name;

        // Delete from Supabase Storage (if configured)
        if (isSupabaseStorageConfigured()) {
            await deleteFromSupabase(fileName, STORAGE_BUCKETS.BACKUPS);
        }

        // Delete file from local disk - ⚡ OPTIMIZED: Use async file operations
        try {
            await fsPromises.access(filePath, fs.constants.F_OK);
            await fsPromises.unlink(filePath);
        } catch {
            // File doesn't exist, that's okay
        }

        // Delete record from database
        await query('DELETE FROM backups WHERE id = $1', [id]);

        console.log(`✅ Backup deleted: ${id}`);
        logAudit({ action: 'BACKUP_DELETE', performedBy: (req as any).user?.username || 'unknown', details: `Deleted backup ID ${id}`, ipAddress: getClientIp(req) });
        res.json({ message: 'Backup deleted successfully' });

    } catch (error: any) {
        console.error('❌ Delete backup error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ================= BULK DELETE BACKUPS =================
export const bulkDeleteBackups = async (req: Request, res: Response) => {
    try {
        const { ids } = req.body;

        if (!ids || !Array.isArray(ids) || ids.length === 0) {
            return res.status(400).json({ error: 'Please provide backup IDs to delete' });
        }

        console.log(`🗑️ Bulk deleting ${ids.length} backups...`);

        // Get all backup details
        const result = await query(
            'SELECT id, file_path, file_name FROM backups WHERE id = ANY($1)',
            [ids]
        );

        let deletedCount = 0;
        let failedCount = 0;
        const errors: string[] = [];
        const successfulIds: number[] = [];

        for (const backup of result.rows) {
            try {
                // Delete from Supabase Storage (if configured)
                if (isSupabaseStorageConfigured()) {
                    await deleteFromSupabase(backup.file_name, STORAGE_BUCKETS.BACKUPS);
                }

                // Delete file from local disk
                try {
                    await fsPromises.access(backup.file_path, fs.constants.F_OK);
                    await fsPromises.unlink(backup.file_path);
                } catch {
                    // File doesn't exist, that's okay
                }

                successfulIds.push(backup.id);
                deletedCount++;
            } catch (err: any) {
                failedCount++;
                errors.push(`Failed to delete backup ${backup.id}: ${err.message}`);
            }
        }

        // BATCH DELETE: Remove all successfully-cleaned backup records in one query
        if (successfulIds.length > 0) {
            await query('DELETE FROM backups WHERE id = ANY($1)', [successfulIds]);
        }

        console.log(`✅ Bulk delete completed: ${deletedCount} deleted, ${failedCount} failed`);

        res.json({
            success: true,
            message: `${deletedCount} backup(s) deleted successfully`,
            deletedCount,
            failedCount,
            errors: errors.length > 0 ? errors : undefined
        });

    } catch (error: any) {
        console.error('❌ Bulk delete backup error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ================= RESTORE DATABASE (ASYNC with Progress) =================
export const restoreBackup = async (req: Request, res: Response) => {
    try {
        const { id } = req.params;
        const { confirm, selectedTables } = req.body;

        if (!confirm) {
            return res.status(400).json({
                error: 'Confirmation required',
                message: 'Please confirm that you want to restore the database.'
            });
        }

        const result = await query(
            'SELECT file_name, file_path, backup_type, file_size FROM backups WHERE id = $1',
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Backup not found' });
        }

        const backup = result.rows[0];
        const filePath = backup.file_path;

        // ⚡ OPTIMIZED: Use async file check
        try {
            await fsPromises.access(filePath, fs.constants.R_OK);
        } catch {
            return res.status(404).json({ error: 'Backup file not found on disk' });
        }

        // Generate restore ID for progress tracking
        const restoreId = `restore_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

        // Initialize progress
        backupProgress.set(restoreId, {
            status: 'in_progress',
            progress: 0,
            message: 'Starting restore...'
        });

        // Send immediate response with restore ID
        res.json({
            success: true,
            message: 'Restore started in background',
            restoreId,
            status: 'in_progress'
        });

        // Process restore in background
        processRestoreAsync(restoreId, id, backup, filePath, selectedTables);

    } catch (error: any) {
        console.error('❌ Restore error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ================= UPLOAD & RESTORE (from local file) =================
export const uploadAndRestore = async (req: Request, res: Response) => {
    try {
        // Support two modes:
        // 1) New file upload via multipart form (file in req.file)
        // 2) Re-use file from upload-preview step (tempFilePath + originalName in body)
        const file = (req as any).file;
        const { selectedTables, tempFilePath, originalName } = req.body;

        let filePath: string;
        let fileName: string;
        let fileSize: number;

        if (tempFilePath && fs.existsSync(tempFilePath)) {
            // Mode 2: File was already uploaded during preview
            filePath = tempFilePath;
            fileName = originalName || path.basename(tempFilePath);
            fileSize = fs.statSync(tempFilePath).size;
        } else if (file) {
            // Mode 1: Fresh upload
            const ext = path.extname(file.originalname).toLowerCase();
            const isGz = file.originalname.endsWith('.json.gz') || ext === '.gz';
            const isJson = ext === '.json';

            if (!isJson && !isGz) {
                fs.unlinkSync(file.path);
                return res.status(400).json({ error: 'Invalid file type. Only .json and .json.gz files are accepted.' });
            }
            filePath = file.path;
            fileName = file.originalname;
            fileSize = file.size;
        } else {
            return res.status(400).json({ error: 'No file uploaded. Please upload a .json or .json.gz backup file.' });
        }

        let parsedSelectedTables: string[] | undefined;
        if (selectedTables) {
            try {
                parsedSelectedTables = typeof selectedTables === 'string' ? JSON.parse(selectedTables) : selectedTables;
            } catch { parsedSelectedTables = undefined; }
        }

        // Generate restore ID for progress tracking
        const restoreId = `restore_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

        backupProgress.set(restoreId, {
            status: 'in_progress',
            progress: 0,
            message: 'Processing uploaded backup file...'
        });

        res.json({
            success: true,
            message: 'Upload received, restore started in background',
            restoreId,
            status: 'in_progress'
        });

        // Build a fake backup record so processRestoreAsync sees the fields it needs
        const fakeBackup = {
            file_name: fileName,
            file_path: filePath,
            backup_type: 'json',
            file_size: fileSize
        };

        // Process restore, then clean up temp file
        processRestoreAsync(restoreId, 'upload', fakeBackup, filePath, parsedSelectedTables, () => {
            try { if (fs.existsSync(filePath)) fs.unlinkSync(filePath); } catch { /* ignore */ }
        });

    } catch (error: any) {
        console.error('❌ Upload & restore error:', error);
        // Clean up file on error
        try {
            const fp = (req as any).file?.path;
            if (fp && fs.existsSync(fp)) fs.unlinkSync(fp);
        } catch { /* ignore */ }
        res.status(500).json({ error: safeError(error) });
    }
};

// ================= CLOUD PREVIEW (download latest + return table comparison) =================
export const cloudPreview = async (req: Request, res: Response) => {
    try {
        if (!isSupabaseStorageConfigured()) {
            return res.status(400).json({ error: 'Supabase Storage is not configured. Please set SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables.' });
        }

        // List all files in the backups bucket (already sorted by created_at desc)
        const files = await listFiles(STORAGE_BUCKETS.BACKUPS);
        const backupFiles = files.filter(f => f.endsWith('.json') || f.endsWith('.json.gz') || f.endsWith('.gz'));

        if (backupFiles.length === 0) {
            return res.status(404).json({ error: 'No backup files found in cloud storage.' });
        }

        const latestFile = backupFiles[0];
        console.log(`☁️ Cloud preview: downloading ${latestFile} for preview...`);

        // Download to temp folder
        const tempDir = path.join(BACKUP_DIR, 'temp');
        if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });
        const tempPath = path.join(tempDir, `cloud-preview-${Date.now()}-${latestFile}`);

        const downloaded = await downloadFromSupabase(latestFile, tempPath, STORAGE_BUCKETS.BACKUPS);
        if (!downloaded || !fs.existsSync(tempPath)) {
            return res.status(500).json({ error: 'Failed to download backup from cloud storage.' });
        }

        try {
            console.log(`☁️ Cloud preview: analyzing ${tempPath}`);

            // Use simple sync decompression + brace-counting — no streaming
            const { tables, counts: backupCounts } = await getBackupTableCounts(tempPath);
            const safeTables = tables.filter((t: string) => isValidTableName(t) && !RESTORE_EXCLUDED_TABLES.has(t));

            // Get DB counts in parallel
            const tableComparison: { table: string; backupRows: number; dbRows: number }[] = [];
            await Promise.all(safeTables.map(async (tableName) => {
                try {
                    const safeTableName = getSafeTableName(tableName);
                    const dbResult = await query(`SELECT COUNT(*) FROM ${safeTableName}`);
                    const dbCount = parseInt(dbResult.rows[0].count, 10);
                    tableComparison.push({ table: tableName, backupRows: backupCounts[tableName] || 0, dbRows: dbCount });
                } catch {
                    tableComparison.push({ table: tableName, backupRows: backupCounts[tableName] || 0, dbRows: 0 });
                }
            }));

            tableComparison.sort((a, b) => safeTables.indexOf(a.table) - safeTables.indexOf(b.table));

            res.json({
                success: true,
                fileName: latestFile,
                filePath: tempPath,
                fileSize: fs.statSync(tempPath).size,
                tables: tableComparison
            });
        } catch (err: any) {
            // Clean up temp file on parse error
            try { if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath); } catch { }
            throw err;
        }
    } catch (error: any) {
        console.error('❌ Cloud preview error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ================= RESTORE LATEST FROM CLOUD =================
export const restoreLatestFromCloud = async (req: Request, res: Response) => {
    try {
        const { confirm, selectedTables, tempFilePath } = req.body;
        if (!confirm) {
            return res.status(400).json({ error: 'Confirmation required', message: 'Please confirm that you want to restore from cloud.' });
        }

        if (!isSupabaseStorageConfigured()) {
            return res.status(400).json({ error: 'Supabase Storage is not configured. Please set SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables.' });
        }

        let filePath: string;
        let fileName: string;

        if (tempFilePath && fs.existsSync(tempFilePath)) {
            // Mode: Re-use file from cloud-preview step
            filePath = tempFilePath;
            fileName = path.basename(tempFilePath).replace(/^cloud-preview-\d+-/, '');
        } else {
            // Fallback: Download fresh from cloud
            const files = await listFiles(STORAGE_BUCKETS.BACKUPS);
            const backupFiles = files.filter(f => f.endsWith('.json') || f.endsWith('.json.gz') || f.endsWith('.gz'));

            if (backupFiles.length === 0) {
                return res.status(404).json({ error: 'No backup files found in cloud storage.' });
            }

            const latestFile = backupFiles[0];
            fileName = latestFile;
            console.log(`☁️ Restore from cloud: downloading ${latestFile}...`);

            const tempDir = path.join(BACKUP_DIR, 'temp');
            if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });
            filePath = path.join(tempDir, `cloud-${Date.now()}-${latestFile}`);

            const downloaded = await downloadFromSupabase(latestFile, filePath, STORAGE_BUCKETS.BACKUPS);
            if (!downloaded || !fs.existsSync(filePath)) {
                return res.status(500).json({ error: 'Failed to download backup from cloud storage.' });
            }
        }

        let parsedSelectedTables: string[] | undefined;
        if (selectedTables) {
            try {
                parsedSelectedTables = typeof selectedTables === 'string' ? JSON.parse(selectedTables) : selectedTables;
            } catch { parsedSelectedTables = undefined; }
        }

        const restoreId = `restore_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        backupProgress.set(restoreId, {
            status: 'in_progress',
            progress: 0,
            message: `Starting cloud restore for ${fileName}...`
        });

        // Return immediately with restoreId
        res.json({
            success: true,
            message: `Restoring ${fileName}`,
            restoreId,
            status: 'in_progress',
            fileName
        });

        // Restore in background
        const fakeBackup = {
            file_name: fileName,
            file_path: filePath,
            backup_type: 'json',
            file_size: fs.statSync(filePath).size
        };

        processRestoreAsync(restoreId, 'cloud', fakeBackup, filePath, parsedSelectedTables, () => {
            try { if (fs.existsSync(filePath)) fs.unlinkSync(filePath); } catch { /* ignore */ }
        });

    } catch (error: any) {
        console.error('❌ Restore latest from cloud error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ================= RESTORE PREVIEW (compare backup vs DB) =================
export const getRestorePreviewCompare = async (req: Request, res: Response) => {
    try {
        const { id } = req.params;

        const result = await query(
            'SELECT file_name, file_path, backup_type, file_size FROM backups WHERE id = $1',
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Backup not found' });
        }

        const backup = result.rows[0];
        let filePath = backup.file_path;

        // Check if file exists on disk; try Supabase download fallback
        try {
            await fsPromises.access(filePath, fs.constants.R_OK);
        } catch {
            if (isSupabaseStorageConfigured()) {
                const tempDir = path.join(BACKUP_DIR, 'temp');
                if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });
                const tempPath = path.join(tempDir, `preview-${Date.now()}-${backup.file_name}`);
                const ok = await downloadFromSupabase(backup.file_name, tempPath, STORAGE_BUCKETS.BACKUPS);
                if (ok && fs.existsSync(tempPath)) {
                    filePath = tempPath;
                    // Clean up after 2 min
                    setTimeout(() => { try { if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath); } catch { } }, 2 * 60 * 1000);
                } else {
                    return res.status(404).json({ error: 'Backup file not found on disk or cloud' });
                }
            } else {
                return res.status(404).json({ error: 'Backup file not found on disk' });
            }
        }

        // Use simple sync decompression + brace-counting for reliable preview
        console.log(`📋 Restore preview: analyzing ${filePath}`);
        const { tables, counts: backupCounts } = await getBackupTableCounts(filePath);
        const safeTables = tables.filter((t: string) => isValidTableName(t) && !RESTORE_EXCLUDED_TABLES.has(t));

        // Get DB row counts in parallel
        const tableComparison: { table: string; backupRows: number; dbRows: number }[] = [];
        await Promise.all(safeTables.map(async (tableName) => {
            try {
                const safeTableName = getSafeTableName(tableName);
                const dbResult = await query(`SELECT COUNT(*) FROM ${safeTableName}`);
                const dbCount = parseInt(dbResult.rows[0].count, 10);
                tableComparison.push({ table: tableName, backupRows: backupCounts[tableName] || 0, dbRows: dbCount });
            } catch {
                tableComparison.push({ table: tableName, backupRows: backupCounts[tableName] || 0, dbRows: 0 });
            }
        }));

        // Sort to match original table order
        tableComparison.sort((a, b) => safeTables.indexOf(a.table) - safeTables.indexOf(b.table));

        res.json({
            fileName: backup.file_name,
            createdAt: backup.created_at || null,
            tables: tableComparison
        });
    } catch (error: any) {
        console.error('❌ Restore preview compare error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// Async restore processor with progress tracking - SAFE UPSERT VERSION
// Uses INSERT ... ON CONFLICT DO UPDATE (UPSERT) — NEVER deletes data
async function processRestoreAsync(
    restoreId: string,
    backupId: string,
    backup: any,
    filePath: string,
    selectedTables?: string[],
    onComplete?: () => void
) {
    try {
        console.log('🔄 Starting SAFE UPSERT database restore (zero data loss)...');

        backupProgress.set(restoreId, {
            status: 'in_progress',
            progress: 2,
            message: 'Creating safety backup of current data...'
        });

        // =====================================================
        // LAYER 1: AUTO PRE-RESTORE BACKUP
        // =====================================================
        let preRestoreBackupName = '';
        try {
            console.log('🛡️ Creating pre-restore safety backup...');
            const preBackupResult = await createJSONBackup({ includeUsers: true });
            preRestoreBackupName = preBackupResult.fileName;

            // Upload to cloud in background (fire-and-forget — don't block restore)
            if (isSupabaseStorageConfigured()) {
                uploadToSupabase(preBackupResult.filePath, preBackupResult.fileName, STORAGE_BUCKETS.BACKUPS).catch(() => { });
            }

            // Save metadata to DB
            await query(
                `INSERT INTO backups (file_name, file_path, file_size, backup_type, description, created_by)
                 VALUES ($1, $2, $3, $4, $5, $6)`,
                [
                    preBackupResult.fileName,
                    preBackupResult.filePath,
                    preBackupResult.fileSize,
                    'json',
                    `Auto pre-restore safety backup (before restoring ${backup.file_name})`,
                    null
                ]
            );
            console.log(`🛡️ Pre-restore backup created: ${preBackupResult.fileName}`);
        } catch (preBackupError: any) {
            console.warn('⚠️ Pre-restore backup failed (continuing with restore):', preBackupError.message);
        }

        backupProgress.set(restoreId, {
            status: 'in_progress',
            progress: 5,
            message: 'Analyzing backup file...'
        });

        // Check if it's a JSON backup (.json or .json.gz)
        if (backup.backup_type === 'json' || filePath.endsWith('.json') || filePath.endsWith('.gz')) {

            // Get file size to estimate progress
            const fileStats = fs.statSync(filePath);
            const fileSizeMB = (fileStats.size / (1024 * 1024)).toFixed(2);
            console.log(`📦 Backup file size: ${fileSizeMB} MB${filePath.endsWith('.gz') ? ' (compressed)' : ''}`);

            // ⚡ DECOMPRESS ONCE: Read + decompress the entire file into memory (fast for typical backup sizes)
            // This avoids N decompressions when parsing N tables — massive speed improvement
            console.time('decompress');
            let fullBackupStr: string;
            if (filePath.endsWith('.gz')) {
                const compressed = fs.readFileSync(filePath);
                const decompressed = zlib.gunzipSync(compressed);
                fullBackupStr = decompressed.toString('utf8');
                console.log(`⚡ Decompressed: ${fileSizeMB} MB → ${(decompressed.length / (1024 * 1024)).toFixed(2)} MB`);
            } else {
                fullBackupStr = fs.readFileSync(filePath, 'utf8');
            }
            console.timeEnd('decompress');

            // Extract table names from metadata
            const tablesMatch = fullBackupStr.match(/"tables"\s*:\s*\[(.*?)\]/s);
            let tables: string[] = [];

            if (tablesMatch) {
                try {
                    tables = JSON.parse(`[${tablesMatch[1]}]`);
                } catch {
                    tables = ['warehouses', 'customers', 'racks', 'master_data', 'inbound', 'qc', 'picking', 'outbound'];
                }
            } else {
                tables = ['warehouses', 'customers', 'racks', 'master_data', 'inbound', 'qc', 'picking', 'outbound'];
            }

            console.log(`📋 Tables in backup: ${tables.join(', ')}`);

            // 🔐 SECURITY: Filter tables to only include whitelisted ones
            // 🛡️ EXCLUDE session/auth tables to prevent logout during restore
            const safeTables = tables.filter((t: string) => isValidTableName(t) && !RESTORE_EXCLUDED_TABLES.has(t));
            if (safeTables.length !== tables.length) {
                const excluded = tables.filter((t: string) => RESTORE_EXCLUDED_TABLES.has(t));
                if (excluded.length > 0) {
                    console.log(`🛡️ Protected tables excluded from restore: ${excluded.join(', ')}`);
                }
                console.warn(`⚠️ Some tables were filtered out. Original: ${tables.length}, Restorable: ${safeTables.length}`);
            }

            // 📋 SELECTIVE RESTORE: If caller specified specific tables, filter to those only
            const tablesToRestore = (selectedTables && selectedTables.length > 0)
                ? safeTables.filter((t: string) => selectedTables.includes(t))
                : safeTables;

            if (selectedTables && selectedTables.length > 0) {
                console.log(`📋 Selective restore: ${tablesToRestore.length}/${safeTables.length} tables selected`);
            }

            backupProgress.set(restoreId, {
                status: 'in_progress',
                progress: 8,
                message: `Found ${tablesToRestore.length} tables, starting safe restore...`
            });

            // Result tracking with detailed per-table stats
            const restoreResults: {
                success: { table: string; inserted: number; updated: number; skipped: number; failed: number; total: number }[];
                failed: { table: string; error: string }[];
                skipped: { table: string; reason: string }[];
                preRestoreBackup: string;
            } = { success: [], failed: [], skipped: [], preRestoreBackup: preRestoreBackupName };
            let totalProcessedRows = 0;

            // Batch size for UPSERT operations (larger = fewer round-trips = faster)
            const BATCH_SIZE = 2000;

            // =====================================================
            // LAYER 5: TRANSACTION SAFETY
            // =====================================================
            const client = await getPool().connect();
            try {
                // Remove query/statement timeouts on restore connection to prevent timeout on large batches
                try {
                    await client.query('SET statement_timeout = 0');
                    await client.query('SET lock_timeout = 0');
                    console.log('⚡ Timeouts disabled for restore connection');
                } catch (e) {
                    console.log('⚠️ Could not disable timeouts (non-critical)');
                }

                // Disable triggers on this connection for speed
                try {
                    await client.query('SET session_replication_role = replica');
                    console.log('⚡ Triggers disabled for faster restore');
                } catch (e) {
                    console.log('⚠️ Could not disable triggers (non-critical)');
                }

                await client.query('BEGIN');

                // ⚡ Parse ALL tables from in-memory string (already decompressed above — zero extra I/O)
                console.log(`📦 Parsing and restoring ${tablesToRestore.length} tables from memory...`);

                // Process each table
                for (let tableIndex = 0; tableIndex < tablesToRestore.length; tableIndex++) {
                    const tableName = tablesToRestore[tableIndex];
                    const safeTableName = getSafeTableName(tableName);
                    const tableSavepoint = `sp_table_${tableIndex}`;

                    try {
                        await client.query(`SAVEPOINT ${tableSavepoint}`);

                        console.log(`  🔄 Safe UPSERT restore for: ${safeTableName}`);

                        backupProgress.set(restoreId, {
                            status: 'in_progress',
                            progress: 10 + Math.round((tableIndex / tablesToRestore.length) * 80),
                            message: `Restoring ${safeTableName}...`,
                            details: {
                                currentTable: tableName,
                                completedTables: tableIndex,
                                totalTables: tablesToRestore.length,
                                processedRows: totalProcessedRows
                            }
                        });

                        // ⚡ Parse this table from in-memory string (no file I/O, no decompression)
                        const tableRows = parseTableFromString(fullBackupStr, tableName);

                        if (tableRows.length === 0) {
                            console.log(`    ⏭️ Skipping ${tableName}: no data in backup`);
                            restoreResults.skipped.push({ table: tableName, reason: 'No data in backup' });
                            await client.query(`RELEASE SAVEPOINT ${tableSavepoint}`);
                            continue;
                        }

                        // =====================================================
                        // LAYER 3: COLUMN COMPATIBILITY CHECK
                        // =====================================================
                        const backupColumns = Object.keys(tableRows[0]);

                        // Get actual DB columns for this table
                        const dbColResult = await client.query(
                            `SELECT column_name FROM information_schema.columns 
                             WHERE table_schema = 'public' AND table_name = $1 
                             ORDER BY ordinal_position`,
                            [tableName]
                        );
                        const dbColumns = dbColResult.rows.map((r: any) => r.column_name);

                        // Use only columns that exist in BOTH backup AND DB
                        const commonColumns = backupColumns.filter(col => dbColumns.includes(col));

                        if (commonColumns.length === 0) {
                            console.warn(`    ⚠️ Skipping ${tableName}: no matching columns between backup and DB`);
                            restoreResults.skipped.push({ table: tableName, reason: 'No matching columns' });
                            await client.query(`RELEASE SAVEPOINT ${tableSavepoint}`);
                            continue;
                        }

                        const skippedCols = backupColumns.filter(col => !dbColumns.includes(col));
                        if (skippedCols.length > 0) {
                            console.log(`    📝 ${tableName}: Skipping ${skippedCols.length} columns not in DB: ${skippedCols.join(', ')}`);
                        }

                        // Check if table has 'id' column for UPSERT conflict target
                        const hasIdColumn = commonColumns.includes('id');
                        const updateColumns = commonColumns.filter(col => col !== 'id');

                        let tableInserted = 0;
                        let tableUpdated = 0;
                        let tableSkipped = 0;
                        let tableFailed = 0;
                        const failedReasons: string[] = [];

                        // Safety guard: compute safe batch size based on column count
                        const safeBatchSize = Math.min(BATCH_SIZE, Math.floor(65000 / commonColumns.length));

                        // =====================================================
                        // LAYER 2: UPSERT — NEVER DELETE, ONLY INSERT/UPDATE
                        // =====================================================
                        for (let i = 0; i < tableRows.length; i += safeBatchSize) {
                            const batch = tableRows.slice(i, Math.min(i + safeBatchSize, tableRows.length));
                            const batchSavepoint = `sp_batch_${tableIndex}_${i}`;
                            await client.query(`SAVEPOINT ${batchSavepoint}`);

                            try {
                                // Build bulk UPSERT
                                const values: any[] = [];
                                const valueRows: string[] = [];
                                let paramIndex = 1;

                                for (const row of batch) {
                                    const rowPlaceholders: string[] = [];
                                    for (const col of commonColumns) {
                                        let val = row[col] !== undefined ? row[col] : null;
                                        // Auto-stringify objects/arrays for JSONB/JSON columns
                                        if (val !== null && typeof val === 'object') {
                                            val = JSON.stringify(val);
                                        }
                                        values.push(val);
                                        rowPlaceholders.push(`$${paramIndex++}`);
                                    }
                                    valueRows.push(`(${rowPlaceholders.join(', ')})`);
                                }

                                let bulkSQL: string;
                                if (hasIdColumn && updateColumns.length > 0) {
                                    // UPSERT: Insert new rows, update existing ones by ID
                                    const updateClause = updateColumns
                                        .map(col => `${col} = EXCLUDED.${col}`)
                                        .join(', ');
                                    bulkSQL = `
                                        INSERT INTO ${safeTableName} (${commonColumns.join(', ')})
                                        VALUES ${valueRows.join(', ')}
                                        ON CONFLICT (id) DO UPDATE SET ${updateClause}
                                    `;
                                } else {
                                    // No id column — just insert, skip conflicts
                                    bulkSQL = `
                                        INSERT INTO ${safeTableName} (${commonColumns.join(', ')})
                                        VALUES ${valueRows.join(', ')}
                                        ON CONFLICT DO NOTHING
                                    `;
                                }

                                const upsertResult = await client.query(bulkSQL, values);
                                // rowCount reflects inserted + updated rows
                                const affectedRows = upsertResult.rowCount || 0;
                                tableInserted += affectedRows; // We track total affected; detailed split below
                                await client.query(`RELEASE SAVEPOINT ${batchSavepoint}`);

                            } catch (batchError: any) {
                                // Roll back the failed batch
                                await client.query(`ROLLBACK TO SAVEPOINT ${batchSavepoint}`);
                                console.warn(`    ⚠️ Bulk upsert failed for ${tableName} batch at ${i}, using row-by-row fallback: ${batchError.message}`);

                                // =====================================================
                                // LAYER 4: PER-ROW ERROR TRACKING
                                // =====================================================
                                for (const row of batch) {
                                    const rowSavepoint = `sp_row_${tableIndex}_${i}_${Math.random().toString(36).substr(2, 5)}`;
                                    try {
                                        await client.query(`SAVEPOINT ${rowSavepoint}`);
                                        const rowValues = commonColumns.map(col => {
                                            let val = row[col] !== undefined ? row[col] : null;
                                            if (val !== null && typeof val === 'object') val = JSON.stringify(val);
                                            return val;
                                        });
                                        const placeholders = commonColumns.map((_, idx) => `$${idx + 1}`).join(', ');

                                        let rowSQL: string;
                                        if (hasIdColumn && updateColumns.length > 0) {
                                            const updateClause = updateColumns
                                                .map(col => `${col} = EXCLUDED.${col}`)
                                                .join(', ');
                                            rowSQL = `INSERT INTO ${safeTableName} (${commonColumns.join(', ')}) VALUES (${placeholders}) ON CONFLICT (id) DO UPDATE SET ${updateClause}`;
                                        } else {
                                            rowSQL = `INSERT INTO ${safeTableName} (${commonColumns.join(', ')}) VALUES (${placeholders}) ON CONFLICT DO NOTHING`;
                                        }

                                        await client.query(rowSQL, rowValues);
                                        tableInserted++;
                                        await client.query(`RELEASE SAVEPOINT ${rowSavepoint}`);
                                    } catch (rowError: any) {
                                        await client.query(`ROLLBACK TO SAVEPOINT ${rowSavepoint}`).catch(() => { });
                                        tableFailed++;
                                        if (failedReasons.length < 5) {
                                            failedReasons.push(`Row ID ${row.id || '?'}: ${rowError.message?.substring(0, 100)}`);
                                        }
                                    }
                                }
                            }

                            // Update progress every 2000 rows or last batch
                            if (i % 2000 === 0 || i + safeBatchSize >= tableRows.length) {
                                const totalAffected = tableInserted + tableSkipped + tableFailed;
                                backupProgress.set(restoreId, {
                                    status: 'in_progress',
                                    progress: 8 + Math.round(((tableIndex + (Math.min(i + safeBatchSize, tableRows.length) / tableRows.length)) / tablesToRestore.length) * 82),
                                    message: `Restoring ${tableName}: ${totalAffected}/${tableRows.length} rows`,
                                    details: {
                                        currentTable: tableName,
                                        tableProgress: Math.round((totalAffected / tableRows.length) * 100),
                                        completedTables: tableIndex,
                                        totalTables: tablesToRestore.length,
                                        processedRows: totalProcessedRows + totalAffected
                                    }
                                });
                            }

                            // Let event loop breathe
                            if (i % 5000 === 0) {
                                await new Promise(resolve => setImmediate(resolve));
                            }
                        }

                        // SAFETY CHECK: If >50% rows failed, rollback this table
                        const totalAttempted = tableInserted + tableFailed;
                        if (totalAttempted > 0 && tableFailed > totalAttempted * 0.5) {
                            console.warn(`    🚨 ${tableName}: >50% rows failed (${tableFailed}/${totalAttempted}), rolling back table`);
                            await client.query(`ROLLBACK TO SAVEPOINT ${tableSavepoint}`);
                            restoreResults.failed.push({ table: tableName, error: `>50% rows failed: ${tableFailed}/${totalAttempted}. ${failedReasons.slice(0, 3).join('; ')}` });
                            continue;
                        }

                        // Table completed — release savepoint
                        await client.query(`RELEASE SAVEPOINT ${tableSavepoint}`);

                        console.log(`  ✅ Restored ${tableName}: ${tableInserted} affected, ${tableFailed} failed out of ${tableRows.length} rows`);
                        restoreResults.success.push({
                            table: tableName,
                            inserted: tableInserted,
                            updated: 0, // Cannot distinguish insert vs update from rowCount alone
                            skipped: tableSkipped,
                            failed: tableFailed,
                            total: tableRows.length
                        });
                        totalProcessedRows += tableInserted;

                    } catch (tableError: any) {
                        // Roll back only this table's changes; the transaction stays valid
                        await client.query(`ROLLBACK TO SAVEPOINT ${tableSavepoint}`).catch(() => { });
                        console.warn(`  ⚠️ Failed to restore ${tableName}:`, tableError.message);
                        restoreResults.failed.push({ table: tableName, error: tableError.message });
                    }
                }

                // Free the large backup string before committing (reduce memory pressure)
                fullBackupStr = '';

                await client.query('COMMIT');
            } catch (restoreError) {
                await client.query('ROLLBACK').catch(() => { });
                throw restoreError;
            } finally {
                // CRITICAL: Always re-enable triggers and release client
                try {
                    await client.query('SET session_replication_role = DEFAULT');
                    console.log('⚡ Triggers re-enabled');
                } catch (e) {
                    console.log('⚠️ Could not re-enable triggers');
                }
                client.release();
            }

            // Log restore action
            try {
                await query(
                    `INSERT INTO backup_restore_logs (backup_id, action, status, message) VALUES ($1, $2, $3, $4)`,
                    [backupId, 'restore', 'success', `Safe UPSERT Restored: ${restoreResults.success.length} tables, ${totalProcessedRows} rows (pre-backup: ${preRestoreBackupName})`]
                );
            } catch { /* ignore logging errors */ }

            backupProgress.set(restoreId, {
                status: 'completed',
                progress: 100,
                message: '✅ Database restored successfully!',
                result: {
                    success: restoreResults.success,
                    failed: restoreResults.failed,
                    skipped: restoreResults.skipped,
                    totalRows: totalProcessedRows,
                    fileName: backup.file_name,
                    preRestoreBackup: preRestoreBackupName,
                    mode: 'safe_upsert'
                }
            });

            console.log(`✅ Safe UPSERT restore completed: ${restoreResults.success.length} tables, ${totalProcessedRows} rows`);

        } else {
            backupProgress.set(restoreId, {
                status: 'failed',
                progress: 0,
                message: 'SQL restore not supported. Please use JSON backups.'
            });
        }

        // Clean up progress after 10 minutes
        setTimeout(() => backupProgress.delete(restoreId), 10 * 60 * 1000);

    } catch (error: any) {
        console.error('❌ Streaming restore error:', error);

        try {
            await query(
                `INSERT INTO backup_restore_logs (backup_id, action, status, message) VALUES ($1, $2, $3, $4)`,
                [backupId, 'restore', 'failed', error.message]
            );
        } catch { /* ignore */ }

        backupProgress.set(restoreId, {
            status: 'failed',
            progress: 0,
            message: `Restore failed: ${error.message}`
        });
    } finally {
        if (onComplete) {
            try { onComplete(); } catch { /* ignore cleanup errors */ }
        }
    }
}

/** Read the first N bytes of a backup file (auto-decompresses .gz) */
async function readBackupHeader(filePath: string, bytes: number): Promise<string> {
    if (filePath.endsWith('.gz')) {
        return new Promise((resolve, reject) => {
            let resolved = false;
            const chunks: Buffer[] = [];
            let totalLen = 0;
            const source = fs.createReadStream(filePath);
            const gunzip = zlib.createGunzip();
            source.pipe(gunzip);
            gunzip.on('data', (chunk: Buffer) => {
                if (resolved) return;
                chunks.push(chunk);
                totalLen += chunk.length;
                if (totalLen >= bytes) {
                    resolved = true;
                    // Resolve IMMEDIATELY — don't wait for close/end which may never fire after destroy
                    resolve(Buffer.concat(chunks).toString('utf8').slice(0, bytes));
                    try { source.destroy(); } catch { }
                    try { gunzip.destroy(); } catch { }
                }
            });
            gunzip.on('end', () => {
                if (!resolved) { resolved = true; resolve(Buffer.concat(chunks).toString('utf8').slice(0, bytes)); }
            });
            gunzip.on('close', () => {
                if (!resolved) { resolved = true; resolve(Buffer.concat(chunks).toString('utf8').slice(0, bytes)); }
            });
            gunzip.on('error', (err) => {
                try { source.destroy(); } catch { }
                if (!resolved) {
                    resolved = true;
                    chunks.length > 0
                        ? resolve(Buffer.concat(chunks).toString('utf8').slice(0, bytes))
                        : reject(err);
                }
            });
            // Safety timeout — 15 seconds max
            setTimeout(() => {
                if (!resolved) {
                    resolved = true;
                    try { source.destroy(); } catch { }
                    try { gunzip.destroy(); } catch { }
                    chunks.length > 0
                        ? resolve(Buffer.concat(chunks).toString('utf8').slice(0, bytes))
                        : reject(new Error('readBackupHeader: timeout after 15s'));
                }
            }, 15000);
        });
    }
    const fd = fs.openSync(filePath, 'r');
    const buf = Buffer.alloc(bytes);
    fs.readSync(fd, buf, 0, bytes, 0);
    fs.closeSync(fd);
    return buf.toString('utf8');
}

/**
 * Simple synchronous backup file analysis for preview endpoints.
 * Decompresses the file fully into memory (safe for typical backup sizes < 100 MB),
 * then extracts table names and counts rows per table via brace-counting.
 * No streaming — no risk of promise-never-resolving due to zlib stream issues.
 */
async function getBackupTableCounts(filePath: string): Promise<{ tables: string[]; counts: Record<string, number> }> {
    console.time('getBackupTableCounts');
    let jsonStr: string;
    if (filePath.endsWith('.gz')) {
        const compressed = await fsPromises.readFile(filePath);
        const decompressed = zlib.gunzipSync(compressed);
        jsonStr = decompressed.toString('utf8');
        console.log(`📊 Preview: decompressed ${(compressed.length / 1024).toFixed(0)} KB → ${(decompressed.length / 1024).toFixed(0)} KB`);
    } else {
        jsonStr = await fsPromises.readFile(filePath, 'utf8');
    }

    // Extract table names from metadata header
    const tablesMatch = jsonStr.match(/"tables"\s*:\s*\[(.*?)\]/s);
    let tables: string[] = [];
    if (tablesMatch) {
        try { tables = JSON.parse(`[${tablesMatch[1]}]`); } catch { }
    }
    if (tables.length === 0) {
        tables = ['warehouses', 'customers', 'racks', 'master_data', 'inbound', 'qc', 'picking', 'outbound'];
    }

    // Count rows per table via brace-counting (no JSON.parse of entire file)
    const counts: Record<string, number> = {};
    for (const t of tables) {
        // Search for '"tableName": [' or '"tableName":[' (flexible whitespace)
        const escaped = t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const markerRegex = new RegExp(`"${escaped}"\\s*:\\s*\\[`);
        const match = markerRegex.exec(jsonStr);
        if (!match) { counts[t] = 0; continue; }

        let pos = match.index + match[0].length;
        let count = 0;
        let depth = 0;
        while (pos < jsonStr.length) {
            const ch = jsonStr[pos];
            if (ch === '{') {
                if (depth === 0) count++;
                depth++;
            } else if (ch === '}') {
                depth--;
            } else if (ch === ']' && depth === 0) {
                break;
            }
            pos++;
        }
        counts[t] = count;
    }

    console.timeEnd('getBackupTableCounts');
    return { tables, counts };
}

/**
 * Fast in-memory table parser — extracts rows from an already-decompressed JSON string.
 * No file I/O, no streaming — just string search + brace matching + JSON.parse per object.
 * Used by processRestoreAsync after the single-decompress step.
 */
function parseTableFromString(jsonStr: string, tableName: string): any[] {
    const rows: any[] = [];

    // Find the table's array start — handle flexible whitespace
    const escaped = tableName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const markerRegex = new RegExp(`"${escaped}"\\s*:\\s*\\[`);
    const match = markerRegex.exec(jsonStr);
    if (!match) return rows;

    let pos = match.index + match[0].length;

    // Check for error object pattern like {"error": "..."}  instead of array of row objects
    // Skip whitespace first
    while (pos < jsonStr.length && (jsonStr[pos] === ' ' || jsonStr[pos] === '\n' || jsonStr[pos] === '\r' || jsonStr[pos] === '\t')) pos++;

    while (pos < jsonStr.length) {
        const ch = jsonStr[pos];

        if (ch === '{') {
            // Find matching closing brace
            let depth = 1;
            let j = pos + 1;
            while (j < jsonStr.length && depth > 0) {
                if (jsonStr[j] === '{') depth++;
                else if (jsonStr[j] === '}') depth--;
                j++;
            }
            if (depth === 0) {
                const objStr = jsonStr.substring(pos, j);
                try {
                    const obj = JSON.parse(objStr);
                    // Skip error objects like {"error": "relation does not exist"}
                    if (!obj.error || Object.keys(obj).length > 1) {
                        rows.push(obj);
                    }
                } catch { /* skip malformed */ }
                pos = j;
            } else {
                break; // incomplete — shouldn't happen with fully loaded string
            }
        } else if (ch === ']') {
            break; // end of array
        } else {
            pos++; // skip commas, whitespace, newlines
        }
    }

    return rows;
}

// Stream parse a specific table's data from backup file - MEMORY EFFICIENT
// Uses line-by-line parsing to extract table data without loading entire file
// Supports both .json and .json.gz files
async function streamParseTableData(filePath: string, tableName: string): Promise<any[]> {
    return new Promise((resolve, reject) => {
        let resolved = false;
        const rows: any[] = [];
        let buffer = '';
        let inTargetTable = false;
        let foundStart = false;

        const rawStream = fs.createReadStream(filePath, { highWaterMark: 128 * 1024 });
        const decompressStream = filePath.endsWith('.gz') ? zlib.createGunzip() : null;
        const readStream = decompressStream ? rawStream.pipe(decompressStream) : rawStream;
        readStream.setEncoding('utf8');

        const done = (result: any[]) => {
            if (resolved) return;
            resolved = true;
            try { rawStream.destroy(); } catch { }
            try { if (decompressStream) decompressStream.destroy(); } catch { }
            resolve(result);
        };

        readStream.on('data', (chunk: Buffer | string) => {
            if (resolved) return;
            const chunkStr = typeof chunk === 'string' ? chunk : chunk.toString('utf8');
            buffer += chunkStr;

            // Look for our table's start — handle flexible whitespace
            if (!foundStart) {
                // Try both '"tableName": [' and '"tableName":['
                const markerRegex = new RegExp(`"${tableName.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&')}"\\s*:\\s*\\[`);
                const markerMatch = markerRegex.exec(buffer);
                if (markerMatch) {
                    foundStart = true;
                    inTargetTable = true;
                    buffer = buffer.substring(markerMatch.index + markerMatch[0].length);
                }
            }

            if (inTargetTable) {
                // Parse JSON objects from buffer
                let i = 0;
                while (i < buffer.length) {
                    const char = buffer[i];

                    if (char === '{') {
                        // Find matching closing brace
                        let braceCount = 1;
                        let j = i + 1;
                        while (j < buffer.length && braceCount > 0) {
                            if (buffer[j] === '{') braceCount++;
                            else if (buffer[j] === '}') braceCount--;
                            j++;
                        }

                        if (braceCount === 0) {
                            // Found complete object
                            const objStr = buffer.substring(i, j);
                            try {
                                const obj = JSON.parse(objStr);
                                rows.push(obj);
                            } catch (e) {
                                // Skip malformed objects
                            }
                            i = j;
                            continue;
                        } else {
                            // Incomplete object - wait for more data
                            buffer = buffer.substring(i);
                            break;
                        }
                    } else if (char === ']') {
                        // End of array - we're done with this table
                        inTargetTable = false;
                        done(rows);
                        return;
                    }
                    i++;
                }

                // Keep only unparsed data in buffer
                if (inTargetTable && i >= buffer.length) {
                    buffer = '';
                }
            }

            // Memory safety - if buffer too large and not in target, clear it
            if (!inTargetTable && buffer.length > 1024 * 1024) {
                buffer = buffer.substring(buffer.length - 10000); // Keep last 10KB for context
            }
        });

        readStream.on('end', () => done(rows));
        readStream.on('error', (err: Error) => { if (!resolved) { resolved = true; reject(err); } });
        readStream.on('close', () => done(rows));

        // Safety timeout: 5 minutes per table (prevents infinite hang)
        setTimeout(() => {
            if (!resolved) {
                console.warn(`⚠️ streamParseTableData timeout for ${tableName} after 5 minutes (${rows.length} rows parsed so far)`);
                done(rows);
            }
        }, 5 * 60 * 1000);
    });
}

/**
 * SINGLE-PASS: Parse ALL specified tables' data from a backup file in one read.
 * Avoids reading + decompressing the file N times (once per table).
 */
async function streamParseAllTables(filePath: string, tableNames: string[]): Promise<Record<string, any[]>> {
    return new Promise((resolve, reject) => {
        const result: Record<string, any[]> = {};
        const tableSet = new Set(tableNames);
        for (const t of tableNames) result[t] = [];

        let buffer = '';
        let currentTable: string | null = null;
        let tablesFound = 0;

        const rawStream = fs.createReadStream(filePath, { highWaterMark: 256 * 1024 });
        const decompressStream = filePath.endsWith('.gz') ? zlib.createGunzip() : null;
        const readStream = decompressStream ? rawStream.pipe(decompressStream) : rawStream;
        readStream.setEncoding('utf8');

        readStream.on('data', (chunk: Buffer | string) => {
            const chunkStr = typeof chunk === 'string' ? chunk : chunk.toString('utf8');
            buffer += chunkStr;

            // eslint-disable-next-line no-constant-condition
            while (true) {
                if (!currentTable) {
                    // Look for the start of any target table
                    let earliestIdx = -1;
                    let earliestTable = '';
                    for (const tName of tableSet) {
                        if (result[tName] && result[tName].length > 0) continue; // already parsed — but could have 0 rows legitimately, skip only found-and-closed
                        const marker = `"${tName}": [`;
                        const idx = buffer.indexOf(marker);
                        if (idx !== -1 && (earliestIdx === -1 || idx < earliestIdx)) {
                            earliestIdx = idx;
                            earliestTable = tName;
                        }
                    }

                    if (earliestIdx === -1) {
                        // No table start found in buffer — keep tail for partial matches
                        if (buffer.length > 50000) {
                            buffer = buffer.substring(buffer.length - 10000);
                        }
                        break;
                    }

                    currentTable = earliestTable;
                    const marker = `"${earliestTable}": [`;
                    buffer = buffer.substring(earliestIdx + marker.length);
                }

                // We are inside a table array — parse JSON objects
                let i = 0;
                let parsedSomething = false;

                while (i < buffer.length) {
                    const char = buffer[i];

                    if (char === '{') {
                        let braceCount = 1;
                        let j = i + 1;
                        while (j < buffer.length && braceCount > 0) {
                            if (buffer[j] === '{') braceCount++;
                            else if (buffer[j] === '}') braceCount--;
                            j++;
                        }

                        if (braceCount === 0) {
                            const objStr = buffer.substring(i, j);
                            try {
                                const obj = JSON.parse(objStr);
                                result[currentTable!].push(obj);
                            } catch { /* skip malformed */ }
                            i = j;
                            parsedSomething = true;
                            continue;
                        } else {
                            // Incomplete object — trim buffer and wait for more data
                            buffer = buffer.substring(i);
                            break;
                        }
                    } else if (char === ']') {
                        // End of array — table done
                        tablesFound++;
                        buffer = buffer.substring(i + 1);
                        currentTable = null;
                        parsedSomething = true;

                        // If all tables found, stop early
                        if (tablesFound >= tableNames.length) {
                            rawStream.destroy();
                            if (decompressStream) decompressStream.destroy();
                            resolve(result);
                            return;
                        }
                        break; // Go back to outer loop to find next table
                    }
                    i++;
                }

                if (!parsedSomething) {
                    // Nothing was parsed in this iteration — need more data
                    if (currentTable && i >= buffer.length) {
                        buffer = '';
                    }
                    break;
                }
            }
        });

        readStream.on('end', () => resolve(result));
        readStream.on('error', (err: Error) => reject(err));
        readStream.on('close', () => resolve(result));
    });
}

/**
 * SINGLE-PASS: Count rows per table without storing them in memory.
 * Used for preview/comparison — much faster and lower memory than parsing full rows.
 */
async function streamCountAllTables(filePath: string, tableNames: string[]): Promise<Record<string, number>> {
    return new Promise((resolve, reject) => {
        const counts: Record<string, number> = {};
        const tableSet = new Set(tableNames);
        for (const t of tableNames) counts[t] = 0;

        let buffer = '';
        let currentTable: string | null = null;
        let tablesFound = 0;

        const rawStream = fs.createReadStream(filePath, { highWaterMark: 256 * 1024 });
        const decompressStream = filePath.endsWith('.gz') ? zlib.createGunzip() : null;
        const readStream = decompressStream ? rawStream.pipe(decompressStream) : rawStream;
        readStream.setEncoding('utf8');

        readStream.on('data', (chunk: Buffer | string) => {
            const chunkStr = typeof chunk === 'string' ? chunk : chunk.toString('utf8');
            buffer += chunkStr;

            // eslint-disable-next-line no-constant-condition
            while (true) {
                if (!currentTable) {
                    let earliestIdx = -1;
                    let earliestTable = '';
                    for (const tName of tableSet) {
                        const marker = `"${tName}": [`;
                        const idx = buffer.indexOf(marker);
                        if (idx !== -1 && (earliestIdx === -1 || idx < earliestIdx)) {
                            earliestIdx = idx;
                            earliestTable = tName;
                        }
                    }

                    if (earliestIdx === -1) {
                        if (buffer.length > 50000) {
                            buffer = buffer.substring(buffer.length - 10000);
                        }
                        break;
                    }

                    currentTable = earliestTable;
                    tableSet.delete(earliestTable); // Don't search for it again
                    const marker = `"${earliestTable}": [`;
                    buffer = buffer.substring(earliestIdx + marker.length);
                }

                // Count JSON objects by tracking braces — don't parse them
                let i = 0;
                let counted = false;

                while (i < buffer.length) {
                    const char = buffer[i];

                    if (char === '{') {
                        let braceCount = 1;
                        let j = i + 1;
                        while (j < buffer.length && braceCount > 0) {
                            if (buffer[j] === '{') braceCount++;
                            else if (buffer[j] === '}') braceCount--;
                            j++;
                        }

                        if (braceCount === 0) {
                            counts[currentTable!]++;
                            i = j;
                            counted = true;
                            continue;
                        } else {
                            buffer = buffer.substring(i);
                            break;
                        }
                    } else if (char === ']') {
                        tablesFound++;
                        buffer = buffer.substring(i + 1);
                        currentTable = null;
                        counted = true;

                        if (tablesFound >= tableNames.length) {
                            rawStream.destroy();
                            if (decompressStream) decompressStream.destroy();
                            resolve(counts);
                            return;
                        }
                        break;
                    }
                    i++;
                }

                if (!counted) {
                    if (currentTable && i >= buffer.length) {
                        buffer = '';
                    }
                    break;
                }
            }
        });

        readStream.on('end', () => resolve(counts));
        readStream.on('error', (err: Error) => reject(err));
        readStream.on('close', () => resolve(counts));
    });
}

// ================= UPLOAD PREVIEW (parse uploaded file for table counts) =================
export const uploadPreview = async (req: Request, res: Response) => {
    try {
        const file = (req as any).file;
        if (!file) {
            return res.status(400).json({ error: 'No file uploaded.' });
        }

        const filePath = file.path;

        try {
            console.log(`📋 Upload preview: analyzing ${file.originalname} (${filePath})`);

            // Use simple sync decompression + brace-counting — no streaming
            const { tables, counts: backupCounts } = await getBackupTableCounts(filePath);
            const safeTables = tables.filter((t: string) => isValidTableName(t) && !RESTORE_EXCLUDED_TABLES.has(t));

            // Get DB counts in parallel
            const tableComparison: { table: string; backupRows: number; dbRows: number }[] = [];
            await Promise.all(safeTables.map(async (tableName) => {
                try {
                    const safeTableName = getSafeTableName(tableName);
                    const dbResult = await query(`SELECT COUNT(*) FROM ${safeTableName}`);
                    const dbCount = parseInt(dbResult.rows[0].count, 10);
                    tableComparison.push({ table: tableName, backupRows: backupCounts[tableName] || 0, dbRows: dbCount });
                } catch {
                    tableComparison.push({ table: tableName, backupRows: backupCounts[tableName] || 0, dbRows: 0 });
                }
            }));

            tableComparison.sort((a, b) => safeTables.indexOf(a.table) - safeTables.indexOf(b.table));

            res.json({
                success: true,
                fileName: file.originalname,
                filePath: filePath,
                tables: tableComparison
            });
        } catch (err: any) {
            // Clean up temp file on error
            try { if (fs.existsSync(filePath)) fs.unlinkSync(filePath); } catch { }
            throw err;
        }
    } catch (error: any) {
        console.error('❌ Upload preview error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ================= GET RESTORE LOGS =================
export const getRestoreLogs = async (req: Request, res: Response) => {
    try {
        const result = await query(
            `SELECT 
        l.*,
        b.file_name
      FROM backup_restore_logs l
      LEFT JOIN backups b ON l.backup_id = b.id
      ORDER BY l.created_at DESC
      LIMIT 50`
        );

        res.json(result.rows);
    } catch (error: any) {
        console.error('❌ Get restore logs error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ================= EXPORT BACKUP AS JSON =================
export const exportAsJSON = async (req: Request, res: Response) => {
    try {
        const { tables } = req.body; // Array of table names to export

        const exportData: any = {
            export_date: new Date().toISOString(),
            database: 'wms_database',
            tables: {}
        };

        const tablesToExport = tables && tables.length > 0
            ? tables
            : [
                'warehouses', 'users', 'customers', 'master_data',
                'inbound', 'qc', 'picking', 'outbound', 'racks'
            ];

        for (const tableName of tablesToExport) {
            // 🔐 SECURITY: Validate table name before SQL
            if (!isValidTableName(tableName)) {
                console.warn(`⚠️ Skipping invalid table name: ${tableName}`);
                continue;
            }
            const safeTableName = getSafeTableName(tableName);
            try {
                const result = await query(`SELECT * FROM ${safeTableName}`);
                exportData.tables[safeTableName] = result.rows;
            } catch (err) {
                console.warn(`⚠️ Could not export table ${tableName}:`, err);
            }
        }

        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Disposition', `attachment; filename=wms_backup_${Date.now()}.json`);
        res.send(JSON.stringify(exportData, null, 2));

    } catch (error: any) {
        console.error('❌ JSON export error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ================= GET DATABASE STATISTICS =================
export const getDatabaseStats = async (req: Request, res: Response) => {
    try {
        const result = await query(`
      SELECT 
        schemaname as schema,
        tablename as table_name,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
        pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
      FROM pg_tables
      WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
      ORDER BY size_bytes DESC
    `);

        const totalSize = await query(`
      SELECT pg_size_pretty(pg_database_size(current_database())) as total_size
    `);

        res.json({
            tables: result.rows,
            total_database_size: totalSize.rows[0].total_size
        });

    } catch (error: any) {
        console.error('❌ Database stats error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ================= BACKUP SCHEDULES MANAGEMENT =================

// Get all backup schedules
export const getAllSchedules = async (req: Request, res: Response) => {
    try {
        const result = await query(
            `SELECT 
                id, name, frequency, backup_type, description, enabled,
                time_of_day, day_of_week, day_of_month, retention_days,
                last_run_at, next_run_at, created_at, updated_at
            FROM backup_schedules 
            ORDER BY enabled DESC, id DESC`
        );

        res.json(result.rows);
    } catch (error: any) {
        console.error('❌ Get schedules error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// Create new backup schedule
export const createSchedule = async (req: Request, res: Response) => {
    try {
        const {
            name,
            frequency,
            backup_type = 'full',
            description,
            enabled = true,
            time_of_day = '02:00:00',
            day_of_week = 0,
            day_of_month = 1,
            retention_days = 30,
            selected_tables = null
        } = req.body;
        const user = (req as any).user;

        if (!name || !frequency) {
            return res.status(400).json({ error: 'Name and frequency are required' });
        }

        // Validate selective backup has modules selected
        if (backup_type === 'selective' && (!selected_tables || selected_tables.length === 0)) {
            return res.status(400).json({ error: 'Selective backup requires at least one module to be selected' });
        }

        const result = await query(
            `INSERT INTO backup_schedules (
                name, frequency, backup_type, description, enabled,
                time_of_day, day_of_week, day_of_month, retention_days, created_by, selected_tables
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING *`,
            [name, frequency, backup_type, description, enabled,
                time_of_day, day_of_week, day_of_month, retention_days, user?.id || null,
                backup_type === 'selective' ? selected_tables : null]
        );

        const schedule = result.rows[0];

        // Schedule the backup if enabled
        if (enabled) {
            await backupScheduler.reloadSchedule(schedule.id);
        }

        res.json({
            success: true,
            message: 'Backup schedule created successfully',
            schedule
        });
    } catch (error: any) {
        console.error('❌ Create schedule error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// Update backup schedule
export const updateSchedule = async (req: Request, res: Response) => {
    try {
        const { id } = req.params;
        const {
            name,
            frequency,
            backup_type,
            description,
            enabled,
            time_of_day,
            day_of_week,
            day_of_month,
            retention_days
        } = req.body;

        const result = await query(
            `UPDATE backup_schedules SET
                name = COALESCE($1, name),
                frequency = COALESCE($2, frequency),
                backup_type = COALESCE($3, backup_type),
                description = COALESCE($4, description),
                enabled = COALESCE($5, enabled),
                time_of_day = COALESCE($6, time_of_day),
                day_of_week = COALESCE($7, day_of_week),
                day_of_month = COALESCE($8, day_of_month),
                retention_days = COALESCE($9, retention_days),
                updated_at = NOW()
            WHERE id = $10
            RETURNING *`,
            [name, frequency, backup_type, description, enabled,
                time_of_day, day_of_week, day_of_month, retention_days, id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Schedule not found' });
        }

        // Reload the schedule
        await backupScheduler.reloadSchedule(parseInt(id));

        res.json({
            success: true,
            message: 'Schedule updated successfully',
            schedule: result.rows[0]
        });
    } catch (error: any) {
        console.error('❌ Update schedule error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// Delete backup schedule
export const deleteSchedule = async (req: Request, res: Response) => {
    try {
        const { id } = req.params;

        // Cancel the scheduled job
        backupScheduler.cancelSchedule(parseInt(id));

        // Delete from database
        const result = await query(
            'DELETE FROM backup_schedules WHERE id = $1 RETURNING id',
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Schedule not found' });
        }

        res.json({
            success: true,
            message: 'Schedule deleted successfully'
        });
    } catch (error: any) {
        console.error('❌ Delete schedule error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// Toggle schedule enabled/disabled
export const toggleSchedule = async (req: Request, res: Response) => {
    try {
        const { id } = req.params;
        const { enabled } = req.body;

        const result = await query(
            `UPDATE backup_schedules SET enabled = $1, updated_at = NOW() 
             WHERE id = $2 RETURNING *`,
            [enabled, id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Schedule not found' });
        }

        // Reload the schedule
        await backupScheduler.reloadSchedule(parseInt(id));

        res.json({
            success: true,
            message: `Schedule ${enabled ? 'enabled' : 'disabled'} successfully`,
            schedule: result.rows[0]
        });
    } catch (error: any) {
        console.error('❌ Toggle schedule error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// Get backup health statistics
export const getHealthStats = async (req: Request, res: Response) => {
    try {
        // Update stats first
        await query('SELECT update_backup_health_stats()');

        // Get stats
        const result = await query('SELECT * FROM backup_health_stats WHERE id = 1');

        if (result.rows.length === 0) {
            return res.json({
                total_backups: 0,
                successful_backups: 0,
                failed_backups: 0,
                last_backup_at: null,
                last_backup_status: null,
                last_backup_size: 0,
                total_storage_used: 0,
                average_backup_size: 0,
                success_rate: 0
            });
        }

        const stats = result.rows[0];

        // Add formatted sizes
        const formatBytes = (bytes: number) => {
            if (bytes === 0) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
        };

        res.json({
            ...stats,
            total_storage_used_formatted: formatBytes(stats.total_storage_used || 0),
            average_backup_size_formatted: formatBytes(stats.average_backup_size || 0),
            last_backup_size_formatted: formatBytes(stats.last_backup_size || 0)
        });
    } catch (error: any) {
        console.error('❌ Get health stats error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// Get scheduler status
export const getSchedulerStatus = async (req: Request, res: Response) => {
    try {
        const status = backupScheduler.getStatus();
        res.json(status);
    } catch (error: any) {
        console.error('❌ Get scheduler status error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// Manual trigger of scheduled backup
export const triggerScheduledBackup = async (req: Request, res: Response) => {
    try {
        const { id } = req.params;

        const result = await query(
            'SELECT * FROM backup_schedules WHERE id = $1',
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Schedule not found' });
        }

        const schedule = result.rows[0];

        // Create backup immediately
        const backupResult = await createJSONBackup({
            includeUsers: schedule.backup_type === 'full'
        });

        // Save backup metadata
        await query(
            `INSERT INTO backups (
                file_name, file_path, file_size, backup_type, description, created_by
            ) VALUES ($1, $2, $3, $4, $5, $6)`,
            [
                backupResult.fileName,
                backupResult.filePath,
                backupResult.fileSize,
                'json',
                `${schedule.description || 'Manual trigger'} (${schedule.name})`,
                (req as any).user?.id || null
            ]
        );

        res.json({
            success: true,
            message: 'Backup triggered successfully',
            backup: backupResult
        });
    } catch (error: any) {
        console.error('❌ Trigger scheduled backup error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ========== TIMELINE / POINT-IN-TIME RESTORE APIs ==========

/**
 * GET /backups/timeline?days=7
 * Returns backup history + change log summary grouped by day
 * for building a timeline UI like Supabase PITR or Google Sheets version history.
 */
export const getTimeline = async (req: Request, res: Response) => {
    try {
        const days = Math.min(parseInt(req.query.days as string) || 7, 30);
        const warehouseId = req.query.warehouseId as string;

        // Get all backups within the date range
        const backupsResult = await query(
            `SELECT id, file_name, file_size, backup_type, description, created_by, created_at
             FROM backups
             WHERE created_at >= NOW() - ($1 || ' days')::interval
             ORDER BY created_at DESC`,
            [days]
        );

        // Get change log summary grouped by hour
        let changeLogQuery = `
            SELECT 
                date_trunc('hour', changed_at) as hour,
                table_name,
                operation,
                COUNT(*)::int as count,
                COUNT(DISTINCT changed_by)::int as unique_users,
                MIN(changed_at) as first_change,
                MAX(changed_at) as last_change
            FROM data_change_log
            WHERE changed_at >= NOW() - ($1 || ' days')::interval
        `;
        const changeLogParams: any[] = [days];

        if (warehouseId) {
            changeLogQuery += ` AND warehouse_id = $2`;
            changeLogParams.push(warehouseId);
        }

        changeLogQuery += `
            GROUP BY date_trunc('hour', changed_at), table_name, operation
            ORDER BY hour DESC
        `;

        const changeLogResult = await query(changeLogQuery, changeLogParams);

        // Get daily summary stats
        let dailySummaryQuery = `
            SELECT 
                date_trunc('day', changed_at)::date as day,
                COUNT(*)::int as total_changes,
                COUNT(DISTINCT table_name)::int as tables_affected,
                COUNT(DISTINCT changed_by)::int as unique_users,
                jsonb_object_agg(
                    COALESCE(operation, 'UNKNOWN'),
                    op_count
                ) as operations
            FROM (
                SELECT 
                    changed_at, table_name, changed_by, operation,
                    COUNT(*)::int as op_count
                FROM data_change_log
                WHERE changed_at >= NOW() - ($1 || ' days')::interval
        `;
        const dailyParams: any[] = [days];

        if (warehouseId) {
            dailySummaryQuery += ` AND warehouse_id = $2`;
            dailyParams.push(warehouseId);
        }

        dailySummaryQuery += `
                GROUP BY changed_at, table_name, changed_by, operation
            ) sub
            GROUP BY date_trunc('day', changed_at)::date
            ORDER BY day DESC
        `;

        const dailySummaryResult = await query(dailySummaryQuery, dailyParams);

        // Get total change count
        let totalQuery = `SELECT COUNT(*)::int as total FROM data_change_log WHERE changed_at >= NOW() - ($1 || ' days')::interval`;
        const totalParams: any[] = [days];
        if (warehouseId) {
            totalQuery += ` AND warehouse_id = $2`;
            totalParams.push(warehouseId);
        }
        const totalResult = await query(totalQuery, totalParams);

        res.json({
            days,
            totalBackups: backupsResult.rows.length,
            totalChanges: totalResult.rows[0]?.total || 0,
            backups: backupsResult.rows,
            changeLogByHour: changeLogResult.rows,
            dailySummary: dailySummaryResult.rows
        });

    } catch (error: any) {
        console.error('❌ Get timeline error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * GET /backups/change-log?since=ISO_DATE&limit=100&table=inbound&operation=INSERT
 * Returns detailed change log entries for the timeline feed.
 */
export const getChangeLog = async (req: Request, res: Response) => {
    try {
        const since = req.query.since as string;
        const until = req.query.until as string;
        const table = req.query.table as string;
        const operation = req.query.operation as string;
        const warehouseId = req.query.warehouseId as string;
        const limit = Math.min(parseInt(req.query.limit as string) || 100, 500);
        const offset = parseInt(req.query.offset as string) || 0;

        let sql = `
            SELECT id, table_name, operation, record_id, record_wsn,
                   batch_id, changed_by, changed_by_name, warehouse_id, changed_at
            FROM data_change_log
            WHERE 1=1
        `;
        const params: any[] = [];
        let paramIdx = 1;

        if (since) {
            sql += ` AND changed_at >= $${paramIdx++}`;
            params.push(since);
        }
        if (until) {
            sql += ` AND changed_at <= $${paramIdx++}`;
            params.push(until);
        }
        if (table) {
            sql += ` AND table_name = $${paramIdx++}`;
            params.push(table);
        }
        if (operation) {
            sql += ` AND operation = $${paramIdx++}`;
            params.push(operation.toUpperCase());
        }
        if (warehouseId) {
            sql += ` AND warehouse_id = $${paramIdx++}`;
            params.push(warehouseId);
        }

        // Get total count for pagination
        const countSql = sql.replace(
            /SELECT .+? FROM/,
            'SELECT COUNT(*)::int as total FROM'
        );
        const countResult = await query(countSql, params);

        sql += ` ORDER BY changed_at DESC LIMIT $${paramIdx++} OFFSET $${paramIdx++}`;
        params.push(limit, offset);

        const result = await query(sql, params);

        res.json({
            total: countResult.rows[0]?.total || 0,
            limit,
            offset,
            data: result.rows
        });

    } catch (error: any) {
        console.error('❌ Get change log error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * GET /backups/change-log/:id/detail
 * Returns the full old_data and new_data for a single change log entry.
 */
export const getChangeLogDetail = async (req: Request, res: Response) => {
    try {
        const { id } = req.params;

        const result = await query(
            `SELECT * FROM data_change_log WHERE id = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Change log entry not found' });
        }

        res.json(result.rows[0]);

    } catch (error: any) {
        console.error('❌ Get change log detail error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * GET /backups/:id/preview
 * Returns a preview of what data a backup contains (table names + record counts).
 */
export const getBackupPreview = async (req: Request, res: Response) => {
    try {
        const { id } = req.params;

        // Get backup metadata
        const backupResult = await query(
            'SELECT * FROM backups WHERE id = $1',
            [id]
        );

        if (backupResult.rows.length === 0) {
            return res.status(404).json({ error: 'Backup not found' });
        }

        const backup = backupResult.rows[0];
        let filePath = backup.file_path;

        // If file not on disk (e.g. Render ephemeral FS), try downloading from Supabase Storage
        if (!fs.existsSync(filePath)) {
            if (isSupabaseStorageConfigured()) {
                const tempDir = path.join(__dirname, '../../tmp');
                if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });
                const tempPath = path.join(tempDir, backup.file_name);
                const downloaded = await downloadFromSupabase(backup.file_name, tempPath, STORAGE_BUCKETS.BACKUPS);
                if (downloaded && fs.existsSync(tempPath)) {
                    filePath = tempPath;
                } else {
                    return res.status(404).json({ error: 'Backup file not found on disk or in cloud storage' });
                }
            } else {
                return res.status(404).json({ error: 'Backup file not found on disk' });
            }
        }

        // Stream-parse the backup file to extract table names and record counts
        // without loading entire file into memory (backup files can be 18MB+)
        const preview: { table: string; count: number }[] = [];
        let metadata: any = {};

        await new Promise<void>((resolve, reject) => {
            const inputStream: NodeJS.ReadableStream = filePath.endsWith('.gz')
                ? fs.createReadStream(filePath).pipe(zlib.createGunzip())
                : fs.createReadStream(filePath, { encoding: 'utf8' });

            const rl = readline.createInterface({
                input: inputStream,
                crlfDelay: Infinity
            });

            let currentTable: string | null = null;
            let rowCount = 0;
            let metadataLines: string[] = [];
            let inMetadata = false;
            let metadataBraceDepth = 0;

            rl.on('line', (line: string) => {
                const trimmed = line.trim();

                // Detect "metadata": { ... } block
                if (trimmed.startsWith('"metadata"')) {
                    inMetadata = true;
                    metadataBraceDepth = 0;
                    // Extract from first {
                    const braceStart = line.indexOf('{');
                    if (braceStart !== -1) {
                        metadataLines.push(line.substring(braceStart));
                        for (const ch of line.substring(braceStart)) {
                            if (ch === '{') metadataBraceDepth++;
                            if (ch === '}') metadataBraceDepth--;
                        }
                        if (metadataBraceDepth <= 0) {
                            inMetadata = false;
                            try {
                                let raw = metadataLines.join('\n');
                                // Remove trailing comma
                                raw = raw.replace(/,\s*$/, '');
                                metadata = JSON.parse(raw);
                            } catch { /* ignore parse error */ }
                        }
                    }
                    return;
                }

                if (inMetadata) {
                    metadataLines.push(line);
                    for (const ch of line) {
                        if (ch === '{') metadataBraceDepth++;
                        if (ch === '}') metadataBraceDepth--;
                    }
                    if (metadataBraceDepth <= 0) {
                        inMetadata = false;
                        try {
                            let raw = metadataLines.join('\n');
                            raw = raw.replace(/,\s*$/, '');
                            metadata = JSON.parse(raw);
                        } catch { /* ignore parse error */ }
                    }
                    return;
                }

                // Detect table key: "tableName": [
                const tableMatch = trimmed.match(/^"([a-z_]+)"\s*:\s*\[/);
                if (tableMatch) {
                    // Save previous table if any
                    if (currentTable) {
                        preview.push({ table: currentTable, count: rowCount });
                    }
                    currentTable = tableMatch[1];
                    rowCount = 0;
                    return;
                }

                // Detect end of array: ] or ],
                if (currentTable && (trimmed === ']' || trimmed === '],')) {
                    preview.push({ table: currentTable, count: rowCount });
                    currentTable = null;
                    return;
                }

                // Count data rows (lines starting with { are records)
                if (currentTable && trimmed.startsWith('{')) {
                    rowCount++;
                }
            });

            rl.on('close', () => {
                // Handle last table if file didn't end cleanly
                if (currentTable && rowCount > 0) {
                    preview.push({ table: currentTable, count: rowCount });
                }
                resolve();
            });

            rl.on('error', reject);
        });

        res.json({
            id: backup.id,
            fileName: backup.file_name,
            fileSize: backup.file_size,
            createdAt: backup.created_at,
            description: backup.description,
            backupType: backup.backup_type,
            metadata,
            tables: preview,
            totalRecords: preview.reduce((sum, t) => sum + t.count, 0)
        });

    } catch (error: any) {
        console.error('❌ Get backup preview error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

// ========== AUTO-BACKUP SETTINGS API ==========

/**
 * GET /backups/auto-settings
 * Returns the current auto-backup configuration.
 */
export const getAutoBackupSettings = async (req: Request, res: Response) => {
    try {
        const settings = await backupScheduler.getSettings(true);
        res.json(settings);
    } catch (error: any) {
        console.error('❌ Get auto-backup settings error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * PUT /backups/auto-settings
 * Update auto-backup configuration (frequency, threshold, retention, on/off).
 */
export const updateAutoBackupSettings = async (req: Request, res: Response) => {
    try {
        const userId = (req as any).user?.userId;

        const allowedFields = [
            'enabled', 'frequency', 'min_change_threshold',
            'event_backup_enabled', 'event_throttle_minutes',
            'retention_keep_all_hours', 'retention_daily_days',
            'retention_weekly_days', 'retention_delete_after_days',
        ];

        // Only pick allowed fields from body
        const updates: Record<string, any> = {};
        for (const key of allowedFields) {
            if (req.body[key] !== undefined) {
                updates[key] = req.body[key];
            }
        }

        if (Object.keys(updates).length === 0) {
            return res.status(400).json({ error: 'No valid fields to update' });
        }

        // Validate frequency
        if (updates.frequency && !['1h', '2h', '4h', '6h', '12h', '24h'].includes(updates.frequency)) {
            return res.status(400).json({ error: 'Invalid frequency. Allowed: 1h, 2h, 4h, 6h, 12h, 24h' });
        }

        // Validate numeric fields
        const numericFields = [
            'min_change_threshold', 'event_throttle_minutes',
            'retention_keep_all_hours', 'retention_daily_days',
            'retention_weekly_days', 'retention_delete_after_days',
        ];
        for (const key of numericFields) {
            if (updates[key] !== undefined) {
                const val = Number(updates[key]);
                if (isNaN(val) || val < 0) {
                    return res.status(400).json({ error: `${key} must be a non-negative number` });
                }
                updates[key] = val;
            }
        }

        const newSettings = await backupScheduler.updateSettings(updates, userId);

        // Log the settings change
        await query(
            `INSERT INTO backup_restore_logs (action, status, message) VALUES ($1, $2, $3)`,
            ['settings', 'success', `Auto-backup settings updated: ${JSON.stringify(updates)}`]
        ).catch(() => { });

        res.json({ message: 'Settings updated successfully', settings: newSettings });
    } catch (error: any) {
        console.error('❌ Update auto-backup settings error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};

/**
 * POST /backups/run-retention-cleanup
 * Manually trigger the tiered retention cleanup.
 */
export const runRetentionCleanup = async (req: Request, res: Response) => {
    try {
        await backupScheduler.tieredRetentionCleanup();
        res.json({ message: 'Tiered retention cleanup completed' });
    } catch (error: any) {
        console.error('❌ Run retention cleanup error:', error);
        res.status(500).json({ error: safeError(error) });
    }
};
