// File Path = warehouse-backend/src/utils/supabaseBackup.ts
/**
 * Supabase-friendly backup utilities
 * This provides JSON-based backups that work without pg_dump
 * Uses streaming to handle large datasets (2-5GB+)
 */

import { query } from '../config/database';
import fs from 'fs';
import path from 'path';
import zlib from 'zlib';
import { pipeline } from 'stream/promises';

const BACKUP_DIR = path.join(__dirname, '../../backups');

// Ensure backup directory exists
if (!fs.existsSync(BACKUP_DIR)) {
    fs.mkdirSync(BACKUP_DIR, { recursive: true });
}

// 🔐 SECURITY: Whitelist of allowed table names to prevent SQL injection
const ALLOWED_BACKUP_TABLES = new Set([
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
    // Error tracking
    'error_logs',
    // Change tracking (CCTV-style)
    'data_change_log'
]);

/**
 * Validate table name against whitelist to prevent SQL injection
 */
export function isValidTableName(tableName: string): boolean {
    return ALLOWED_BACKUP_TABLES.has(tableName.toLowerCase());
}

/**
 * Get sanitized table name or throw error
 */
function getSafeTableName(tableName: string): string {
    const normalizedName = tableName.toLowerCase().trim();
    if (!ALLOWED_BACKUP_TABLES.has(normalizedName)) {
        throw new Error(`Invalid table name: ${tableName}`);
    }
    return normalizedName;
}

interface BackupOptions {
    tables?: string[];
    warehouseId?: number;
    includeUsers?: boolean;
    onProgress?: (table: string, current: number, total: number) => void;
}

/**
 * Create a JSON backup of the database using STREAMING
 * Writes directly to file to handle 2-5GB+ data without memory issues
 */
export async function createJSONBackup(options: BackupOptions = {}) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const backupFileName = `wms_backup_json_${timestamp}.json`;
    const backupFilePath = path.join(BACKUP_DIR, backupFileName);

    const defaultTables = [
        // Core business data (CRITICAL)
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
        // RBAC & permissions
        'roles',
        'permissions',
        'role_permissions',
        'user_permissions',
        'user_permission_overrides',
        'user_warehouses',
        'role_ui_access',
        'user_ui_overrides',
        // Permission approvals
        'permission_change_requests',
        'permission_change_details',
        // Upload history
        'upload_logs',
        'batch_snapshots',
    ];

    if (options.includeUsers) {
        defaultTables.push('users', 'active_sessions', 'login_history');
    }

    const tablesToBackup = options.tables || defaultTables;
    const CHUNK_SIZE = 5000; // Smaller chunks for memory efficiency

    console.log('📦 Creating streaming JSON backup...');

    // Create write stream for memory-efficient file writing
    const writeStream = fs.createWriteStream(backupFilePath, { encoding: 'utf8' });

    // Write metadata header
    const metadata = {
        backup_date: new Date().toISOString(),
        database: 'wms',
        version: '2.0',
        tables: tablesToBackup,
        warehouse_id: options.warehouseId || 'all'
    };

    writeStream.write('{\n');
    writeStream.write(`"metadata": ${JSON.stringify(metadata, null, 2)},\n`);
    writeStream.write('"data": {\n');

    let tableIndex = 0;
    const tableStats: Record<string, number> = {};

    for (const tableName of tablesToBackup) {
        try {
            // 🔐 SECURITY: Validate table name against whitelist
            const safeTableName = getSafeTableName(tableName);

            // Get total count first
            let countSql = `SELECT COUNT(*) FROM ${safeTableName}`;
            const countParams: any[] = [];

            if (options.warehouseId &&
                ['inbound', 'qc', 'picking', 'outbound', 'racks'].includes(tableName)) {
                countSql += ` WHERE warehouse_id = $1`;
                countParams.push(options.warehouseId);
            }

            const countResult = await query(countSql, countParams.length > 0 ? countParams : undefined);
            const totalRows = parseInt(countResult.rows[0].count);

            console.log(`  📊 ${tableName}: ${totalRows} rows`);

            // Write table start
            if (tableIndex > 0) writeStream.write(',\n');
            writeStream.write(`"${tableName}": [\n`);

            let offset = 0;
            let rowsWritten = 0;

            while (offset < totalRows || totalRows === 0) {
                let sql = `SELECT * FROM ${safeTableName}`;
                const params: any[] = [];

                if (options.warehouseId &&
                    ['inbound', 'qc', 'picking', 'outbound', 'racks'].includes(safeTableName)) {
                    sql += ` WHERE warehouse_id = $1`;
                    params.push(options.warehouseId);
                }

                sql += ` ORDER BY id LIMIT ${CHUNK_SIZE} OFFSET ${offset}`;

                const result = await query(sql, params.length > 0 ? params : undefined);

                if (result.rows.length === 0) break;

                // Write each row
                for (let i = 0; i < result.rows.length; i++) {
                    if (rowsWritten > 0) writeStream.write(',\n');
                    writeStream.write(JSON.stringify(result.rows[i]));
                    rowsWritten++;
                }

                offset += CHUNK_SIZE;

                // Progress callback
                if (options.onProgress) {
                    options.onProgress(tableName, Math.min(offset, totalRows), totalRows);
                }

                // Log progress for large tables
                if (totalRows > 10000 && offset % 20000 === 0) {
                    console.log(`    Progress: ${Math.min(offset, totalRows)}/${totalRows}`);
                }

                // Allow event loop to breathe (prevents blocking)
                await new Promise(resolve => setImmediate(resolve));
            }

            // Write table end
            writeStream.write('\n]');
            tableStats[tableName] = rowsWritten;
            console.log(`  ✓ ${tableName}: ${rowsWritten} rows exported`);

        } catch (error: any) {
            console.warn(`  ⚠️ Could not backup ${tableName}:`, error.message);
            if (tableIndex > 0) writeStream.write(',\n');
            writeStream.write(`"${tableName}": {"error": "${error.message}"}`);
            tableStats[tableName] = 0;
        }

        tableIndex++;
    }

    // Close JSON structure
    writeStream.write('\n}\n}');

    // Wait for stream to finish
    await new Promise<void>((resolve, reject) => {
        writeStream.end(() => resolve());
        writeStream.on('error', reject);
    });

    // Compress to .gz (saves 70-85% storage)
    const gzFileName = backupFileName + '.gz';
    const gzFilePath = backupFilePath + '.gz';

    console.log('🗜️ Compressing backup with gzip...');
    await pipeline(
        fs.createReadStream(backupFilePath),
        zlib.createGzip({ level: 6 }),
        fs.createWriteStream(gzFilePath)
    );

    // Remove uncompressed file
    fs.unlinkSync(backupFilePath);

    const stats = fs.statSync(gzFilePath);
    const fileSizeInMB = (stats.size / (1024 * 1024)).toFixed(2);

    console.log(`✅ Backup complete: ${gzFileName} (${fileSizeInMB} MB compressed)`);

    return {
        fileName: gzFileName,
        filePath: gzFilePath,
        fileSize: stats.size,
        fileSizeMB: fileSizeInMB,
        tableCount: tablesToBackup.length,
        tableStats
    };
}

/**
 * Create a CSV export of specific table
 */
export async function exportTableAsCSV(tableName: string, warehouseId?: number) {
    // 🔐 SECURITY: Validate table name against whitelist
    const safeTableName = getSafeTableName(tableName);

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const csvFileName = `${safeTableName}_export_${timestamp}.csv`;
    const csvFilePath = path.join(BACKUP_DIR, csvFileName);

    let sql = `SELECT * FROM ${safeTableName}`;
    const params: any[] = [];

    if (warehouseId &&
        ['inbound', 'qc', 'picking', 'outbound', 'racks'].includes(safeTableName)) {
        sql += ` WHERE warehouse_id = $1`;
        params.push(warehouseId);
    }

    const result = await query(sql, params.length > 0 ? params : undefined);

    if (result.rows.length === 0) {
        throw new Error('No data to export');
    }

    // Generate CSV content
    const headers = Object.keys(result.rows[0]);
    const csvContent = [
        headers.join(','),
        ...result.rows.map(row =>
            headers.map(header => {
                const value = row[header];
                // Escape commas and quotes in values
                if (value === null || value === undefined) return '';
                const stringValue = String(value);
                if (stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')) {
                    return `"${stringValue.replace(/"/g, '""')}"`;
                }
                return stringValue;
            }).join(',')
        )
    ].join('\n');

    fs.writeFileSync(csvFilePath, csvContent);

    const stats = fs.statSync(csvFilePath);

    console.log(`✅ CSV export created: ${csvFileName} (${result.rows.length} rows)`);

    return {
        fileName: csvFileName,
        filePath: csvFilePath,
        fileSize: stats.size,
        rowCount: result.rows.length
    };
}

/**
 * Schedule automatic backups (call this from a cron job or scheduler)
 */
export async function scheduleBackup(frequency: 'daily' | 'weekly' = 'daily') {
    try {
        console.log(`🕐 Running scheduled ${frequency} backup...`);

        const backup = await createJSONBackup({
            includeUsers: false // Don't backup passwords in scheduled backups
        });

        // Save backup metadata to database
        await query(
            `INSERT INTO backups (
        file_name, 
        file_path, 
        file_size, 
        backup_type, 
        description
      ) VALUES ($1, $2, $3, $4, $5)`,
            [
                backup.fileName,
                backup.filePath,
                backup.fileSize,
                'scheduled_json',
                `Automated ${frequency} backup`
            ]
        );

        // Clean up old backups (keep last 30 days)
        const retentionDays = 30;
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() - retentionDays);

        const oldBackups = await query(
            `SELECT id, file_path FROM backups 
       WHERE created_at < $1 AND backup_type = 'scheduled_json'`,
            [cutoffDate]
        );

        for (const oldBackup of oldBackups.rows) {
            try {
                if (fs.existsSync(oldBackup.file_path)) {
                    fs.unlinkSync(oldBackup.file_path);
                }
                await query('DELETE FROM backups WHERE id = $1', [oldBackup.id]);
                console.log(`  🗑️ Cleaned up old backup: ${oldBackup.id}`);
            } catch (err) {
                console.warn(`  ⚠️ Failed to clean up backup ${oldBackup.id}`);
            }
        }

        console.log(`✅ Scheduled backup completed successfully`);
        return backup;

    } catch (error: any) {
        console.error('❌ Scheduled backup failed:', error);
        throw error;
    }
}

/**
 * Get database size and statistics
 */
export async function getDatabaseStatistics() {
    try {
        // Get total database size
        const dbSizeResult = await query(`
      SELECT pg_size_pretty(pg_database_size(current_database())) as total_size,
             pg_database_size(current_database()) as total_size_bytes
    `);

        // Get table sizes
        const tableSizesResult = await query(`
      SELECT 
        schemaname as schema,
        tablename as table_name,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
        pg_total_relation_size(schemaname||'.'||tablename) as size_bytes,
        (SELECT COUNT(*) FROM information_schema.tables WHERE table_name = tablename) as row_count_estimate
      FROM pg_tables
      WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
      ORDER BY size_bytes DESC
    `);

        // Get row counts for main tables (only whitelisted tables)
        const tables = ['warehouses', 'users', 'master_data', 'inbound', 'qc', 'picking', 'outbound', 'racks'];
        const rowCounts: any = {};

        for (const table of tables) {
            try {
                // 🔐 SECURITY: Validate table name before SQL
                if (!isValidTableName(table)) continue;
                const safeTable = getSafeTableName(table);
                const countResult = await query(`SELECT COUNT(*) as count FROM ${safeTable}`);
                rowCounts[safeTable] = parseInt(countResult.rows[0].count);
            } catch (err) {
                rowCounts[table] = 0;
            }
        }

        return {
            total_size: dbSizeResult.rows[0].total_size,
            total_size_bytes: dbSizeResult.rows[0].total_size_bytes,
            tables: tableSizesResult.rows,
            row_counts: rowCounts
        };

    } catch (error) {
        console.error('Failed to get database statistics:', error);
        throw error;
    }
}
