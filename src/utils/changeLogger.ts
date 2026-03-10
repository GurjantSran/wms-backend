// File Path = warehouse-backend/src/utils/changeLogger.ts
/**
 * CCTV-Style Change Logger
 * 
 * Logs every data_change (INSERT/UPDATE/DELETE) to the data_change_log table
 * for point-in-time recovery. Designed for near-zero performance overhead.
 * 
 * Usage in controllers:
 *   await logChanges(client, 'inbound', 'INSERT', records, meta);
 *   await logChange(client, 'outbound', 'UPDATE', record, meta);
 */

import { PoolClient } from 'pg';
import { query } from '../config/database';

// ============================================================================
// TYPES
// ============================================================================

export interface ChangeRecord {
    id?: number;
    wsn?: string;
    oldData?: Record<string, any> | null;
    newData?: Record<string, any> | null;
}

export interface ChangeMeta {
    batchId?: string;
    userId?: number | null;
    userName?: string | null;
    warehouseId?: number | string | null;
}

// ============================================================================
// BULK CHANGE LOGGING (for multi-entry, bulk uploads)
// ============================================================================

/**
 * Log multiple changes in a single bulk INSERT (very fast).
 * Use this for multi-entry submit, bulk upload, batch delete, etc.
 * 
 * @param client - PostgreSQL client (same transaction)
 * @param tableName - Table that was modified
 * @param operation - 'INSERT' | 'UPDATE' | 'DELETE'
 * @param records - Array of changed records
 * @param meta - User/warehouse context
 */
export async function logChanges(
    client: PoolClient,
    tableName: string,
    operation: 'INSERT' | 'UPDATE' | 'DELETE',
    records: ChangeRecord[],
    meta: ChangeMeta
): Promise<void> {
    if (records.length === 0) return;

    try {
        // Build bulk INSERT with parameterized values
        const BATCH_SIZE = 500; // Insert 500 log rows at a time

        for (let i = 0; i < records.length; i += BATCH_SIZE) {
            const batch = records.slice(i, i + BATCH_SIZE);
            const values: any[] = [];
            const placeholders: string[] = [];

            batch.forEach((record, idx) => {
                const offset = idx * 8;
                placeholders.push(
                    `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8})`
                );
                values.push(
                    tableName,
                    operation,
                    record.id || null,
                    record.wsn || null,
                    record.oldData ? JSON.stringify(record.oldData) : null,
                    record.newData ? JSON.stringify(record.newData) : null,
                    meta.batchId || null,
                    meta.userId || null
                );
            });

            // Add warehouse_id and changed_by_name as extra columns
            // Use a CTE-style insert for consistency
            const sql = `
                INSERT INTO data_change_log 
                    (table_name, operation, record_id, record_wsn, old_data, new_data, batch_id, changed_by, changed_by_name, warehouse_id)
                VALUES ${placeholders.map((p, idx) => {
                // Extend each placeholder to include the 2 extra static columns
                const base = idx * 8;
                return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, '${(meta.userName || '').replace(/'/g, "''")}', ${meta.warehouseId || 'NULL'})`;
            }).join(', ')}
            `;

            // Actually, let's use a cleaner approach with proper parameterization
            const cleanValues: any[] = [];
            const cleanPlaceholders: string[] = [];

            batch.forEach((record, idx) => {
                const offset = idx * 10;
                cleanPlaceholders.push(
                    `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10})`
                );
                cleanValues.push(
                    tableName,
                    operation,
                    record.id || null,
                    record.wsn || null,
                    record.oldData ? JSON.stringify(record.oldData) : null,
                    record.newData ? JSON.stringify(record.newData) : null,
                    meta.batchId || null,
                    meta.userId || null,
                    meta.userName || null,
                    meta.warehouseId || null
                );
            });

            const cleanSql = `
                INSERT INTO data_change_log 
                    (table_name, operation, record_id, record_wsn, old_data, new_data, batch_id, changed_by, changed_by_name, warehouse_id)
                VALUES ${cleanPlaceholders.join(', ')}
            `;

            await client.query(cleanSql, cleanValues);
        }
    } catch (error: any) {
        // Change logging should NEVER break the main operation
        // Log error but don't throw
        console.error(`[ChangeLog] Failed to log ${records.length} ${operation}s on ${tableName}:`, error.message);
    }
}

// ============================================================================
// SINGLE CHANGE LOGGING (for individual create/update/delete)
// ============================================================================

/**
 * Log a single change. Use this for individual CRUD operations.
 * 
 * @param client - PostgreSQL client (same transaction)
 * @param tableName - Table that was modified
 * @param operation - 'INSERT' | 'UPDATE' | 'DELETE'
 * @param record - The changed record
 * @param meta - User/warehouse context
 */
export async function logChange(
    client: PoolClient,
    tableName: string,
    operation: 'INSERT' | 'UPDATE' | 'DELETE',
    record: ChangeRecord,
    meta: ChangeMeta
): Promise<void> {
    try {
        await client.query(
            `INSERT INTO data_change_log 
                (table_name, operation, record_id, record_wsn, old_data, new_data, batch_id, changed_by, changed_by_name, warehouse_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
            [
                tableName,
                operation,
                record.id || null,
                record.wsn || null,
                record.oldData ? JSON.stringify(record.oldData) : null,
                record.newData ? JSON.stringify(record.newData) : null,
                meta.batchId || null,
                meta.userId || null,
                meta.userName || null,
                meta.warehouseId || null
            ]
        );
    } catch (error: any) {
        // Never break the main operation
        console.error(`[ChangeLog] Failed to log ${operation} on ${tableName}:`, error.message);
    }
}

// ============================================================================
// STANDALONE LOGGING (without transaction client - for simple queries)
// ============================================================================

/**
 * Log a change using the shared pool (no transaction client needed).
 * Use this when you don't have a PoolClient (simple query-based operations).
 */
export async function logChangeSimple(
    tableName: string,
    operation: 'INSERT' | 'UPDATE' | 'DELETE',
    record: ChangeRecord,
    meta: ChangeMeta
): Promise<void> {
    try {
        await query(
            `INSERT INTO data_change_log 
                (table_name, operation, record_id, record_wsn, old_data, new_data, batch_id, changed_by, changed_by_name, warehouse_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
            [
                tableName,
                operation,
                record.id || null,
                record.wsn || null,
                record.oldData ? JSON.stringify(record.oldData) : null,
                record.newData ? JSON.stringify(record.newData) : null,
                meta.batchId || null,
                meta.userId || null,
                meta.userName || null,
                meta.warehouseId || null
            ]
        );
    } catch (error: any) {
        console.error(`[ChangeLog] Failed to log ${operation} on ${tableName}:`, error.message);
    }
}

/**
 * Batch version of logChangeSimple — logs multiple records in a single INSERT.
 * Used for bulk deletions to avoid N+1 queries.
 */
export async function logChangeSimpleBatch(
    tableName: string,
    operation: 'INSERT' | 'UPDATE' | 'DELETE',
    records: ChangeRecord[],
    meta: ChangeMeta
): Promise<void> {
    if (records.length === 0) return;
    try {
        const values: any[] = [];
        const placeholders: string[] = [];
        records.forEach((record, idx) => {
            const o = idx * 10;
            placeholders.push(
                `($${o+1}, $${o+2}, $${o+3}, $${o+4}, $${o+5}, $${o+6}, $${o+7}, $${o+8}, $${o+9}, $${o+10})`
            );
            values.push(
                tableName, operation,
                record.id || null, record.wsn || null,
                record.oldData ? JSON.stringify(record.oldData) : null,
                record.newData ? JSON.stringify(record.newData) : null,
                meta.batchId || null, meta.userId || null,
                meta.userName || null, meta.warehouseId || null
            );
        });
        await query(
            `INSERT INTO data_change_log
                (table_name, operation, record_id, record_wsn, old_data, new_data, batch_id, changed_by, changed_by_name, warehouse_id)
             VALUES ${placeholders.join(', ')}`,
            values
        );
    } catch (error: any) {
        console.error(`[ChangeLog] Failed to batch log ${operation} on ${tableName}:`, error.message);
    }
}

// ============================================================================
// UTILITY: Get change count since a timestamp
// ============================================================================

/**
 * Get the number of changes recorded since a given timestamp.
 * Used by auto-backup scheduler to decide if backup is needed.
 */
export async function getChangeCountSince(since: Date): Promise<number> {
    try {
        const result = await query(
            'SELECT COUNT(*)::int as count FROM data_change_log WHERE changed_at > $1',
            [since]
        );
        return result.rows[0]?.count || 0;
    } catch (error: any) {
        console.error('[ChangeLog] Failed to get change count:', error.message);
        return -1; // Return -1 to signal error (caller should do backup anyway)
    }
}

// ============================================================================
// CLEANUP: Remove old change logs
// ============================================================================

/**
 * Delete change logs older than specified days.
 * Called by daily cleanup cron job.
 */
export async function cleanupOldChangeLogs(retentionDays: number = 30): Promise<number> {
    try {
        const result = await query(
            'DELETE FROM data_change_log WHERE changed_at < NOW() - ($1 || \' days\')::interval RETURNING id',
            [retentionDays]
        );
        const deletedCount = result.rows.length;
        if (deletedCount > 0) {
            console.log(`[ChangeLog] 🧹 Cleaned up ${deletedCount} old change log entries (>${retentionDays} days)`);
        }
        return deletedCount;
    } catch (error: any) {
        console.error('[ChangeLog] Cleanup failed:', error.message);
        return 0;
    }
}
