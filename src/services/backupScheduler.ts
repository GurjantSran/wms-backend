// File Path = warehouse-backend/src/services/backupScheduler.ts
import cron from 'node-cron';
import crypto from 'crypto';
import { query } from '../config/database';
import { createJSONBackup } from '../utils/supabaseBackup';
import { uploadToSupabase, isSupabaseStorageConfigured, STORAGE_BUCKETS, deleteFromSupabase } from './supabaseStorage';
import { getChangeCountSince, cleanupOldChangeLogs } from '../utils/changeLogger';
import fs from 'fs';
import path from 'path';
import logger from '../utils/logger';

const BACKUP_DIR = path.join(__dirname, '../../backups');

interface Schedule {
    id: number;
    name: string;
    frequency: string;
    backup_type: string;
    description: string;
    enabled: boolean;
    time_of_day: string;
    day_of_week: number;
    day_of_month: number;
    retention_days: number;
    next_run_at: string;
    selected_tables: string[] | null;
}

interface AutoBackupSettings {
    enabled: boolean;
    frequency: string;           // '1h', '2h', '4h', '6h', '12h', '24h'
    min_change_threshold: number;
    event_backup_enabled: boolean;
    event_throttle_minutes: number;
    retention_keep_all_hours: number;
    retention_daily_days: number;
    retention_weekly_days: number;
    retention_delete_after_days: number;
}

const DEFAULT_SETTINGS: AutoBackupSettings = {
    enabled: true,
    frequency: '1h',
    min_change_threshold: 5,
    event_backup_enabled: true,
    event_throttle_minutes: 5,
    retention_keep_all_hours: 24,
    retention_daily_days: 7,
    retention_weekly_days: 30,
    retention_delete_after_days: 90,
};

/** Convert frequency string to cron expression */
function frequencyToCron(freq: string): string {
    switch (freq) {
        case '1h': return '0 * * * *';
        case '2h': return '0 */2 * * *';
        case '4h': return '0 */4 * * *';
        case '6h': return '0 */6 * * *';
        case '12h': return '0 */12 * * *';
        case '24h': return '0 3 * * *';       // Once a day at 3 AM
        default: return '0 * * * *';
    }
}

/** Convert frequency string to milliseconds */
function frequencyToMs(freq: string): number {
    switch (freq) {
        case '1h': return 60 * 60 * 1000;
        case '2h': return 2 * 60 * 60 * 1000;
        case '4h': return 4 * 60 * 60 * 1000;
        case '6h': return 6 * 60 * 60 * 1000;
        case '12h': return 12 * 60 * 60 * 1000;
        case '24h': return 24 * 60 * 60 * 1000;
        default: return 60 * 60 * 1000;
    }
}

/** Compute SHA256 hash of a file (streaming, memory-efficient) */
function computeFileHash(filePath: string): Promise<string> {
    return new Promise((resolve, reject) => {
        const hash = crypto.createHash('sha256');
        const stream = fs.createReadStream(filePath);
        stream.on('data', (chunk) => hash.update(chunk));
        stream.on('end', () => resolve(hash.digest('hex')));
        stream.on('error', reject);
    });
}

class BackupScheduler {
    private scheduledJobs: Map<number, cron.ScheduledTask> = new Map();
    private runningBackups: Set<number> = new Set(); // Prevent overlapping executions
    private isInitialized = false;
    private autoBackupRunning = false;
    private lastAutoBackupTime: Date | null = null;
    private lastEventBackupTime: Date | null = null;
    private autoBackupCronJob: cron.ScheduledTask | null = null;
    private cachedSettings: AutoBackupSettings = { ...DEFAULT_SETTINGS };
    private settingsLoadedAt: Date | null = null;
    private static SETTINGS_CACHE_MS = 60 * 1000; // Re-read settings every 60s

    // ========== SETTINGS MANAGEMENT ==========

    /** Load auto-backup settings from DB (with caching) */
    async getSettings(forceReload = false): Promise<AutoBackupSettings> {
        if (
            !forceReload &&
            this.settingsLoadedAt &&
            Date.now() - this.settingsLoadedAt.getTime() < BackupScheduler.SETTINGS_CACHE_MS
        ) {
            return this.cachedSettings;
        }
        try {
            const result = await query('SELECT * FROM auto_backup_settings WHERE id = 1');
            if (result.rows.length > 0) {
                const row = result.rows[0];
                this.cachedSettings = {
                    enabled: row.enabled,
                    frequency: row.frequency,
                    min_change_threshold: row.min_change_threshold,
                    event_backup_enabled: row.event_backup_enabled,
                    event_throttle_minutes: row.event_throttle_minutes,
                    retention_keep_all_hours: row.retention_keep_all_hours,
                    retention_daily_days: row.retention_daily_days,
                    retention_weekly_days: row.retention_weekly_days,
                    retention_delete_after_days: row.retention_delete_after_days,
                };
            }
            this.settingsLoadedAt = new Date();
        } catch (err) {
            logger.warn('Failed to load auto_backup_settings, using defaults', err as Error);
        }
        return this.cachedSettings;
    }

    /** Update settings and restart cron if frequency changed */
    async updateSettings(updates: Partial<AutoBackupSettings>, userId?: number): Promise<AutoBackupSettings> {
        const fields: string[] = [];
        const values: any[] = [];
        let idx = 1;

        const allowedKeys: (keyof AutoBackupSettings)[] = [
            'enabled', 'frequency', 'min_change_threshold',
            'event_backup_enabled', 'event_throttle_minutes',
            'retention_keep_all_hours', 'retention_daily_days',
            'retention_weekly_days', 'retention_delete_after_days',
        ];

        for (const key of allowedKeys) {
            if (updates[key] !== undefined) {
                fields.push(`${key} = $${idx}`);
                values.push(updates[key]);
                idx++;
            }
        }

        if (fields.length === 0) return this.getSettings(true);

        fields.push(`updated_at = NOW()`);
        fields.push(`updated_by = $${idx}`);
        values.push(userId || null);

        await query(`UPDATE auto_backup_settings SET ${fields.join(', ')} WHERE id = 1`, values);

        // Force reload
        const newSettings = await this.getSettings(true);

        // If frequency or enabled changed, restart auto-backup cron
        if (updates.frequency !== undefined || updates.enabled !== undefined) {
            this.restartAutoBackupCron(newSettings);
        }

        return newSettings;
    }

    // Initialize scheduler - load all active schedules
    async initialize() {
        if (this.isInitialized) {
            logger.warn('Backup scheduler already initialized');
            return;
        }

        logger.info('Initializing backup scheduler...');

        try {
            // Load settings from DB
            await this.getSettings(true);

            // Load all enabled schedules
            const result = await query(
                'SELECT * FROM backup_schedules WHERE enabled = true'
            );

            for (const schedule of result.rows) {
                this.scheduleBackup(schedule);
            }

            // Start cleanup job (runs daily at 3 AM)
            this.startCleanupJob();

            // Start auto-backup job based on settings
            this.startAutoBackupJob();

            this.isInitialized = true;
            logger.info('Backup scheduler initialized', { activeSchedules: result.rows.length });
        } catch (error: any) {
            logger.error('Failed to initialize backup scheduler', error);
        }
    }

    // Create a cron schedule from schedule config
    private getCronExpression(schedule: Schedule): string {
        const [hour, minute] = schedule.time_of_day.split(':').map(Number);

        switch (schedule.frequency) {
            case 'hourly':
                return `${minute} * * * *`; // Every hour at specified minute

            case 'daily':
                return `${minute} ${hour} * * *`; // Daily at specified time

            case 'weekly':
                return `${minute} ${hour} * * ${schedule.day_of_week}`; // Weekly on specified day

            case 'monthly':
                return `${minute} ${hour} ${schedule.day_of_month} * *`; // Monthly on specified day

            default:
                return `${minute} ${hour} * * *`; // Default to daily
        }
    }

    // Schedule a backup job
    scheduleBackup(schedule: Schedule) {
        try {
            // Cancel existing job if any
            if (this.scheduledJobs.has(schedule.id)) {
                this.scheduledJobs.get(schedule.id)?.stop();
                this.scheduledJobs.delete(schedule.id);
            }

            const cronExpression = this.getCronExpression(schedule);
            logger.info('Scheduling backup', { name: schedule.name, cron: cronExpression });

            const job = cron.schedule(cronExpression, async () => {
                await this.executeScheduledBackup(schedule);
            });

            this.scheduledJobs.set(schedule.id, job);

            // Update next run time
            this.updateNextRunTime(schedule.id);

        } catch (error: any) {
            logger.error('Failed to schedule backup', error, { scheduleId: schedule.id });
        }
    }

    // Execute a scheduled backup
    private async executeScheduledBackup(schedule: Schedule) {
        // Guard: prevent overlapping executions for the same schedule
        if (this.runningBackups.has(schedule.id)) {
            logger.warn('Skipping overlapping backup execution', { name: schedule.name, scheduleId: schedule.id });
            return;
        }
        this.runningBackups.add(schedule.id);

        logger.info('Executing scheduled backup', {
            name: schedule.name,
            type: schedule.backup_type,
            selectedTables: schedule.selected_tables
        });

        try {
            // Prepare backup options based on backup type
            const backupOptions: { tables?: string[]; includeUsers?: boolean } = {};

            if (schedule.backup_type === 'selective' && schedule.selected_tables && schedule.selected_tables.length > 0) {
                // Selective backup: use only the selected tables
                backupOptions.tables = schedule.selected_tables;
                logger.info('Creating selective backup', { tables: schedule.selected_tables });
            } else {
                // Full backup: include users
                backupOptions.includeUsers = schedule.backup_type === 'full';
            }

            // Create JSON backup
            const backupResult = await createJSONBackup(backupOptions);

            // Upload to Supabase Storage (if configured)
            if (isSupabaseStorageConfigured()) {
                await uploadToSupabase(backupResult.filePath, backupResult.fileName, STORAGE_BUCKETS.BACKUPS);
            }

            // Build description based on backup type
            let backupDescription = schedule.description || 'Scheduled backup';
            if (schedule.backup_type === 'selective' && schedule.selected_tables) {
                backupDescription += ` [Modules: ${schedule.selected_tables.join(', ')}]`;
            }
            backupDescription += ` (${schedule.name})`;

            // Save backup metadata to database
            await query(
                `INSERT INTO backups (
                    file_name, 
                    file_path, 
                    file_size, 
                    backup_type, 
                    description, 
                    created_by
                ) VALUES ($1, $2, $3, $4, $5, $6)`,
                [
                    backupResult.fileName,
                    backupResult.filePath,
                    backupResult.fileSize,
                    'json',
                    backupDescription,
                    null // System-generated
                ]
            );

            // Log success
            await query(
                `INSERT INTO backup_restore_logs (action, status, message)
                 VALUES ($1, $2, $3)`,
                ['backup', 'success', `Scheduled backup completed: ${schedule.name}`]
            );

            // Update last run time
            await query(
                'UPDATE backup_schedules SET last_run_at = NOW() WHERE id = $1',
                [schedule.id]
            );

            // Update next run time
            await this.updateNextRunTime(schedule.id);

            logger.info('Scheduled backup completed', { name: schedule.name, fileSizeMB: backupResult.fileSizeMB });

            // Run retention cleanup for this schedule
            await this.cleanupOldBackups(schedule.retention_days);

        } catch (error: any) {
            logger.error('Scheduled backup failed', error, { name: schedule.name });

            // Log failure
            await query(
                `INSERT INTO backup_restore_logs (action, status, message)
                 VALUES ($1, $2, $3)`,
                ['backup', 'failed', `Scheduled backup failed: ${schedule.name} - ${error.message}`]
            );
        } finally {
            this.runningBackups.delete(schedule.id);
        }
    }

    // Update next run time for a schedule
    private async updateNextRunTime(scheduleId: number) {
        try {
            const result = await query(
                'SELECT * FROM backup_schedules WHERE id = $1',
                [scheduleId]
            );

            if (result.rows.length === 0) return;

            const schedule = result.rows[0];
            const cronExpression = this.getCronExpression(schedule);

            // Calculate next run (simplified - just add interval to current time)
            let nextRun = new Date();

            switch (schedule.frequency) {
                case 'hourly':
                    nextRun.setHours(nextRun.getHours() + 1);
                    break;
                case 'daily':
                    nextRun.setDate(nextRun.getDate() + 1);
                    break;
                case 'weekly':
                    nextRun.setDate(nextRun.getDate() + 7);
                    break;
                case 'monthly':
                    nextRun.setMonth(nextRun.getMonth() + 1);
                    break;
            }

            await query(
                'UPDATE backup_schedules SET next_run_at = $1, updated_at = NOW() WHERE id = $2',
                [nextRun, scheduleId]
            );
        } catch (error) {
            logger.error('Failed to update next run time', error as Error);
        }
    }

    // Cancel a scheduled job
    cancelSchedule(scheduleId: number) {
        if (this.scheduledJobs.has(scheduleId)) {
            this.scheduledJobs.get(scheduleId)?.stop();
            this.scheduledJobs.delete(scheduleId);
            logger.info('Cancelled schedule', { scheduleId });
        }
    }

    // Reload a specific schedule
    async reloadSchedule(scheduleId: number) {
        try {
            const result = await query(
                'SELECT * FROM backup_schedules WHERE id = $1',
                [scheduleId]
            );

            if (result.rows.length === 0) {
                logger.error('Schedule not found', undefined, { scheduleId });
                return;
            }

            const schedule = result.rows[0];

            // Cancel existing job
            this.cancelSchedule(scheduleId);

            // Reschedule if enabled
            if (schedule.enabled) {
                this.scheduleBackup(schedule);
            }
        } catch (error: any) {
            logger.error('Failed to reload schedule', error, { scheduleId });
        }
    }

    // Reload all schedules
    async reloadAllSchedules() {
        logger.info('Reloading all backup schedules...');

        // Cancel all existing jobs
        this.scheduledJobs.forEach((job, id) => {
            job.stop();
        });
        this.scheduledJobs.clear();

        // Reload from database
        this.isInitialized = false;
        await this.initialize();
    }

    // Cleanup old backups based on retention policy
    private async cleanupOldBackups(retentionDays: number) {
        if (retentionDays <= 0) return; // 0 means never delete

        try {
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - retentionDays);

            // Get old backups
            const result = await query(
                'SELECT id, file_path FROM backups WHERE created_at < $1',
                [cutoffDate]
            );

            console.log(`🗑️ Found ${result.rows.length} old backup(s) to clean up`);

            const deletedIds: number[] = [];

            for (const backup of result.rows) {
                try {
                    // Delete file from disk
                    if (fs.existsSync(backup.file_path)) {
                        fs.unlinkSync(backup.file_path);
                    }

                    deletedIds.push(backup.id);
                    console.log(`  ✓ Deleted old backup file: ${backup.id}`);
                } catch (err) {
                    console.error(`  ✗ Failed to delete backup ${backup.id}:`, err);
                }
            }

            // BATCH DELETE: Remove all successfully-cleaned records in one query
            if (deletedIds.length > 0) {
                await query('DELETE FROM backups WHERE id = ANY($1)', [deletedIds]);
                console.log(`  ✓ Batch deleted ${deletedIds.length} backup records`);
            }
        } catch (error: any) {
            console.error('Cleanup failed:', error.message);
        }
    }

    // Start daily cleanup job
    private startCleanupJob() {
        // Run at 3 AM every day
        cron.schedule('0 3 * * *', async () => {
            console.log('🧹 Running daily backup cleanup...');

            try {
                // Get all schedules with retention policies
                const result = await query(
                    'SELECT DISTINCT retention_days FROM backup_schedules WHERE retention_days > 0'
                );

                for (const row of result.rows) {
                    await this.cleanupOldBackups(row.retention_days);
                }

                // Run tiered retention cleanup for auto/event backups
                await this.tieredRetentionCleanup();

                // Clean up old change logs (30-day retention)
                await cleanupOldChangeLogs(30);

                console.log('✅ Daily cleanup completed');
            } catch (error) {
                console.error('Daily cleanup failed:', error);
            }
        });

        console.log('🧹 Daily cleanup job scheduled (3:00 AM)');
    }

    // ========== AUTO-BACKUP WITH SMART SKIP + HASH DEDUP ==========

    /**
     * Start the auto-backup cron job based on configured frequency.
     * Runs at the configured interval but skips if changes below threshold.
     */
    startAutoBackupJob() {
        const settings = this.cachedSettings;

        if (!settings.enabled) {
            logger.info('⏸️ Auto-backup is DISABLED via settings');
            console.log('⏸️ Auto-backup is DISABLED via settings');
            return;
        }

        const cronExpr = frequencyToCron(settings.frequency);

        // Stop existing cron if any
        if (this.autoBackupCronJob) {
            this.autoBackupCronJob.stop();
            this.autoBackupCronJob = null;
        }

        this.autoBackupCronJob = cron.schedule(cronExpr, async () => {
            const currentSettings = await this.getSettings();
            if (!currentSettings.enabled) return;
            await this.executeAutoBackup();
        });

        logger.info(`⏰ Auto-backup job scheduled (every ${settings.frequency})`);
        console.log(`⏰ Auto-backup job scheduled (every ${settings.frequency})`);
    }

    /** Restart auto-backup cron when settings change */
    private restartAutoBackupCron(settings: AutoBackupSettings) {
        if (this.autoBackupCronJob) {
            this.autoBackupCronJob.stop();
            this.autoBackupCronJob = null;
        }
        if (settings.enabled) {
            this.startAutoBackupJob();
        } else {
            logger.info('⏸️ Auto-backup disabled');
            console.log('⏸️ Auto-backup disabled');
        }
    }

    /**
     * Execute auto-backup with smart skip + hash-based dedup.
     * 1. Check change count against threshold → skip if below
     * 2. Create backup file
     * 3. Compute SHA256 hash → compare with last backup → skip if identical
     * 4. Upload & save metadata
     */
    private async executeAutoBackup() {
        if (this.autoBackupRunning) {
            logger.warn('Auto-backup already running, skipping');
            return;
        }

        this.autoBackupRunning = true;
        const settings = await this.getSettings();

        try {
            // LAYER 1: Smart Skip — check change count against threshold
            const sinceTime = this.lastAutoBackupTime || new Date(Date.now() - frequencyToMs(settings.frequency));
            const changeCount = await getChangeCountSince(sinceTime);

            if (changeCount < settings.min_change_threshold) {
                logger.info(`⏭️ Auto-backup skipped: only ${changeCount} changes (threshold: ${settings.min_change_threshold})`);
                console.log(`⏭️ Auto-backup skipped: only ${changeCount} changes (threshold: ${settings.min_change_threshold})`);
                return;
            }

            logger.info(`📦 Auto-backup triggered: ${changeCount} changes detected since ${sinceTime.toISOString()}`);
            console.log(`📦 Auto-backup triggered: ${changeCount} changes detected`);

            // Create full backup
            const backupResult = await createJSONBackup({ includeUsers: false });

            // LAYER 2: Hash-based dedup — compute SHA256 and compare with last backup
            const fileHash = await computeFileHash(backupResult.filePath);
            const lastHashResult = await query(
                `SELECT file_hash FROM backups WHERE file_hash IS NOT NULL ORDER BY created_at DESC LIMIT 1`
            );

            if (lastHashResult.rows.length > 0 && lastHashResult.rows[0].file_hash === fileHash) {
                // Identical data — delete the temp file and skip
                try { fs.unlinkSync(backupResult.filePath); } catch (_) { }
                logger.info('⏭️ Auto-backup skipped: identical data (hash match)');
                console.log('⏭️ Auto-backup skipped: identical data (hash match)');

                await query(
                    `INSERT INTO backup_restore_logs (action, status, message) VALUES ($1, $2, $3)`,
                    ['backup', 'skipped', `Auto-backup skipped: identical data (SHA256 hash match), ${changeCount} changes were cosmetic`]
                );
                return;
            }

            // Upload to Supabase Storage (if configured)
            if (isSupabaseStorageConfigured()) {
                await uploadToSupabase(backupResult.filePath, backupResult.fileName, STORAGE_BUCKETS.BACKUPS);
            }

            // Save backup metadata with hash and source
            await query(
                `INSERT INTO backups (file_name, file_path, file_size, backup_type, description, created_by, file_hash, source)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
                [
                    backupResult.fileName,
                    backupResult.filePath,
                    backupResult.fileSize,
                    'json',
                    `Auto-backup (${changeCount} changes detected)`,
                    null,
                    fileHash,
                    'auto'
                ]
            );

            // Log success
            await query(
                `INSERT INTO backup_restore_logs (action, status, message)
                 VALUES ($1, $2, $3)`,
                ['backup', 'success', `Auto-backup completed: ${changeCount} changes, ${backupResult.fileSizeMB}MB`]
            );

            this.lastAutoBackupTime = new Date();
            logger.info(`✅ Auto-backup completed: ${backupResult.fileSizeMB}MB (hash: ${fileHash.slice(0, 12)}...)`);
            console.log(`✅ Auto-backup completed: ${backupResult.fileSizeMB}MB`);

        } catch (error: any) {
            logger.error('Auto-backup failed', error);
            console.error('❌ Auto-backup failed:', error.message);

            await query(
                `INSERT INTO backup_restore_logs (action, status, message)
                 VALUES ($1, $2, $3)`,
                ['backup', 'failed', `Auto-backup failed: ${error.message}`]
            ).catch(() => { });
        } finally {
            this.autoBackupRunning = false;
        }
    }

    // ========== EVENT-TRIGGERED BACKUP ==========

    /**
     * Trigger an event-based backup after significant data operations.
     * Throttled based on settings (default 5 min). Respects enabled flag.
     */
    async triggerEventBackup(reason: string = 'data_event') {
        try {
            const settings = await this.getSettings();

            if (!settings.event_backup_enabled) {
                logger.info('⏭️ Event backup disabled via settings');
                return;
            }

            const throttleMs = settings.event_throttle_minutes * 60 * 1000;

            if (this.lastEventBackupTime) {
                const elapsed = Date.now() - this.lastEventBackupTime.getTime();
                if (elapsed < throttleMs) {
                    logger.info(`⏭️ Event backup throttled (${Math.round((throttleMs - elapsed) / 1000)}s remaining)`);
                    return;
                }
            }

            if (this.autoBackupRunning) {
                logger.info('⏭️ Event backup skipped: auto-backup already running');
                return;
            }

            this.autoBackupRunning = true;
            this.lastEventBackupTime = new Date();

            logger.info(`⚡ Event backup triggered: ${reason}`);
            console.log(`⚡ Event backup triggered: ${reason}`);

            // Create full backup
            const backupResult = await createJSONBackup({ includeUsers: false });

            // Compute hash for event backups too
            const fileHash = await computeFileHash(backupResult.filePath);

            // Upload to Supabase Storage (if configured)
            if (isSupabaseStorageConfigured()) {
                await uploadToSupabase(backupResult.filePath, backupResult.fileName, STORAGE_BUCKETS.BACKUPS);
            }

            // Save backup metadata with hash and source
            await query(
                `INSERT INTO backups (file_name, file_path, file_size, backup_type, description, created_by, file_hash, source)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
                [
                    backupResult.fileName,
                    backupResult.filePath,
                    backupResult.fileSize,
                    'json',
                    `Event backup: ${reason}`,
                    null,
                    fileHash,
                    'event'
                ]
            );

            // Log success
            await query(
                `INSERT INTO backup_restore_logs (action, status, message)
                 VALUES ($1, $2, $3)`,
                ['backup', 'success', `Event backup completed: ${reason}, ${backupResult.fileSizeMB}MB`]
            );

            this.lastAutoBackupTime = new Date(); // Update so auto-backup can skip
            logger.info(`✅ Event backup completed: ${backupResult.fileSizeMB}MB`);

        } catch (error: any) {
            logger.error('Event backup failed', error);
            console.error('❌ Event backup failed:', error.message);

            await query(
                `INSERT INTO backup_restore_logs (action, status, message)
                 VALUES ($1, $2, $3)`,
                ['backup', 'failed', `Event backup failed (${reason}): ${error.message}`]
            ).catch(() => { });
        } finally {
            this.autoBackupRunning = false;
        }
    }

    // ========== TIERED RETENTION CLEANUP ==========

    /**
     * Tiered retention cleanup — runs daily at 3 AM.
     * - Keep ALL backups within retention_keep_all_hours (default 24h)
     * - Keep 1 per DAY within retention_daily_days (default 7d)
     * - Keep 1 per WEEK within retention_weekly_days (default 30d)
     * - DELETE everything older than retention_delete_after_days (default 90d)
     * Only applies to auto/event source backups. Manual & scheduled are untouched.
     */
    async tieredRetentionCleanup() {
        const settings = await this.getSettings();
        logger.info('🧹 Running tiered retention cleanup...');
        console.log('🧹 Running tiered retention cleanup...');

        let totalDeleted = 0;

        try {
            // TIER 4: Delete everything older than retention_delete_after_days
            const hardCutoff = new Date();
            hardCutoff.setDate(hardCutoff.getDate() - settings.retention_delete_after_days);

            const oldResult = await query(
                `SELECT id, file_name FROM backups WHERE source IN ('auto', 'event') AND created_at < $1`,
                [hardCutoff]
            );

            if (oldResult.rows.length > 0) {
                const ids = oldResult.rows.map((r: any) => r.id);
                await this.deleteBackupsByIds(ids, oldResult.rows.map((r: any) => r.file_name));
                totalDeleted += ids.length;
                logger.info(`🗑️ Tier 4: Deleted ${ids.length} backups older than ${settings.retention_delete_after_days} days`);
            }

            // TIER 3: Keep 1 per week between retention_daily_days and retention_weekly_days
            const weeklyCutoffStart = new Date();
            weeklyCutoffStart.setDate(weeklyCutoffStart.getDate() - settings.retention_weekly_days);
            const weeklyCutoffEnd = new Date();
            weeklyCutoffEnd.setDate(weeklyCutoffEnd.getDate() - settings.retention_daily_days);

            const weeklyResult = await query(
                `SELECT id, file_name, created_at,
                        EXTRACT(ISOYEAR FROM created_at) AS yr,
                        EXTRACT(WEEK FROM created_at) AS wk
                 FROM backups
                 WHERE source IN ('auto', 'event')
                   AND created_at >= $1 AND created_at < $2
                 ORDER BY created_at DESC`,
                [weeklyCutoffStart, weeklyCutoffEnd]
            );

            const weeklyKeep = new Set<string>();
            const weeklyDeleteIds: number[] = [];
            const weeklyDeleteNames: string[] = [];
            for (const row of weeklyResult.rows) {
                const key = `${row.yr}-W${row.wk}`;
                if (weeklyKeep.has(key)) {
                    weeklyDeleteIds.push(row.id);
                    weeklyDeleteNames.push(row.file_name);
                } else {
                    weeklyKeep.add(key); // Keep the first (latest) per week
                }
            }

            if (weeklyDeleteIds.length > 0) {
                await this.deleteBackupsByIds(weeklyDeleteIds, weeklyDeleteNames);
                totalDeleted += weeklyDeleteIds.length;
                logger.info(`🗑️ Tier 3: Deleted ${weeklyDeleteIds.length} weekly-excess backups`);
            }

            // TIER 2: Keep 1 per day between retention_keep_all_hours and retention_daily_days
            const dailyCutoffStart = new Date();
            dailyCutoffStart.setDate(dailyCutoffStart.getDate() - settings.retention_daily_days);
            const dailyCutoffEnd = new Date();
            dailyCutoffEnd.setHours(dailyCutoffEnd.getHours() - settings.retention_keep_all_hours);

            const dailyResult = await query(
                `SELECT id, file_name, created_at,
                        created_at::date AS day
                 FROM backups
                 WHERE source IN ('auto', 'event')
                   AND created_at >= $1 AND created_at < $2
                 ORDER BY created_at DESC`,
                [dailyCutoffStart, dailyCutoffEnd]
            );

            const dailyKeep = new Set<string>();
            const dailyDeleteIds: number[] = [];
            const dailyDeleteNames: string[] = [];
            for (const row of dailyResult.rows) {
                const dayStr = row.day instanceof Date ? row.day.toISOString().slice(0, 10) : String(row.day);
                if (dailyKeep.has(dayStr)) {
                    dailyDeleteIds.push(row.id);
                    dailyDeleteNames.push(row.file_name);
                } else {
                    dailyKeep.add(dayStr);
                }
            }

            if (dailyDeleteIds.length > 0) {
                await this.deleteBackupsByIds(dailyDeleteIds, dailyDeleteNames);
                totalDeleted += dailyDeleteIds.length;
                logger.info(`🗑️ Tier 2: Deleted ${dailyDeleteIds.length} daily-excess backups`);
            }

            // TIER 1: keep ALL within retention_keep_all_hours — no action needed

            logger.info(`✅ Tiered retention cleanup done: ${totalDeleted} backups deleted total`);
            console.log(`✅ Tiered retention cleanup done: ${totalDeleted} backups deleted total`);

            if (totalDeleted > 0) {
                await query(
                    `INSERT INTO backup_restore_logs (action, status, message) VALUES ($1, $2, $3)`,
                    ['cleanup', 'success', `Tiered retention: deleted ${totalDeleted} auto/event backups`]
                );
            }
        } catch (error: any) {
            logger.error('Tiered retention cleanup failed', error);
            console.error('❌ Tiered retention cleanup failed:', error.message);
        }
    }

    /** Helper: delete backups from DB + Supabase Storage */
    private async deleteBackupsByIds(ids: number[], fileNames: string[]) {
        // Delete from Supabase Storage
        if (isSupabaseStorageConfigured()) {
            for (const fileName of fileNames) {
                try {
                    await deleteFromSupabase(fileName, STORAGE_BUCKETS.BACKUPS);
                } catch (_) { }
            }
        }

        // Delete from local disk
        for (const fileName of fileNames) {
            const localPath = path.join(BACKUP_DIR, fileName);
            try { if (fs.existsSync(localPath)) fs.unlinkSync(localPath); } catch (_) { }
        }

        // Delete DB records
        await query('DELETE FROM backups WHERE id = ANY($1)', [ids]);
    }

    // Get scheduler status
    getStatus() {
        const s = this.cachedSettings;
        return {
            initialized: this.isInitialized,
            activeJobs: this.scheduledJobs.size,
            schedules: Array.from(this.scheduledJobs.keys()),
            autoBackup: {
                enabled: s.enabled,
                frequency: s.frequency,
                minChangeThreshold: s.min_change_threshold,
                lastRun: this.lastAutoBackupTime?.toISOString() || null,
                running: this.autoBackupRunning
            },
            eventBackup: {
                enabled: s.event_backup_enabled,
                throttleMinutes: s.event_throttle_minutes,
                lastRun: this.lastEventBackupTime?.toISOString() || null
            },
            retention: {
                keepAllHours: s.retention_keep_all_hours,
                dailyDays: s.retention_daily_days,
                weeklyDays: s.retention_weekly_days,
                deleteAfterDays: s.retention_delete_after_days,
            }
        };
    }
}

// Export singleton instance
export const backupScheduler = new BackupScheduler();
