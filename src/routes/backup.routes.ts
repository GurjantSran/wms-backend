// File Path = warehouse-backend/src/routes/backup.routes.ts
import express, { Router } from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { authMiddleware } from '../middleware/auth.middleware';
import { backupTimeout } from '../middleware/timeout.middleware';
import { sensitiveWriteRateLimit } from '../middleware/rateLimit.middleware';
import { requirePermissionOrRole } from '../middleware/rbac.middleware';
import * as backupController from '../controllers/backup.controller';

// Multer config for backup file uploads (temp directory)
const backupTempDir = path.join(__dirname, '../../backups/temp');
if (!fs.existsSync(backupTempDir)) {
    fs.mkdirSync(backupTempDir, { recursive: true });
}
const backupUpload = multer({
    storage: multer.diskStorage({
        destination: (req, file, cb) => cb(null, backupTempDir),
        filename: (req, file, cb) => {
            const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
            cb(null, `upload-${uniqueSuffix}${path.extname(file.originalname)}`);
        }
    }),
    limits: { fileSize: 500 * 1024 * 1024 }, // 500MB limit
    fileFilter: (req, file, cb) => {
        const name = file.originalname.toLowerCase();
        if (name.endsWith('.json') || name.endsWith('.json.gz') || name.endsWith('.gz')) {
            cb(null, true);
        } else {
            cb(new Error('Only .json and .json.gz backup files are accepted'));
        }
    }
});

// Permission-based access: allows users with 'menu:settings:backups' override OR admin role
const canAccessBackups = requirePermissionOrRole('menu:settings:backups', 'admin');

const router: Router = express.Router();

// All backup routes require authentication
router.use(authMiddleware);
// NOTE: sensitiveWriteRateLimit applied per-route to CUD operations only.
// Read/GET endpoints use only the global API limiter.

// ========== ALL BACKUP OPERATIONS REQUIRE ADMIN/SUPER_ADMIN ==========
// Security: Backups contain full database including PII and password hashes

// Create new backup (async mode - no timeout needed)
router.post(
    '/',
    sensitiveWriteRateLimit,
    canAccessBackups,
    backupController.createBackup
);

// Check backup progress (for async backups)
router.get(
    '/progress/:backupId',
    canAccessBackups,
    backupController.getBackupProgress
);

// Get all backups
router.get(
    '/',
    canAccessBackups,
    backupController.getAllBackups
);

// Get database statistics
router.get(
    '/stats',
    canAccessBackups,
    backupController.getDatabaseStats
);

// ========== TIMELINE / POINT-IN-TIME RESTORE ROUTES ==========

// Get backup timeline (backups + change log summary grouped by day)
router.get(
    '/timeline',
    canAccessBackups,
    backupController.getTimeline
);

// Get change log entries (paginated, filterable)
router.get(
    '/change-log',
    canAccessBackups,
    backupController.getChangeLog
);

// Get change log entry detail (full old_data + new_data)
router.get(
    '/change-log/:id/detail',
    canAccessBackups,
    backupController.getChangeLogDetail
);

// Get backup preview (table names + record counts)
router.get(
    '/preview/:id',
    canAccessBackups,
    backupController.getBackupPreview
);

// Get restore preview with comparison (backup rows vs DB rows)
router.get(
    '/restore-preview/:id',
    canAccessBackups,
    backupController.getRestorePreviewCompare
);

// Get restore logs
router.get(
    '/restore-logs',
    canAccessBackups,
    backupController.getRestoreLogs
);

// Download backup file
router.get(
    '/download/:id',
    canAccessBackups,
    backupController.downloadBackup
);

// Restore latest backup from cloud storage
router.post(
    '/restore-latest-cloud',
    sensitiveWriteRateLimit,
    backupTimeout,
    canAccessBackups,
    backupController.restoreLatestFromCloud
);

// Cloud preview (download latest from cloud + return table comparison)
router.post(
    '/cloud-preview',
    backupTimeout,
    canAccessBackups,
    backupController.cloudPreview
);

// Upload & preview (parse uploaded file for table/row comparison — no restore yet)
router.post(
    '/upload-preview',
    backupTimeout,
    canAccessBackups,
    backupUpload.single('backupFile'),
    backupController.uploadPreview
);

// Upload & restore from local backup file
router.post(
    '/upload-restore',
    sensitiveWriteRateLimit,
    backupTimeout,
    canAccessBackups,
    backupUpload.single('backupFile'),
    backupController.uploadAndRestore
);

// Restore database from backup - extended timeout
router.post(
    '/restore/:id',
    sensitiveWriteRateLimit,
    backupTimeout,
    canAccessBackups,
    backupController.restoreBackup
);

// Delete backup
router.delete(
    '/:id',
    sensitiveWriteRateLimit,
    canAccessBackups,
    backupController.deleteBackup
);

// Bulk delete backups
router.post(
    '/bulk-delete',
    sensitiveWriteRateLimit,
    canAccessBackups,
    backupController.bulkDeleteBackups
);

// Selective backup (specific tables)
router.post(
    '/selective',
    sensitiveWriteRateLimit,
    canAccessBackups,
    backupController.createSelectiveBackup
);

// Export as JSON (extended timeout)
router.post(
    '/export-json',
    sensitiveWriteRateLimit,
    backupTimeout,
    canAccessBackups,
    backupController.exportAsJSON
);

// ========== SCHEDULED BACKUP ROUTES (admin/super_admin only) ==========

// Get backup health statistics
router.get(
    '/health/stats',
    canAccessBackups,
    backupController.getHealthStats
);

// Get scheduler status
router.get(
    '/scheduler/status',
    canAccessBackups,
    backupController.getSchedulerStatus
);

// ========== AUTO-BACKUP SETTINGS ROUTES ==========

// Get auto-backup settings
router.get(
    '/auto-settings',
    canAccessBackups,
    backupController.getAutoBackupSettings
);

// Update auto-backup settings
router.put(
    '/auto-settings',
    sensitiveWriteRateLimit,
    canAccessBackups,
    backupController.updateAutoBackupSettings
);

// Manually trigger tiered retention cleanup
router.post(
    '/run-retention-cleanup',
    sensitiveWriteRateLimit,
    canAccessBackups,
    backupController.runRetentionCleanup
);

// Get all schedules
router.get(
    '/schedules',
    canAccessBackups,
    backupController.getAllSchedules
);

// Create new schedule
router.post(
    '/schedules',
    sensitiveWriteRateLimit,
    canAccessBackups,
    backupController.createSchedule
);

// Update schedule
router.put(
    '/schedules/:id',
    sensitiveWriteRateLimit,
    canAccessBackups,
    backupController.updateSchedule
);

// Delete schedule
router.delete(
    '/schedules/:id',
    sensitiveWriteRateLimit,
    canAccessBackups,
    backupController.deleteSchedule
);

// Toggle schedule enabled/disabled
router.patch(
    '/schedules/:id/toggle',
    sensitiveWriteRateLimit,
    canAccessBackups,
    backupController.toggleSchedule
);

// Manually trigger a scheduled backup
router.post(
    '/schedules/:id/trigger',
    sensitiveWriteRateLimit,
    canAccessBackups,
    backupController.triggerScheduledBackup
);

export default router;
