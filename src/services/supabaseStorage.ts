// File Path = wms_backend/src/services/supabaseStorage.ts
/**
 * Supabase Storage Service
 * Handles file uploads/downloads for backups and print agent
 */

import { SupabaseClient } from '@supabase/supabase-js';
import { getSupabase, isSupabaseConfigured } from '../config/supabase';
import fs from 'fs';
import logger from '../utils/logger';

// Storage bucket names
export const STORAGE_BUCKETS = {
    BACKUPS: 'backups',
    DOWNLOADS: 'downloads',
    UPLOADS: 'uploads'
} as const;

// Use singleton Supabase client - returns null if not configured
const getSupabaseClient = (): SupabaseClient | null => {
    if (!isSupabaseConfigured()) {
        logger.warn('Supabase credentials not configured. Storage features will be disabled.');
        return null;
    }
    return getSupabase();
};

/**
 * Check if Supabase Storage is configured
 */
export const isSupabaseStorageConfigured = (): boolean => {
    return !!(
        process.env.SUPABASE_URL &&
        process.env.SUPABASE_SERVICE_ROLE_KEY
    );
};

/**
 * Upload file to Supabase Storage
 */
export const uploadToSupabase = async (
    filePath: string,
    fileName: string,
    bucket: string = STORAGE_BUCKETS.BACKUPS
): Promise<boolean> => {
    try {
        const supabase = getSupabaseClient();
        if (!supabase) {
            logger.debug('Supabase not configured, skipping cloud upload');
            return false;
        }

        // Read file
        const fileContent = fs.readFileSync(filePath);

        // Determine content type
        const contentType = fileName.endsWith('.json')
            ? 'application/json'
            : fileName.endsWith('.sql')
                ? 'application/sql'
                : 'application/octet-stream';

        // Upload to Supabase Storage
        const { data, error } = await supabase.storage
            .from(bucket)
            .upload(fileName, fileContent, {
                contentType,
                upsert: true // Overwrite if exists
            });

        if (error) {
            logger.error('Supabase Storage upload failed', { error: error.message, fileName });
            return false;
        }

        logger.info('File uploaded to Supabase Storage', { fileName, bucket, path: data?.path });
        return true;

    } catch (error: any) {
        logger.error('Supabase upload error', { error: error.message });
        return false;
    }
};

/**
 * Delete file from Supabase Storage
 */
export const deleteFromSupabase = async (
    fileName: string,
    bucket: string = STORAGE_BUCKETS.BACKUPS
): Promise<boolean> => {
    try {
        const supabase = getSupabaseClient();
        if (!supabase) return false;

        const { error } = await supabase.storage
            .from(bucket)
            .remove([fileName]);

        if (error) {
            logger.error('Supabase Storage delete failed', { error: error.message, fileName });
            return false;
        }

        logger.info('File deleted from Supabase Storage', { fileName, bucket });
        return true;

    } catch (error: any) {
        logger.error('Supabase delete error', { error: error.message });
        return false;
    }
};

/**
 * Download file from Supabase Storage
 */
export const downloadFromSupabase = async (
    fileName: string,
    destinationPath: string,
    bucket: string = STORAGE_BUCKETS.BACKUPS
): Promise<boolean> => {
    try {
        const supabase = getSupabaseClient();
        if (!supabase) return false;

        const { data, error } = await supabase.storage
            .from(bucket)
            .download(fileName);

        if (error) {
            logger.error('Supabase Storage download failed', { error: error.message, fileName });
            return false;
        }

        if (data) {
            const buffer = Buffer.from(await data.arrayBuffer());
            fs.writeFileSync(destinationPath, buffer);
            logger.info('File downloaded from Supabase Storage', { fileName, destinationPath });
            return true;
        }

        return false;

    } catch (error: any) {
        logger.error('Supabase download error', { error: error.message });
        return false;
    }
};

/**
 * Get public URL for a file (for public buckets like downloads)
 */
export const getPublicUrl = (
    fileName: string,
    bucket: string = STORAGE_BUCKETS.DOWNLOADS
): string | null => {
    const supabase = getSupabaseClient();
    if (!supabase) return null;

    const { data } = supabase.storage
        .from(bucket)
        .getPublicUrl(fileName);

    return data?.publicUrl || null;
};

/**
 * Get signed URL for private files (for private buckets like backups)
 */
export const getSignedUrl = async (
    fileName: string,
    bucket: string = STORAGE_BUCKETS.BACKUPS,
    expiresIn: number = 3600 // 1 hour default
): Promise<string | null> => {
    try {
        const supabase = getSupabaseClient();
        if (!supabase) return null;

        const { data, error } = await supabase.storage
            .from(bucket)
            .createSignedUrl(fileName, expiresIn);

        if (error) {
            logger.error('Failed to create signed URL', { error: error.message, fileName });
            return null;
        }

        return data?.signedUrl || null;

    } catch (error: any) {
        logger.error('Signed URL error', { error: error.message });
        return null;
    }
};

/**
 * List files in a bucket
 */
export const listFiles = async (
    bucket: string = STORAGE_BUCKETS.BACKUPS,
    folder: string = ''
): Promise<string[]> => {
    try {
        const supabase = getSupabaseClient();
        if (!supabase) return [];

        const { data, error } = await supabase.storage
            .from(bucket)
            .list(folder, {
                sortBy: { column: 'created_at', order: 'desc' }
            });

        if (error) {
            logger.error('Failed to list files', { error: error.message, bucket });
            return [];
        }

        return data?.map(file => file.name) || [];

    } catch (error: any) {
        logger.error('List files error', { error: error.message });
        return [];
    }
};

/**
 * Stream file from Supabase Storage (for large files like print agent)
 */
export const streamFromSupabase = async (
    fileName: string,
    bucket: string = STORAGE_BUCKETS.DOWNLOADS
): Promise<{ data: Blob | null; error: any }> => {
    try {
        const supabase = getSupabaseClient();
        if (!supabase) {
            return { data: null, error: { message: 'Supabase not configured' } };
        }

        const { data, error } = await supabase.storage
            .from(bucket)
            .download(fileName);

        return { data, error };

    } catch (error: any) {
        return { data: null, error };
    }
};

/**
 * Upload a Buffer directly to Supabase Storage (no local file required)
 */
export const uploadBufferToSupabase = async (
    buffer: Buffer,
    fileName: string,
    bucket: string,
    contentType: string = 'application/octet-stream'
): Promise<boolean> => {
    try {
        const supabase = getSupabaseClient();
        if (!supabase) {
            logger.debug('Supabase not configured, skipping cloud upload');
            return false;
        }

        const { data, error } = await supabase.storage
            .from(bucket)
            .upload(fileName, buffer, {
                contentType,
                upsert: true
            });

        if (error) {
            logger.error('Supabase buffer upload failed', { error: error.message, fileName, bucket });
            return false;
        }

        logger.info('Buffer uploaded to Supabase Storage', { fileName, bucket, path: data?.path });
        return true;

    } catch (error: any) {
        logger.error('Supabase buffer upload error', { error: error.message });
        return false;
    }
};

/**
 * Ensure a storage bucket exists, create if not
 */
export const ensureBucketExists = async (
    bucketName: string,
    isPublic: boolean = false
): Promise<boolean> => {
    try {
        const supabase = getSupabaseClient();
        if (!supabase) return false;

        // Try to get bucket info first
        const { error: getError } = await supabase.storage.getBucket(bucketName);
        if (!getError) return true; // Bucket already exists

        // Create the bucket
        const { error: createError } = await supabase.storage.createBucket(bucketName, {
            public: isPublic,
            fileSizeLimit: 100 * 1024 * 1024 // 100MB
        });

        if (createError) {
            // Ignore "already exists" errors (race condition)
            if (createError.message?.includes('already exists')) return true;
            logger.error('Failed to create bucket', { error: createError.message, bucketName });
            return false;
        }

        logger.info('Storage bucket created', { bucketName, isPublic });
        return true;

    } catch (error: any) {
        logger.error('Ensure bucket error', { error: error.message });
        return false;
    }
};
