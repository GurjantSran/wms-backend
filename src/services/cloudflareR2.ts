// File Path = warehouse-backend/src/services/cloudflareR2.ts
import { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import fs from 'fs';
import logger from '../utils/logger';

// Initialize R2 Client
const getR2Client = () => {
    if (!process.env.CLOUDFLARE_R2_ACCOUNT_ID || !process.env.CLOUDFLARE_R2_ACCESS_KEY || !process.env.CLOUDFLARE_R2_SECRET_KEY) {
        logger.warn('Cloudflare R2 credentials not configured. Backups will be stored locally only.');
        return null;
    }

    return new S3Client({
        region: 'auto',
        endpoint: `https://${process.env.CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
        credentials: {
            accessKeyId: process.env.CLOUDFLARE_R2_ACCESS_KEY,
            secretAccessKey: process.env.CLOUDFLARE_R2_SECRET_KEY,
        },
    });
};

// Upload file to R2
export const uploadToR2 = async (filePath: string, fileName: string): Promise<boolean> => {
    try {
        const r2Client = getR2Client();
        if (!r2Client) {
            logger.debug('R2 not configured, skipping cloud upload');
            return false;
        }

        const bucketName = process.env.CLOUDFLARE_R2_BUCKET || 'wms-backups';

        // Read file
        const fileContent = fs.readFileSync(filePath);

        // Upload to R2
        const command = new PutObjectCommand({
            Bucket: bucketName,
            Key: fileName,
            Body: fileContent,
            ContentType: 'application/json',
        });

        await r2Client.send(command);
        logger.info('Backup uploaded to Cloudflare R2', { fileName });
        return true;

    } catch (error: any) {
        logger.error('R2 upload failed', error);
        return false;
    }
};

// Delete file from R2
export const deleteFromR2 = async (fileName: string): Promise<boolean> => {
    try {
        const r2Client = getR2Client();
        if (!r2Client) return false;

        const bucketName = process.env.CLOUDFLARE_R2_BUCKET || 'wms-backups';

        const command = new DeleteObjectCommand({
            Bucket: bucketName,
            Key: fileName,
        });

        await r2Client.send(command);
        logger.info('Backup deleted from R2', { fileName });
        return true;

    } catch (error: any) {
        logger.error('R2 delete failed', error);
        return false;
    }
};

// Download file from R2
export const downloadFromR2 = async (fileName: string, destinationPath: string): Promise<boolean> => {
    try {
        const r2Client = getR2Client();
        if (!r2Client) return false;

        const bucketName = process.env.CLOUDFLARE_R2_BUCKET || 'wms-backups';

        const command = new GetObjectCommand({
            Bucket: bucketName,
            Key: fileName,
        });

        const response = await r2Client.send(command);

        if (response.Body) {
            const chunks: any[] = [];
            for await (const chunk of response.Body as any) {
                chunks.push(chunk);
            }
            const fileBuffer = Buffer.concat(chunks);
            fs.writeFileSync(destinationPath, fileBuffer);
            logger.info('Backup downloaded from R2', { fileName });
            return true;
        }

        return false;

    } catch (error: any) {
        logger.error('R2 download failed', error);
        return false;
    }
};

// Check if R2 is configured
export const isR2Configured = (): boolean => {
    return !!(
        process.env.CLOUDFLARE_R2_ACCOUNT_ID &&
        process.env.CLOUDFLARE_R2_ACCESS_KEY &&
        process.env.CLOUDFLARE_R2_SECRET_KEY
    );
};
