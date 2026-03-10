// File Path = warehouse-backend/src/server.ts
import dotenv from 'dotenv';
import fs from 'fs';
import fsPromises from 'fs/promises';
import path from 'path';

// Load .env from project root or fallback to repo's safe_secrets/.env for local dev (do NOT commit secrets)
const standardEnv = path.join(process.cwd(), '.env');
const safeEnv = path.join(process.cwd(), '..', 'safe_secrets', '.env');
const envPath = fs.existsSync(standardEnv) ? standardEnv : (fs.existsSync(safeEnv) ? safeEnv : undefined);
dotenv.config(envPath ? { path: envPath } : undefined);

import express, { Express, Request, Response } from 'express';
import cors from 'cors';
import compression from 'compression';
import helmet from 'helmet';
import { initializeDatabase } from './config/database';
import authRoutes from './routes/auth.routes';
import warehousesRoutes from './routes/warehouses.routes';
import inboundRoutes from './routes/inbound.routes';
import masterDataRoutes from './routes/master-data.routes';
import usersRoutes from './routes/users.routes';
import rackRoutes from './routes/rack.routes';
import qcRoutes from './routes/qc.routes';
import pickingRoutes from './routes/picking.routes';
import { errorHandler } from './middleware/errorHandler.middleware';
import outboundRoutes from './routes/outbound.routes';
import customerRoutes from './routes/customer.routes';
import dashboardRoutes from './routes/dashboard.routes';
import inventoryRoutes from './routes/inventory.routes';
import reportsRoutes from './routes/reports.routes';
import backupRoutes from './routes/backup.routes';
import permissionsRoutes from './routes/permissions.routes';
import uiAccessRoutes from './routes/ui-access.routes';
import errorLogsRoutes from './routes/error-logs.routes';
import sessionsRoutes from './routes/sessions.routes';
import rejectionRoutes from './routes/rejection.routes';
import liveViewRoutes from './routes/live-view.routes';
import cacheRoutes from './routes/cache.routes';
import eventsRoutes from './routes/events.routes';
import printQueueRoutes from './routes/print-queue.routes';
import logger from './utils/logger';

import { isDbReady, getConnectionHealth, warmupConnection, forceReconnect } from "./config/database";
import { apiTimeout } from './middleware/timeout.middleware';
import { authMiddleware, hasRole } from './middleware/auth.middleware';
import { globalRateLimitWithExclusions } from './middleware/rateLimit.middleware';
import { backupScheduler } from './services/backupScheduler';

// ── Startup Validation ──────────────────────────────────────────────────────
if (!process.env.NODE_ENV) {
  console.warn('[SECURITY] NODE_ENV is not set — defaulting to development. Set NODE_ENV=production in production deployments.');
}
if (process.env.JWT_SECRET && process.env.JWT_SECRET.length < 32) {
  console.warn('[SECURITY] JWT_SECRET is shorter than 32 characters — use a strong, random secret in production.');
}

const app: Express = express();
//const PORT = process.env.PORT || 5000;
const PORT = Number(process.env.PORT) || 3000;

// Parse allowed origins from environment variable or use defaults
const getAllowedOrigins = (): string[] => {
  const envOrigins = process.env.CORS_ORIGINS;
  if (envOrigins) {
    return envOrigins.split(',').map(origin => origin.trim());
  }
  // Default origins - include both production and development
  return [
    'https://ddwms.vercel.app',
    process.env.FRONTEND_URL || '',
    'http://localhost:3000',
    'http://127.0.0.1:3000',
    'http://127.0.0.1:9100'
  ].filter(Boolean);
};

// Handle OPTIONS preflight requests FIRST (before any other middleware)
// Using middleware instead of app.options('*') for Express 5 compatibility
app.use((req, res, next) => {
  // Set CORS headers for ALL requests (including error responses)
  const origin = req.headers.origin;
  const allowedOrigins = getAllowedOrigins();

  if (origin && allowedOrigins.includes(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Access-Control-Allow-Credentials', 'true');
  }

  // Handle OPTIONS preflight
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, PATCH, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-device-id, x-print-agent-key');
    res.setHeader('Access-Control-Max-Age', '86400'); // 24 hours
    return res.status(204).end();
  }
  next();
});

// CORS middleware (backup for non-preflight requests)
app.use(cors({
  origin: getAllowedOrigins(),
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'x-device-id', 'x-print-agent-key'],
}));

// 🔐 SECURITY: Apply Helmet for HTTP security headers
app.use(helmet({
  contentSecurityPolicy: false, // Disable CSP to avoid breaking frontend
  crossOriginEmbedderPolicy: false, // Allow embedding resources
}));

// ⚡ EGRESS OPTIMIZATION: Enhanced compression settings
// Compresses responses > 1KB, reduces egress by 50-70%
app.use(compression({
  level: 6, // Balanced compression (1-9, 6 is good balance of speed/size)
  threshold: 1024, // Only compress responses > 1KB
  filter: (req, res) => {
    // Don't compress if client doesn't accept it
    if (req.headers['x-no-compression']) return false;
    // Skip compression for SSE (long-lived stream, must not buffer)
    if (req.path === '/api/events/subscribe') return false;
    // Use default filter (compresses text, json, etc.)
    return compression.filter(req, res);
  }
}));

// 🚧 Ensure DB is ready before hitting any API (skip check in tests and health/wake endpoints)
app.use((req, res, next) => {
  if (process.env.NODE_ENV === 'test') return next();
  // Skip DB check for health, wake, and reconnect endpoints (used for monitoring and recovery)
  if (req.path === '/api/health' || req.path === '/api/wake' || req.path === '/api/reconnect' || req.path === '/api/events/subscribe') return next();

  if (!isDbReady()) {
    return res.status(503).json({
      error: "Server is starting up",
      message: "The server is currently connecting to the database. Please try again in a moment.",
      isRetryable: true,
      retryAfterMs: 5000,
      timestamp: new Date().toISOString()
    });
  }
  next();
});

// Request timeout (60 seconds for all requests) — skip for SSE (long-lived)
app.use((req, res, next) => {
  if (req.path === '/api/events/subscribe') return next();
  return apiTimeout(req, res, next);
});

// 🔐 SECURITY: Global API rate limiting (300 req/min per IP, excludes heartbeat/health/wake)
app.use(globalRateLimitWithExclusions);

// Body parsers with reasonable limits (10MB default, specific routes can have higher)
app.use(express.urlencoded({ limit: '10mb', extended: true }));
app.use(express.json({ limit: '10mb' }));
// Security: Static uploads require authentication - served via authenticated middleware
// app.use('/uploads', express.static(path.join(__dirname, '../uploads')));
// Uploads are now served through authenticated route below
app.use('/uploads', authMiddleware, express.static(path.join(__dirname, '../uploads')));

// Serve Print Agent installer (from Supabase Storage)
app.get('/downloads/print-agent', authMiddleware, async (req: Request, res: Response) => {
  try {
    const { getSupabase, isSupabaseConfigured } = require('./config/supabase');

    // Check if Supabase is configured
    if (!isSupabaseConfigured()) {
      // Fallback to local file
      const installerPath = path.join(__dirname, '../installers/WMS-Print-Agent-Setup.exe');
      try {
        await fsPromises.access(installerPath, fs.constants.R_OK);
        return res.download(installerPath, 'WMS-Print-Agent-Setup.exe');
      } catch {
        return res.status(404).json({ error: 'Print Agent installer not found' });
      }
    }

    // Use singleton Supabase client
    const supabase = getSupabase();

    const bucketName = 'downloads';
    const fileName = 'WMS Print Agent Setup 1.0.0.exe';

    // Create a signed URL (valid for 1 hour) and redirect user directly to Supabase
    // This avoids downloading the large file through our backend (timeout issues)
    const { data, error } = await supabase.storage
      .from(bucketName)
      .createSignedUrl(fileName, 3600, {
        download: true  // Forces download instead of opening in browser
      });

    if (error || !data?.signedUrl) {
      console.error('Supabase signed URL error:', error);
      return res.status(404).json({ error: 'File not found in cloud storage' });
    }

    // Redirect user to download directly from Supabase (much faster, no timeout)
    console.log(`📥 Redirecting to Supabase download: ${fileName}`);

    // Return signed URL as JSON so frontend can handle it (avoids CORS redirect issues)
    res.json({ downloadUrl: data.signedUrl });

  } catch (error: any) {
    console.error('Print Agent download error:', error);
    res.status(500).json({ error: 'Download failed' });
  }
});

// API Routes
app.use('/api/auth', authRoutes);
app.use('/api/warehouses', warehousesRoutes);
app.use('/api/inbound', inboundRoutes);
app.use('/api/master-data', masterDataRoutes);
app.use('/api/users', usersRoutes);
app.use('/api/racks', rackRoutes);
app.use('/api/qc', qcRoutes);
app.use('/api/picking', pickingRoutes);
app.use('/api/outbound', outboundRoutes);
app.use('/api/customers', customerRoutes);
app.use('/api/dashboard', dashboardRoutes);
app.use('/api/inventory', inventoryRoutes);

app.use('/api/reports', reportsRoutes);
app.use('/api/backups', backupRoutes);
app.use('/api/permissions', permissionsRoutes);
app.use('/api/ui-access', uiAccessRoutes);
app.use('/api/error-logs', errorLogsRoutes);
app.use('/api/sessions', sessionsRoutes);
app.use('/api/rejections', rejectionRoutes);
app.use('/api/live-view', liveViewRoutes);
app.use('/api/cache', cacheRoutes);
app.use('/api/events', eventsRoutes);
app.use('/api/print-queue', printQueueRoutes);



// Health Check - LIGHTWEIGHT: No DB queries to avoid connection pressure
// DB health is tracked passively via successful API queries
app.get('/api/health', (req: Request, res: Response) => {
  // Memory usage
  const memoryUsage = process.memoryUsage();
  const memoryMB = {
    heapUsed: Math.round(memoryUsage.heapUsed / 1024 / 1024),
    heapTotal: Math.round(memoryUsage.heapTotal / 1024 / 1024),
    rss: Math.round(memoryUsage.rss / 1024 / 1024),
  };

  // Return process status only - no DB query
  // DB readiness is a cached flag, not a live check
  const connectionInfo = getConnectionHealth();

  res.json({
    status: 'OK',
    timestamp: new Date().toISOString(),
    environment: process.env.NODE_ENV || 'development',
    database: {
      ready: connectionInfo.dbReady,
      lastSuccessfulQuery: connectionInfo.lastSuccessfulQuery,
    },
    memory: memoryMB,
    uptime: Math.round(process.uptime()),
  });
});

// Wake-up endpoint - Used by frontend to warm up the server after cold start
// NOTE: This does ONE DB query via warmupConnection, not multiple health checks
app.post('/api/wake', async (req: Request, res: Response) => {
  try {
    const startTime = Date.now();

    // Single attempt to warm up - no retry loop to avoid hammering DB
    const warmedUp = await warmupConnection();
    const responseTime = Date.now() - startTime;
    const connectionInfo = getConnectionHealth();

    if (warmedUp) {
      res.json({
        success: true,
        message: 'Server is ready',
        responseTimeMs: responseTime,
        database: {
          ready: connectionInfo.dbReady,
        },
        timestamp: new Date().toISOString(),
      });
    } else {
      // Return 503 so frontend knows to retry
      res.status(503).json({
        success: false,
        message: 'Server is starting up. Please retry in a moment.',
        responseTimeMs: responseTime,
        database: {
          ready: false,
        },
        isRetryable: true,
        retryAfterMs: 5000,
        timestamp: new Date().toISOString(),
      });
    }
  } catch (error: any) {
    res.status(503).json({
      success: false,
      message: 'Server is starting up. Please retry in a moment.',
      isRetryable: true,
      retryAfterMs: 5000,
      timestamp: new Date().toISOString(),
    });
  }
});

// Force reconnect endpoint (for manual recovery - requires super_admin)
app.post('/api/reconnect', authMiddleware, hasRole('super_admin'), async (req: Request, res: Response) => {
  try {
    logger.info("Manual reconnection triggered");
    const success = await forceReconnect();

    res.json({
      success,
      message: success ? 'Database reconnected successfully' : 'Reconnection failed',
      timestamp: new Date().toISOString(),
    });
  } catch (error: any) {
    res.status(500).json({
      success: false,
      error: 'Reconnection failed',
      timestamp: new Date().toISOString(),
    });
  }
});

// 404
app.use((req: Request, res: Response) => {
  res.status(404).json({ error: 'Route not found' });
});

// Error Handler
app.use(errorHandler);

// Start Server (skip real DB init and listen when running tests)
if (process.env.NODE_ENV !== 'test') {
  // CRITICAL: Start HTTP server IMMEDIATELY - don't wait for anything
  // Render needs to see the port binding within 60 seconds or deployment fails
  console.log(`[STARTUP] Starting HTTP server on port ${PORT}...`);

  const server = app.listen(PORT, () => {
    // Use console.log for immediate output (logger might buffer)
    console.log(`[STARTUP] ✅ Server listening on port ${PORT}`);
    logger.info(`Server running on port ${PORT}`);
  });

  // Handle server errors
  server.on('error', (err: any) => {
    console.error(`[STARTUP] ❌ Server error:`, err.message);
    process.exit(1);
  });

  // Initialize database in the background (non-blocking)
  // Don't crash if database fails - keep retrying
  const initDb = async (attempt = 0): Promise<void> => {
    console.log(`[DB] Attempting database connection (attempt ${attempt + 1})...`);

    try {
      await initializeDatabase();
      console.log(`[DB] ✅ Database connected successfully`);
      logger.info("Database initialization complete");

      // Initialize backup scheduler after DB is ready
      await backupScheduler.initialize();
    } catch (err: any) {
      console.error(`[DB] ❌ Database connection failed (attempt ${attempt + 1}):`, err.message);
      logger.error(`Database initialization failed (attempt ${attempt + 1})`, err);

      // Keep retrying with exponential backoff (max 30 seconds between retries)
      const delay = Math.min(3000 * Math.pow(1.5, attempt), 30000);
      console.log(`[DB] Retrying in ${Math.round(delay / 1000)}s...`);

      setTimeout(() => initDb(attempt + 1), delay);
    }
  };

  // Start database initialization after a tiny delay to ensure server is listening
  setImmediate(() => initDb());

  // 🔐 GRACEFUL SHUTDOWN: Clean up connections on termination
  const gracefulShutdown = async (signal: string) => {
    console.log(`[SHUTDOWN] ${signal} received, shutting down gracefully...`);
    try {
      // Close all SSE connections
      const { sseManager } = require('./services/sseManager');
      sseManager.destroy();

      // Stop accepting new connections
      server.close(() => {
        console.log('[SHUTDOWN] HTTP server closed');
      });
      // Give existing requests 10 seconds to complete
      setTimeout(() => {
        console.log('[SHUTDOWN] Forcing exit after timeout');
        process.exit(0);
      }, 10000);
    } catch (err) {
      console.error('[SHUTDOWN] Error during shutdown:', err);
      process.exit(1);
    }
  };

  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
  process.on('SIGINT', () => gracefulShutdown('SIGINT'));
} else {
  // In test environment, avoid starting DB connection and listener.
  // Tests set NODE_ENV=test and should stub or avoid DB access.
}

export default app;
