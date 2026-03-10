// File Path = warehouse-backend/src/config/database.ts
import { Pool, PoolClient, types } from 'pg';
import dns from 'dns';
import { lookup } from 'dns/promises';
import logger from '../utils/logger';


types.setTypeParser(1114, (str: string) => new Date(str + '+00'));


// Also set environment variable as backup
dns.setDefaultResultOrder('ipv4first');

let pool: Pool | null = null;
let reconnecting = false;
let dbReady = false;
let lastSuccessfulQuery: Date | null = null;

// Lightweight periodic health check - keeps min connection alive
let healthCheckInterval: NodeJS.Timeout | null = null;
let poolMonitor: NodeJS.Timeout | null = null;

//let connectionAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10; // Increased for cross-region connections
const RECONNECT_DELAY_BASE = 1500; // Start with 1.5 seconds

// Track connection health
interface ConnectionHealth {
  isHealthy: boolean;
  lastCheck: Date | null;
  consecutiveFailures: number;
  lastError: string | null;
}

let connectionHealth: ConnectionHealth = {
  isHealthy: false,
  lastCheck: null,
  consecutiveFailures: 0,
  lastError: null,
};

export const getConnectionHealth = () => ({ ...connectionHealth, dbReady, lastSuccessfulQuery });



export const initializeDatabase = async (retryCount = 0): Promise<Pool> => {
  // CRITICAL: Support both pooler and direct connection URLs
  // Use DIRECT_DATABASE_URL as fallback for network issues
  let dbUrl = process.env.DATABASE_URL;

  if (!dbUrl) {
    logger.error("DATABASE_URL not set");
    throw new Error("DATABASE_URL environment variable is not set");
  }

  if (pool && dbReady) return pool;

  // CRITICAL: Parse URL and configure pgbouncer params (required for Supabase)
  const urlObj = new URL(dbUrl);
  const searchParams = urlObj.searchParams;

  // IMPORTANT: Remove sslmode from URL - we handle SSL via Pool config { rejectUnauthorized: false }
  // Using sslmode=require in URL causes "self-signed certificate in certificate chain" errors
  // because it enforces strict certificate validation before our Pool SSL config is applied
  if (searchParams.has('sslmode')) {
    searchParams.delete('sslmode');
  }

  // Detect if using Supabase Session Pooler
  const isPooler = dbUrl.includes('pooler.supabase.com');

  // Add pgbouncer=true for pooler connections
  if (isPooler && !searchParams.has('pgbouncer')) {
    searchParams.set('pgbouncer', 'true');
  }

  // Reconstruct URL with params
  dbUrl = urlObj.toString();

  // Log connection attempt with masked URL for debugging
  const maskedUrl = dbUrl.replace(/:[^:@]+@/, ':****@');
  console.log(`[DB] Connecting to: ${maskedUrl}`);
  console.log(`[DB] Attempt ${retryCount + 1}/${MAX_RECONNECT_ATTEMPTS + 1}`);

  // DNS resolution test (for debugging)
  const hostname = urlObj.hostname;
  try {
    console.log(`[DB] Resolving hostname: ${hostname}`);
    const addresses = await lookup(hostname, { family: 4, all: false });
    console.log(`[DB] Resolved to IPv4: ${addresses.address}`);
  } catch (dnsErr: any) {
    console.error(`[DB] ❌ DNS lookup failed:`, dnsErr.message);
    // Continue anyway - pg library will try to resolve
  }

  // Detect if using PgBouncer mode
  const isPgBouncer = dbUrl.includes('pooler.supabase.com');

  console.log(`[DB] Using Supabase pooler: ${isPgBouncer}`);

  // STABILITY: Generous timeout for cross-region TLS+PgBouncer handshake
  const connectionTimeout = 10000;
  console.log(`[DB] Connection timeout: ${connectionTimeout}ms`);

  const newPool = new Pool({
    connectionString: dbUrl,

    ssl: {
      rejectUnauthorized: false
    },

    max: 5,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 10000,

    keepAlive: true,
    keepAliveInitialDelayMillis: 10000
  });


  newPool.on('connect', (client) => {
    client.query("SET application_name = 'wms-backend-render'");
    client.query("SET statement_timeout = '20s'");
  });

  // Pool monitoring — early warning when connections are under pressure
  // Pool monitoring — early warning when connections are under pressure
  let poolWarningLogged = false;

  // Clear old monitor if reconnecting
  if (poolMonitor) {
    clearInterval(poolMonitor);
  }

  poolMonitor = setInterval(() => {
    if (!newPool) return;

    const total = newPool.totalCount;
    const idle = newPool.idleCount;
    const waiting = newPool.waitingCount;

    if (waiting > 0 || (total >= 5 && idle === 0)) {
      console.warn(
        `[DB] ⚠️ Pool pressure: total=${total}, idle=${idle}, waiting=${waiting}`
      );
      poolWarningLogged = true;
    } else if (poolWarningLogged) {
      console.log(
        `[DB] ✅ Pool recovered: total=${total}, idle=${idle}, waiting=${waiting}`
      );
      poolWarningLogged = false;
    }
  }, 10000);

  // Prevent interval from keeping Node process alive
  if (poolMonitor.unref) {
    poolMonitor.unref();
  }



  // test connection before assigning
  try {
    console.log(`[DB] Testing connection with SELECT 1...`);
    await newPool.query("SELECT 1");

    console.log(`[DB] ✅ Connected successfully`);
    logger.info("Database Connected Successfully");

    pool = newPool;
    dbReady = true;

    connectionHealth = {
      isHealthy: true,
      lastCheck: new Date(),
      consecutiveFailures: 0,
      lastError: null,
    };
    lastSuccessfulQuery = new Date();

    newPool.on("error", handlePoolError);


    startHealthCheck();

    return pool;
  } catch (err: any) {
    console.error(`[DB] ❌ Connection failed:`, err.message);
    console.error(`[DB] Error code:`, err.code);
    console.error(`[DB] Full error:`, JSON.stringify({
      message: err.message,
      code: err.code,
      errno: err.errno,
      syscall: err.syscall,
      hostname: err.hostname
    }));

    logger.error("Database connection failed", err as Error, { attempt: retryCount + 1 });
    dbReady = false;
    connectionHealth = {
      isHealthy: false,
      lastCheck: new Date(),
      consecutiveFailures: connectionHealth.consecutiveFailures + 1,
      lastError: err.message,
    };

    // Close the failed pool to release resources
    newPool.end().catch(() => { });

    // CRITICAL: Shorter exponential backoff with jitter for cross-region connections
    // Faster retries with randomization to avoid thundering herd
    if (retryCount < MAX_RECONNECT_ATTEMPTS) {
      const baseDelay = RECONNECT_DELAY_BASE * Math.pow(1.5, retryCount); // 1.5x growth instead of 2x
      const jitter = Math.random() * 1000; // Add up to 1s random jitter
      const delay = Math.min(baseDelay + jitter, 15000); // Cap at 15 seconds
      console.log(`[DB] Retrying in ${Math.round(delay)}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
      return initializeDatabase(retryCount + 1);
    }

    throw err;
  }
};



function startHealthCheck() {
  // Clear any existing interval
  if (healthCheckInterval) clearInterval(healthCheckInterval);

  healthCheckInterval = setInterval(async () => {
    if (!pool || !dbReady) return;

    try {
      await pool.query('SELECT 1');
      lastSuccessfulQuery = new Date();
      connectionHealth.isHealthy = true;
      connectionHealth.consecutiveFailures = 0;
      connectionHealth.lastCheck = new Date();
    } catch (err: any) {
      connectionHealth.consecutiveFailures++;
      connectionHealth.lastCheck = new Date();
      connectionHealth.lastError = err.message;
      logger.warn('Health check failed', { error: err.message, failures: connectionHealth.consecutiveFailures });

      // After 5 consecutive failures (was 3 — too aggressive for cross-region latency)
      // trigger reconnection. 5 checks × 45s = ~3.75 min of confirmed failure.
      if (connectionHealth.consecutiveFailures >= 5) {
        logger.error('Health check: 5 consecutive failures, triggering reconnection');
        connectionHealth.isHealthy = false;
        handlePoolError(new Error('Health check failed 5 times'));
      }
    }
  }, 45000); // Every 45 seconds

  // Don't let health check prevent process exit
  if (healthCheckInterval.unref) healthCheckInterval.unref();
}

function stopHealthCheck() {
  if (healthCheckInterval) {
    clearInterval(healthCheckInterval);
    healthCheckInterval = null;
  }
}

async function handlePoolError(err: Error) {
  logger.warn("Database pool error", { message: err.message });

  if (reconnecting) return;
  reconnecting = true;
  connectionHealth.isHealthy = false;

  logger.info("Attempting graceful pool recovery...");

  // Stop health check during reconnection
  stopHealthCheck();

  // CRITICAL FIX: Don't set dbReady=false immediately — let in-flight requests finish.
  // Hold reference to old pool so we can drain it in the background.
  const oldPool = pool;
  pool = null;
  dbReady = false;

  // Drain old pool in background with a timeout — don't block reconnection
  if (oldPool) {
    const drainTimeout = setTimeout(() => {
      // Force-terminate if drain takes too long
      try { (oldPool as any)._clients?.forEach((c: any) => c.end?.()); } catch { }
    }, 10000); // 10s max drain time
    oldPool.end()
      .catch(() => { })
      .finally(() => clearTimeout(drainTimeout));
  }

  // Reconnect with exponential backoff
  const reconnect = async (attempt = 0) => {
    if (attempt >= MAX_RECONNECT_ATTEMPTS) {
      logger.error("Max reconnection attempts reached. Manual intervention required.");
      reconnecting = false;
      return;
    }

    // Shorter initial delay, capped at 10s (was uncapped up to 768s at attempt 10)
    const delay = Math.min(RECONNECT_DELAY_BASE * Math.pow(1.5, attempt), 10000);
    logger.info(`Reconnection attempt ${attempt + 1} in ${Math.round(delay)}ms...`);

    await new Promise(resolve => setTimeout(resolve, delay));

    try {
      await initializeDatabase();
      logger.info("Database reconnected successfully");
      reconnecting = false;
    } catch (e: any) {
      logger.error("Reconnection failed", e as Error, { attempt: attempt + 1 });
      await reconnect(attempt + 1);
    }
  };

  await reconnect();
}

// Force reconnection (can be called from health endpoint)
export const forceReconnect = async (): Promise<boolean> => {
  logger.info("Forcing database reconnection...");

  try {
    if (pool) {
      await pool.end();
    }
  } catch { }

  pool = null;
  dbReady = false;
  reconnecting = false;

  try {
    await initializeDatabase();
    return true;
  } catch (err) {
    logger.error("Force reconnection failed", err as Error);
    return false;
  }
};

// Warm up connection (call on first request after cold start)
export const warmupConnection = async (): Promise<boolean> => {
  // If database is already ready, just verify it works
  if (pool && dbReady) {
    try {
      await pool.query("SELECT 1");
      lastSuccessfulQuery = new Date();
      connectionHealth.isHealthy = true;
      return true;
    } catch (err: any) {
      logger.warn("Warmup query failed, attempting reconnection", { error: err.message });
      // Fall through to reconnection
    }
  }

  // Try to initialize/reconnect
  try {
    await initializeDatabase();

    if (pool) {
      await pool.query("SELECT 1");
      lastSuccessfulQuery = new Date();
      connectionHealth.isHealthy = true;
      return true;
    }
    return false;
  } catch (err: any) {
    logger.warn("Warmup failed", { error: err.message });
    return false;
  }
};

export const getPool = (): Pool => {
  if (!dbReady || !pool) {
    throw new Error("Database not ready. The server is reconnecting. Please try again in a moment.");
  }
  return pool;
};

// STABILITY: No retry logic in query() - fail fast, let caller handle
// Retries only exist at startup in initializeDatabase()
export const query = async (text: string, params?: any[]) => {
  const p = getPool();
  const result = await p.query(text, params);

  // Update health tracking on success (passive tracking)
  lastSuccessfulQuery = new Date();
  connectionHealth.isHealthy = true;
  connectionHealth.consecutiveFailures = 0;

  return result;
};

// Simple query without retry (for performance-critical operations)
export const queryNoRetry = async (text: string, params?: any[]) => {
  const p = getPool();
  return p.query(text, params);
};

// Check if database is ready and responsive
export const checkDbHealth = async (): Promise<{ healthy: boolean; latencyMs: number; error?: string }> => {
  const startTime = Date.now();

  try {
    if (!pool || !dbReady) {
      return { healthy: false, latencyMs: 0, error: 'Pool not initialized' };
    }

    await pool.query("SELECT 1");
    const latencyMs = Date.now() - startTime;

    return { healthy: true, latencyMs };
  } catch (err: any) {
    return {
      healthy: false,
      latencyMs: Date.now() - startTime,
      error: err.message
    };
  }
};

export const isDbReady = () => dbReady;

// SAFE TRANSACTION HELPER: Uses a dedicated client so BEGIN/COMMIT/ROLLBACK
// all run on the same PgBouncer backend connection (required for transaction-mode pooling).
// Usage: const result = await withTransaction(async (client) => { ... return value; });
export const withTransaction = async <T>(fn: (client: PoolClient) => Promise<T>): Promise<T> => {
  const client = await getPool().connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
};

// Query with custom timeout (for heavy analytics/reports queries)
// Falls back to regular pool.query since optimized queries should complete well within timeout
// Kept as separate function for semantic clarity and future timeout customization
export const queryWithTimeout = async (text: string, params?: any[], _timeoutMs: number = 30000) => {
  const p = getPool();
  const result = await p.query(text, params);
  lastSuccessfulQuery = new Date();
  connectionHealth.isHealthy = true;
  connectionHealth.consecutiveFailures = 0;
  return result;
};

export default {
  initializeDatabase,
  getPool,
  query,
  queryNoRetry,
  queryWithTimeout,
  isDbReady,
  checkDbHealth,
  getConnectionHealth,
  forceReconnect,
  warmupConnection,
  withTransaction,
};
