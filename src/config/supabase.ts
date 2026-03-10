// File Path = wms_backend/src/config/supabase.ts
/**
 * Singleton Supabase Client
 * 
 * CRITICAL: Reuses a single SupabaseClient instance across all requests.
 * Previously, every API call created a new createClient() → new HTTPS connection + TLS handshake.
 * Over hours of operation, this caused Cloudflare connection rate limiting (500 errors).
 * 
 * This module provides:
 * 1. Singleton client with connection reuse (keep-alive)
 * 2. 30s fetch timeout to prevent hanging requests  
 * 3. Retry wrapper for transient Cloudflare/network errors
 * 4. Circuit breaker to stop hammering Supabase when it's unreachable
 * 5. Detailed error logging for Cloudflare HTML error responses
 */

import { createClient, SupabaseClient } from '@supabase/supabase-js';

let supabaseInstance: SupabaseClient | null = null;

/**
 * Get the singleton Supabase client.
 * Creates one on first call, reuses it for all subsequent calls.
 */
export const getSupabase = (): SupabaseClient => {
    if (supabaseInstance) return supabaseInstance;

    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

    if (!url || !key) {
        throw new Error('SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set');
    }

    supabaseInstance = createClient(url, key, {
        auth: {
            autoRefreshToken: false,
            persistSession: false,
        },
        global: {
            fetch: (input: any, init?: any) => {
                // 30s timeout to prevent hanging requests on Cloudflare issues
                // (was 10s — too aggressive, causing mass AbortErrors under load)
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), 30000);

                // If the caller already provides a signal, listen for its abort too
                if (init?.signal) {
                    init.signal.addEventListener('abort', () => controller.abort(), { once: true });
                }

                return fetch(input, {
                    ...init,
                    signal: controller.signal,
                    keepalive: true,
                }).then((response) => {
                    clearTimeout(timeoutId);
                    return response;
                }).catch((err) => {
                    clearTimeout(timeoutId);
                    throw err;
                });
            },
        },
    });

    console.log('[Supabase] ✅ Singleton client initialized (connection reuse enabled)');
    return supabaseInstance;
};

/**
 * Check if Supabase is configured (URL + key present).
 */
export const isSupabaseConfigured = (): boolean => {
    return !!(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY);
};

// ===== CIRCUIT BREAKER =====
// Prevents flooding Supabase with requests when it's genuinely unreachable.
// States: CLOSED (normal) → OPEN (blocking) → HALF_OPEN (probe one request)
const circuitBreaker = {
    state: 'CLOSED' as 'CLOSED' | 'OPEN' | 'HALF_OPEN',
    consecutiveFailures: 0,
    lastFailureTime: 0,
    /** Number of consecutive failures before opening the circuit */
    FAILURE_THRESHOLD: 5,
    /** How long to keep circuit open before allowing a probe (ms) */
    COOLDOWN_MS: 60_000, // 60 seconds
    /** Timestamp of last "circuit opened" log (avoid log spam) */
    lastOpenLogTime: 0,
};

/**
 * Record a successful Supabase call — resets the circuit breaker.
 */
const recordSuccess = (): void => {
    if (circuitBreaker.state !== 'CLOSED') {
        console.log(`[Supabase] ✅ Circuit breaker CLOSED — connectivity restored (was ${circuitBreaker.state})`);
    }
    circuitBreaker.consecutiveFailures = 0;
    circuitBreaker.state = 'CLOSED';
};

/**
 * Record a failed Supabase call — may trip the circuit breaker.
 */
const recordFailure = (): void => {
    circuitBreaker.consecutiveFailures++;
    circuitBreaker.lastFailureTime = Date.now();

    if (circuitBreaker.consecutiveFailures >= circuitBreaker.FAILURE_THRESHOLD && circuitBreaker.state === 'CLOSED') {
        circuitBreaker.state = 'OPEN';
        console.error(
            `[Supabase] 🔴 Circuit breaker OPEN — ${circuitBreaker.consecutiveFailures} consecutive failures. ` +
            `Blocking all Supabase calls for ${circuitBreaker.COOLDOWN_MS / 1000}s.`
        );
        circuitBreaker.lastOpenLogTime = Date.now();
    }
};

/**
 * Check if a request is allowed through the circuit breaker.
 * Returns true if the request can proceed, false if it should be blocked.
 */
const isCircuitAllowed = (): boolean => {
    if (circuitBreaker.state === 'CLOSED') return true;

    if (circuitBreaker.state === 'OPEN') {
        const elapsed = Date.now() - circuitBreaker.lastFailureTime;
        if (elapsed >= circuitBreaker.COOLDOWN_MS) {
            // Cooldown expired — allow one probe request
            circuitBreaker.state = 'HALF_OPEN';
            console.log('[Supabase] 🟡 Circuit breaker HALF_OPEN — allowing probe request...');
            return true;
        }

        // Still in cooldown — block (log at most once per 30s to reduce spam)
        const now = Date.now();
        if (now - circuitBreaker.lastOpenLogTime > 30_000) {
            const remaining = Math.round((circuitBreaker.COOLDOWN_MS - elapsed) / 1000);
            console.warn(`[Supabase] 🔴 Circuit OPEN — blocking request (${remaining}s remaining in cooldown)`);
            circuitBreaker.lastOpenLogTime = now;
        }
        return false;
    }

    // HALF_OPEN: only one probe at a time, block the rest
    return false;
};

/** Public getter for circuit breaker state (for routes that want to check) */
export const isSupabaseCircuitOpen = (): boolean => circuitBreaker.state === 'OPEN';

/**
 * Check if the error is a hard connectivity failure (TCP can't connect at all).
 * These should NEVER be retried — the server is unreachable, retrying immediately is pointless.
 */
const isHardConnectivityFailure = (error: any): boolean => {
    if (!error) return false;
    const msg = (error.message || '').toLowerCase();
    const details = (error.details || error.cause?.message || '').toLowerCase();
    const combined = msg + ' ' + details;

    // ConnectTimeoutError = TCP handshake failed (server unreachable)
    if (combined.includes('connecttimeout') || combined.includes('und_err_connect_timeout')) return true;
    // Our own AbortController timeout
    if (msg.includes('abort') && !msg.includes('cloudflare')) return true;
    // DNS failure
    if (combined.includes('enotfound') || combined.includes('getaddrinfo')) return true;
    // Connection actively refused
    if (combined.includes('econnrefused')) return true;

    return false;
};

/**
 * Detect if an error is a transient issue worth retrying.
 * Hard connectivity failures (ConnectTimeout, DNS, refused) are NOT retried.
 * Only Cloudflare 5xx, short network blips, and socket resets are retried.
 */
const isTransientError = (error: any): boolean => {
    if (!error) return false;

    // Hard connectivity failures — server is completely unreachable, don't retry
    if (isHardConnectivityFailure(error)) return false;

    const msg = (error.message || error.msg || '').toLowerCase();
    const details = (error.details || error.cause?.message || '').toLowerCase();

    // "fetch failed" wrapping ConnectTimeoutError → NOT retryable
    if (msg.includes('fetch failed') && (details.includes('connecttimeout') || details.includes('enotfound') || details.includes('econnrefused'))) return false;

    // Cloudflare HTML error responses (temporary 5xx)
    if (msg.includes('cloudflare') || msg.includes('<!doctype') || msg.includes('<html')) return true;
    // Transient network blips (socket reset, but NOT full connect timeout)
    if (msg.includes('econnreset') || msg.includes('socket hang up')) return true;
    // HTTP 500/502/503/504
    if (error.status >= 500 || error.code === '500' || error.code === '502' || error.code === '503') return true;

    return false;
};

/**
 * Check if error indicates the server is truly unreachable (should trip circuit breaker).
 * Matches: ConnectTimeoutError, ECONNREFUSED, ETIMEDOUT, DNS failures, fetch failed, AbortError.
 */
const isConnectivityError = (error: any): boolean => {
    if (!error) return false;
    const msg = (error.message || '').toLowerCase();
    const details = (error.details || error.cause?.message || '').toLowerCase();
    const combined = msg + ' ' + details;

    return (
        combined.includes('connecttimeout') ||
        combined.includes('econnrefused') ||
        combined.includes('etimedout') ||
        combined.includes('enotfound') ||
        combined.includes('fetch failed') ||
        combined.includes('socket hang up') ||
        combined.includes('abort')
    );
};

/**
 * Log detailed info when a Supabase error occurs.
 * Compact format for connectivity errors, verbose for Cloudflare/unknown.
 */
const logSupabaseError = (context: string, error: any): void => {
    const msg = error?.message || error?.msg || 'Unknown error';

    // For connectivity errors, log a single compact line (not the full stack dump)
    if (isConnectivityError(error)) {
        console.error(`[Supabase] ❌ ${context}: ${msg}`);
        return;
    }

    // For Cloudflare/other errors, log full details
    const errorStr = JSON.stringify(error, null, 2);
    const isHtml = errorStr.includes('<!DOCTYPE') || errorStr.includes('<html') || errorStr.includes('cloudflare');

    console.error(`[Supabase] ❌ ${context}:`, {
        message: msg,
        code: error.code,
        status: error.status || error.statusCode,
        hint: error.hint,
        details: error.details,
        isCloudflareError: isHtml,
        errorPreview: isHtml ? errorStr.substring(0, 500) : undefined,
        timestamp: new Date().toISOString(),
    });
};

/**
 * Execute a Supabase query with retry logic for transient errors.
 * 
 * Usage:
 *   const { data, error } = await supabaseRetry(
 *       () => getSupabase().from('table').select('*'),
 *       'fetchSessions'
 *   );
 * 
 * @param queryFn - Function that returns a Supabase query (PromiseLike with data/error)
 * @param context - Description for error logging (e.g., 'fetchLiveSessions')
 * @param maxRetries - Number of retries (default: 2, so 3 total attempts)
 * @param baseDelayMs - Base delay between retries in ms (default: 500)
 */
export const supabaseRetry = async <T>(
    queryFn: () => PromiseLike<{ data: T; error: any }>,
    context: string = 'supabaseQuery',
    maxRetries: number = 2,
    baseDelayMs: number = 500,
): Promise<{ data: T; error: any }> => {
    // ===== Circuit breaker check =====
    if (!isCircuitAllowed()) {
        // Circuit is open — fail fast without hitting Supabase
        return {
            data: null as any,
            error: {
                message: 'Supabase circuit breaker is OPEN — connectivity down, skipping request',
                code: 'CIRCUIT_OPEN',
                isCircuitBreaker: true,
            },
        };
    }

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
        // Re-check circuit breaker on each retry (stop early if circuit opened mid-retry)
        if (attempt > 0 && !isCircuitAllowed()) {
            return {
                data: null as any,
                error: {
                    message: 'Supabase circuit breaker opened during retry — aborting',
                    code: 'CIRCUIT_OPEN',
                    isCircuitBreaker: true,
                },
            };
        }

        try {
            const result = await queryFn();

            if (!result.error) {
                recordSuccess();
                return result;
            }

            // Hard connectivity failure → don't retry, record and return immediately
            if (isConnectivityError(result.error)) {
                recordFailure();
                logSupabaseError(`${context} (attempt ${attempt + 1}/${maxRetries + 1}, connectivity)`, result.error);
                return result;
            }

            // Check if this error is retryable
            if (!isTransientError(result.error) || attempt === maxRetries) {
                logSupabaseError(`${context} (attempt ${attempt + 1}/${maxRetries + 1}, final)`, result.error);
                return result;
            }

            // Transient error — log and retry
            const delay = baseDelayMs * Math.pow(2, attempt) + Math.random() * 200;
            console.warn(
                `[Supabase] ⚠️ ${context} transient error (attempt ${attempt + 1}/${maxRetries + 1}), retrying in ${Math.round(delay)}ms:`,
                result.error.message || result.error
            );
            await new Promise(resolve => setTimeout(resolve, delay));

        } catch (err: any) {
            // Hard connectivity failure → don't retry, record and throw immediately
            if (isConnectivityError(err)) {
                recordFailure();
                logSupabaseError(`${context} (attempt ${attempt + 1}/${maxRetries + 1}, connectivity)`, err);
                throw err;
            }

            // Exception thrown (other network error)
            if (!isTransientError(err) || attempt === maxRetries) {
                logSupabaseError(`${context} (attempt ${attempt + 1}/${maxRetries + 1}, exception)`, err);
                throw err;
            }

            const delay = baseDelayMs * Math.pow(2, attempt) + Math.random() * 200;
            console.warn(
                `[Supabase] ⚠️ ${context} transient error (attempt ${attempt + 1}/${maxRetries + 1}), retrying in ${Math.round(delay)}ms:`,
                err.message
            );
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    // Should not reach here, but safety fallback
    return queryFn();
};

export default { getSupabase, isSupabaseConfigured, isSupabaseCircuitOpen, supabaseRetry };
