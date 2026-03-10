// File Path = warehouse-backend/src/services/sseManager.ts
// SSE (Server-Sent Events) Connection Manager
// Manages real-time push connections for multi-device sync
import { Response } from 'express';

interface SSEConnection {
    res: Response;
    userId: number;
    warehouseId: number;
    page: string; // 'inbound' | 'qc' | 'picking' | 'outbound'
    deviceId: string;
    connectedAt: Date;
}

// Key format: "warehouseId:page" — broadcast to all users on same warehouse+page
type ChannelKey = string;

class SSEManager {
    private connections: Map<ChannelKey, SSEConnection[]> = new Map();
    private heartbeatInterval: NodeJS.Timeout | null = null;

    constructor() {
        // Send heartbeat ping every 30s to keep connections alive (prevents proxy/LB timeout)
        this.heartbeatInterval = setInterval(() => {
            this.sendHeartbeat();
        }, 30000);
    }

    /**
     * Build channel key from warehouseId + page
     */
    private getChannelKey(warehouseId: number, page: string): ChannelKey {
        return `${warehouseId}:${page}`;
    }

    /**
     * Add a new SSE connection
     */
    addConnection(res: Response, userId: number, warehouseId: number, page: string, deviceId: string): void {
        const key = this.getChannelKey(warehouseId, page);
        const conn: SSEConnection = { res, userId, warehouseId, page, deviceId, connectedAt: new Date() };

        if (!this.connections.has(key)) {
            this.connections.set(key, []);
        }

        // Remove any existing connection from same device (reconnect scenario)
        const existing = this.connections.get(key)!;
        const filtered = existing.filter(c => c.deviceId !== deviceId);
        filtered.push(conn);
        this.connections.set(key, filtered);

        // Clean up on client disconnect
        res.on('close', () => {
            this.removeConnection(deviceId, key);
        });

        console.log(`[SSE] ➕ Connected: user=${userId} warehouse=${warehouseId} page=${page} device=${deviceId} (total=${this.getTotalConnections()})`);
    }

    /**
     * Remove a connection by deviceId
     */
    private removeConnection(deviceId: string, channelKey: ChannelKey): void {
        const conns = this.connections.get(channelKey);
        if (!conns) return;

        const filtered = conns.filter(c => c.deviceId !== deviceId);
        if (filtered.length === 0) {
            this.connections.delete(channelKey);
        } else {
            this.connections.set(channelKey, filtered);
        }

        console.log(`[SSE] ➖ Disconnected: device=${deviceId} channel=${channelKey} (total=${this.getTotalConnections()})`);
    }

    /**
     * Broadcast event to all connections on the same warehouse+page
     * Skips the originating deviceId (sender already has the data)
     */
    broadcast(warehouseId: number, page: string, eventType: string, data: any, skipDeviceId?: string): void {
        const key = this.getChannelKey(warehouseId, page);
        const conns = this.connections.get(key);
        if (!conns || conns.length === 0) return;

        const payload = JSON.stringify({ type: eventType, ...data, timestamp: new Date().toISOString() });
        let sentCount = 0;
        const stale: string[] = [];

        for (const conn of conns) {
            // Skip the sender
            if (skipDeviceId && conn.deviceId === skipDeviceId) continue;

            try {
                conn.res.write(`event: ${eventType}\ndata: ${payload}\n\n`);
                // Flush to push through proxy buffers (Render, Nginx, etc.)
                if (typeof (conn.res as any).flush === 'function') (conn.res as any).flush();
                sentCount++;
            } catch (err) {
                // Connection is dead — mark for cleanup
                stale.push(conn.deviceId);
            }
        }

        // Clean up stale connections
        if (stale.length > 0) {
            const cleaned = conns.filter(c => !stale.includes(c.deviceId));
            if (cleaned.length === 0) {
                this.connections.delete(key);
            } else {
                this.connections.set(key, cleaned);
            }
        }

        if (sentCount > 0) {
            console.log(`[SSE] 📡 Broadcast: ${eventType} → ${sentCount} client(s) on ${key}`);
        }
    }

    /**
     * Broadcast event to ONLY the same user's other devices on the same warehouse+page
     * Used for multi-entry row sync (drafts are per-user, so only same user needs the update)
     */
    broadcastToUser(warehouseId: number, page: string, userId: number, eventType: string, data: any, skipDeviceId?: string): void {
        const key = this.getChannelKey(warehouseId, page);
        const conns = this.connections.get(key);
        console.log(`[SSE] 🔍 broadcastToUser: channel=${key} userId=${userId}(type=${typeof userId}) skipDevice=${skipDeviceId} connections=${conns?.length || 0}`);
        if (!conns || conns.length === 0) {
            console.log(`[SSE] ⚠️ No connections on channel ${key}`);
            return;
        }

        const payload = JSON.stringify({ type: eventType, ...data, timestamp: new Date().toISOString() });
        let sentCount = 0;
        const stale: string[] = [];

        for (const conn of conns) {
            console.log(`[SSE] 🔍 Checking conn: userId=${conn.userId}(type=${typeof conn.userId}) deviceId=${conn.deviceId} | match=${conn.userId === userId} skipSelf=${skipDeviceId === conn.deviceId}`);
            // Only send to the same user's other devices
            if (conn.userId !== userId) continue;
            if (skipDeviceId && conn.deviceId === skipDeviceId) continue;

            try {
                conn.res.write(`event: ${eventType}\ndata: ${payload}\n\n`);
                // Flush to push through proxy buffers (Render, Nginx, etc.)
                if (typeof (conn.res as any).flush === 'function') (conn.res as any).flush();
                sentCount++;
            } catch (err) {
                stale.push(conn.deviceId);
            }
        }

        // Clean up stale connections
        if (stale.length > 0) {
            const cleaned = conns.filter(c => !stale.includes(c.deviceId));
            if (cleaned.length === 0) {
                this.connections.delete(key);
            } else {
                this.connections.set(key, cleaned);
            }
        }

        if (sentCount > 0) {
            console.log(`[SSE] 📡 UserBroadcast: ${eventType} → ${sentCount} device(s) for user=${userId} on ${key}`);
        }
    }

    /**
     * Send heartbeat ping to all connections
     */
    private sendHeartbeat(): void {
        const staleDevices: { key: string; deviceId: string }[] = [];

        for (const [key, conns] of this.connections) {
            for (const conn of conns) {
                try {
                    conn.res.write(`event: ping\ndata: ${JSON.stringify({ time: Date.now() })}\n\n`);
                    if (typeof (conn.res as any).flush === 'function') (conn.res as any).flush();
                } catch {
                    staleDevices.push({ key, deviceId: conn.deviceId });
                }
            }
        }

        // Clean up stale connections
        for (const { key, deviceId } of staleDevices) {
            this.removeConnection(deviceId, key);
        }
    }

    /**
     * Get total active connections count
     */
    getTotalConnections(): number {
        let total = 0;
        for (const conns of this.connections.values()) {
            total += conns.length;
        }
        return total;
    }

    /**
     * Get connection stats (for health/debug endpoint)
     */
    getStats(): { totalConnections: number; channels: { key: string; count: number }[] } {
        const channels: { key: string; count: number }[] = [];
        for (const [key, conns] of this.connections) {
            channels.push({ key, count: conns.length });
        }
        return { totalConnections: this.getTotalConnections(), channels };
    }

    /**
     * Cleanup on server shutdown
     */
    destroy(): void {
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
        }
        // Close all connections
        for (const conns of this.connections.values()) {
            for (const conn of conns) {
                try { conn.res.end(); } catch { /* ignore */ }
            }
        }
        this.connections.clear();
    }
}

// Singleton instance
export const sseManager = new SSEManager();
