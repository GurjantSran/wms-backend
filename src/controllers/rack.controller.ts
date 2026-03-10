// File Path = warehouse-backend/src/controllers/rack.controller.ts
import { Request, Response } from 'express';
import { query, getPool } from '../config/database';
import * as XLSX from 'xlsx';
import ExcelJS from 'exceljs';
import fs from 'fs';
import { safeError } from '../utils/sanitizeError';
import { logChangeSimple } from '../utils/changeLogger';


// Get all racks
export const getRacks = async (req: Request, res: Response) => {
  try {
    const { warehouse_id } = req.query;
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;

    let sql = `
      SELECT r.*, w.name as warehouse_name 
      FROM racks r
      LEFT JOIN warehouses w ON r.warehouse_id = w.id
    `;
    const params: any[] = [];
    const conditions: string[] = [];

    if (warehouse_id) {
      params.push(warehouse_id);
      conditions.push(`r.warehouse_id = $${params.length}`);
    } else if (accessibleWarehouses && accessibleWarehouses.length > 0) {
      params.push(accessibleWarehouses);
      conditions.push(`r.warehouse_id = ANY($${params.length})`);
    }

    if (conditions.length > 0) {
      sql += ` WHERE ${conditions.join(' AND ')}`;
    }

    sql += ` ORDER BY r.created_at DESC LIMIT 5000`;

    const result = await query(sql, params);
    res.json(result.rows);

  } catch (error: any) {
    console.error('Get racks error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// Create rack
export const createRack = async (req: Request, res: Response) => {
  try {
    const { rack_name, rack_type, capacity, location, warehouse_id } = req.body;
    const userId = (req as any).user?.id;
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;

    if (!rack_name || !warehouse_id) {
      return res.status(400).json({ error: 'Rack name and warehouse_id are required' });
    }

    // Validate user has access to this warehouse
    if (accessibleWarehouses && accessibleWarehouses.length > 0) {
      if (!accessibleWarehouses.includes(Number(warehouse_id))) {
        return res.status(403).json({ error: 'Access denied to this warehouse' });
      }
    }

    const sql = `
      INSERT INTO racks (rack_name, rack_type, capacity, location, warehouse_id, created_by)
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING *
    `;

    const result = await query(sql, [rack_name, rack_type, capacity, location, warehouse_id, userId]);

    // Log change
    logChangeSimple('racks', 'INSERT', {
      id: result.rows[0].id, newData: result.rows[0]
    }, { userId, userName: (req as any).user?.name, warehouseId: warehouse_id }).catch(() => {});

    res.status(201).json(result.rows[0]);

  } catch (error: any) {
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Rack name already exists in this warehouse' });
    }
    console.error('Create rack error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// Update rack
export const updateRack = async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const { rack_name, rack_type, capacity, location } = req.body;
    const userId = (req as any).user?.id;
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;

    // Check rack exists and user has access to its warehouse
    if (accessibleWarehouses && accessibleWarehouses.length > 0) {
      const existing = await query('SELECT warehouse_id FROM racks WHERE id = $1', [id]);
      if (existing.rows.length > 0 && !accessibleWarehouses.includes(Number(existing.rows[0].warehouse_id))) {
        return res.status(403).json({ error: 'Access denied to this warehouse' });
      }
    }

    const sql = `
      UPDATE racks 
      SET rack_name = $1, rack_type = $2, capacity = $3, location = $4, updated_by = $5, updated_at = NOW()
      WHERE id = $6
      RETURNING *
    `;

    const result = await query(sql, [rack_name, rack_type, capacity, location, userId, id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Rack not found' });
    }

    // Log change
    logChangeSimple('racks', 'UPDATE', {
      id: result.rows[0].id, newData: result.rows[0]
    }, { userId, userName: (req as any).user?.name, warehouseId: String(result.rows[0].warehouse_id) }).catch(() => {});

    res.json(result.rows[0]);

  } catch (error: any) {
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Rack name already exists in this warehouse' });
    }
    console.error('Update rack error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// Delete rack
export const deleteRack = async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const accessibleWarehouses = (req as any).accessibleWarehouses as number[] | null;

    // Check rack exists and user has access to its warehouse
    if (accessibleWarehouses && accessibleWarehouses.length > 0) {
      const existing = await query('SELECT warehouse_id FROM racks WHERE id = $1', [id]);
      if (existing.rows.length > 0 && !accessibleWarehouses.includes(Number(existing.rows[0].warehouse_id))) {
        return res.status(403).json({ error: 'Access denied to this warehouse' });
      }
    }

    const result = await query('DELETE FROM racks WHERE id = $1 RETURNING *', [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Rack not found' });
    }

    // Log deletion
    logChangeSimple('racks', 'DELETE', {
      id: result.rows[0].id, oldData: result.rows[0]
    }, { userId: (req as any).user?.id, userName: (req as any).user?.name, warehouseId: String(result.rows[0].warehouse_id) }).catch(() => {});

    res.json({ message: 'Rack deleted successfully' });

  } catch (error: any) {
    console.error('Delete rack error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// Toggle active status
export const toggleRackStatus = async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    const sql = `
      UPDATE racks 
      SET is_active = NOT is_active, updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `;

    const result = await query(sql, [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Rack not found' });
    }

    // Log status toggle
    logChangeSimple('racks', 'UPDATE', {
      id: result.rows[0].id, newData: result.rows[0]
    }, { userId: (req as any).user?.id, userName: (req as any).user?.name, warehouseId: String(result.rows[0].warehouse_id) }).catch(() => {});

    res.json(result.rows[0]);

  } catch (error: any) {
    console.error('Toggle rack status error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};



// Bulk upload racks
export const bulkUploadRacks = async (req: Request, res: Response) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const { warehouse_id } = req.body;
    const userId = (req as any).user?.id;

    console.log('📤 Bulk rack upload started:', { warehouse_id, userId });

    const filePath = req.file.path;
    // Use shared parser utility for safer parsing
    const buffer = await fs.promises.readFile(filePath);
    const { parseExcelBuffer } = require('../utils/excelParser');
    const data: any[] = await parseExcelBuffer(Buffer.from(buffer));

    if (data.length === 0) {
      fs.unlinkSync(filePath);
      return res.status(400).json({ error: 'Excel file is empty' });
    }

    let successCount = 0;
    let errorCount = 0;

    // BATCH INSERT: Collect valid rows and insert in one query instead of N+1
    const validRows: { rackName: string; rackType: string; capacity: any; location: any }[] = [];
    for (const row of data) {
      const rackName = row['RACK_NAME'] || row['rack_name'];
      if (!rackName) continue;
      validRows.push({
        rackName,
        rackType: row['RACK_TYPE'] || row['rack_type'] || 'Standard',
        capacity: row['CAPACITY'] || row['capacity'] || null,
        location: row['LOCATION'] || row['location'] || null,
      });
    }

    if (validRows.length > 0) {
      const client = await getPool().connect();
      try {
        const values: any[] = [];
        const placeholders: string[] = [];
        validRows.forEach((row, idx) => {
          const offset = idx * 6;
          placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6})`);
          values.push(row.rackName, row.rackType, row.capacity, row.location, warehouse_id, userId);
        });

        await client.query('BEGIN');
        const result = await client.query(
          `INSERT INTO racks (rack_name, rack_type, capacity, location, warehouse_id, created_by)
           VALUES ${placeholders.join(', ')}
           ON CONFLICT DO NOTHING`,
          values
        );
        await client.query('COMMIT');
        successCount = result.rowCount || 0;
        errorCount = validRows.length - successCount;
      } catch (error) {
        await client.query('ROLLBACK').catch(() => {});
        console.error('Rack batch insert error:', error);
        errorCount = validRows.length;
      } finally {
        client.release();
      }
    }

    fs.unlinkSync(filePath);

    console.log(`✅ Bulk rack upload complete: ${successCount} success, ${errorCount} errors`);

    res.json({
      message: 'Bulk upload completed',
      successCount,
      errorCount,
      total: data.length
    });

  } catch (error: any) {
    console.error('❌ Bulk rack upload error:', error);
    if (req.file?.path) {
      try { fs.unlinkSync(req.file.path); } catch (e) { }
    }
    res.status(500).json({ error: safeError(error) });
  }
};

// Get racks by warehouse (for dropdown)
export const getRacksByWarehouse = async (req: Request, res: Response) => {
  try {
    const { warehouse_id } = req.query;

    if (!warehouse_id) {
      return res.status(400).json({ error: 'warehouse_id required' });
    }

    const sql = `
      SELECT id, rack_name, rack_type, capacity, location
      FROM racks
      WHERE warehouse_id = $1 AND is_active = true
      ORDER BY rack_name ASC
      LIMIT 500
    `;

    const result = await query(sql, [warehouse_id]);
    res.json(result.rows);

  } catch (error: any) {
    console.error('Get racks error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};
