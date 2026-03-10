// File Path = warehouse-backend/src/controllers/warehouses.controller.ts
import { Request, Response } from 'express';
import { query } from '../config/database';
import { validateWarehouseCode } from '../utils/validators';
import { safeError } from '../utils/sanitizeError';
import { logChangeSimple } from '../utils/changeLogger';

export const getWarehouses = async (req: Request, res: Response) => {
  try {
    const result = await query(
      `SELECT id, name, city, code, address, phone, is_active, created_at
       FROM warehouses
       ORDER BY created_at DESC`
    );
    res.json(result.rows);
  } catch (error: any) {
    res.status(500).json({ error: safeError(error) });
  }
};

export const createWarehouse = async (req: Request, res: Response) => {
  try {
    const { name, city, code, address, phone } = req.body;

    if (!name || !code) {
      return res.status(400).json({ error: 'Name and code are required' });
    }

    if (!validateWarehouseCode(code)) {
      return res.status(400).json({ error: 'Invalid warehouse code (2-10 chars)' });
    }

    const existing = await query('SELECT id FROM warehouses WHERE code = $1', [code]);
    if (existing.rows.length > 0) {
      return res.status(409).json({ error: 'Warehouse code already exists' });
    }

    const result = await query(
      `INSERT INTO warehouses (name, city, code, address, phone, is_active, created_by, created_at)
       VALUES ($1, $2, $3, $4, $5, true, $6, NOW())
       RETURNING id, name, city, code, address, phone, is_active, created_at`,
      [name, city || null, code, address || null, phone || null, req.user?.userId]
    );

    // Log change
    logChangeSimple('warehouses', 'INSERT', {
      id: result.rows[0].id, newData: result.rows[0]
    }, { userId: req.user?.userId, userName: req.user?.full_name, warehouseId: result.rows[0].id }).catch(() => { });

    res.status(201).json(result.rows[0]);
  } catch (error: any) {
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Warehouse code already exists' });
    }
    res.status(500).json({ error: safeError(error) });
  }
};

export const updateWarehouse = async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const { name, city, code, address, phone } = req.body;

    const result = await query(
      `UPDATE warehouses
       SET name = COALESCE($1, name),
           city = COALESCE($2, city),
           code = COALESCE($3, code),
           address = COALESCE($4, address),
           phone = COALESCE($5, phone)
       WHERE id = $6
       RETURNING *`,
      [name, city, code, address, phone, id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Warehouse not found' });
    }

    // Log change
    logChangeSimple('warehouses', 'UPDATE', {
      id: result.rows[0].id, newData: result.rows[0]
    }, { userId: req.user?.userId, userName: req.user?.full_name, warehouseId: Number(id) }).catch(() => { });

    res.json(result.rows[0]);
  } catch (error: any) {
    res.status(500).json({ error: safeError(error) });
  }
};

export const deleteWarehouse = async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    // Guard: check for related data that would be cascade-deleted or orphaned
    const counts = await query(
      `SELECT
        (SELECT COUNT(*) FROM inbound WHERE warehouse_id = $1)::int AS inbound_count,
        (SELECT COUNT(*) FROM outbound WHERE warehouse_id = $1)::int AS outbound_count,
        (SELECT COUNT(*) FROM qc WHERE warehouse_id = $1)::int AS qc_count,
        (SELECT COUNT(*) FROM picking WHERE warehouse_id = $1)::int AS picking_count,
        (SELECT COUNT(*) FROM customers WHERE warehouse_id = $1)::int AS customer_count,
        (SELECT COUNT(*) FROM racks WHERE warehouse_id = $1)::int AS rack_count`,
      [id]
    );

    const c = counts.rows[0];
    const deps: string[] = [];
    if (c.inbound_count > 0) deps.push(`${c.inbound_count} inbound`);
    if (c.outbound_count > 0) deps.push(`${c.outbound_count} outbound`);
    if (c.qc_count > 0) deps.push(`${c.qc_count} QC`);
    if (c.picking_count > 0) deps.push(`${c.picking_count} picking`);
    if (c.customer_count > 0) deps.push(`${c.customer_count} customer`);
    if (c.rack_count > 0) deps.push(`${c.rack_count} rack`);

    if (deps.length > 0) {
      return res.status(400).json({
        error: `Cannot delete warehouse — it has related records: ${deps.join(', ')}. Remove them first.`
      });
    }

    const result = await query('DELETE FROM warehouses WHERE id = $1 RETURNING id', [id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Warehouse not found' });
    }

    // Log deletion
    logChangeSimple('warehouses', 'DELETE', {
      id: result.rows[0].id, oldData: { id }
    }, { userId: req.user?.userId, userName: req.user?.full_name, warehouseId: Number(id) }).catch(() => { });

    res.json({ message: 'Warehouse deleted successfully' });
  } catch (error: any) {
    res.status(500).json({ error: safeError(error) });
  }
};

export const setActiveWarehouse = async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    const result = await query('SELECT id FROM warehouses WHERE id = $1', [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Warehouse not found' });
    }

    res.json({ message: 'Warehouse set as active', warehouseId: id });
  } catch (error: any) {
    res.status(500).json({ error: safeError(error) });
  }
};
