// File Path = warehouse-backend/src/controllers/customer.controller.ts
import { Request, Response } from 'express';
import { query, getPool } from '../config/database';
import { safeError } from '../utils/sanitizeError';
import { logChange, logChangeSimple } from '../utils/changeLogger';

// ====== GET ALL CUSTOMERS ======
export const getCustomers = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;

    if (!warehouseId) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    const sql = `
      SELECT 
        id, name, contact_person, phone, email, address,
        gst_number, pin_code, city, state,
        billing_address, billing_city, billing_state, billing_pin_code,
        shipping_address, shipping_city, shipping_state, shipping_pin_code,
        shipping_same_as_billing,
        warehouse_id, created_at, updated_at
      FROM customers
      WHERE warehouse_id = $1
      ORDER BY name ASC
      LIMIT 5000
    `;

    const result = await query(sql, [warehouseId]);
    res.json(result.rows);
  } catch (error: any) {
    console.error('Get customers error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== GET SINGLE CUSTOMER ======
export const getCustomerById = async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    // ⚡ EGRESS OPTIMIZATION: Select only needed columns
    const sql = `SELECT id, name, contact_person, phone, email, address,
                        gst_number, pin_code, city, state,
                        billing_address, billing_city, billing_state, billing_pin_code,
                        shipping_address, shipping_city, shipping_state, shipping_pin_code,
                        shipping_same_as_billing,
                        warehouse_id, created_at, updated_at 
                 FROM customers WHERE id = $1`;
    const result = await query(sql, [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Customer not found' });
    }

    res.json(result.rows[0]);
  } catch (error: any) {
    console.error('Get customer by ID error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== CREATE CUSTOMER ======
export const createCustomer = async (req: Request, res: Response) => {
  const client = await getPool().connect();
  try {
    const {
      name, contact_person, phone, email, address, gst_number, pin_code, city, state,
      billing_address, billing_city, billing_state, billing_pin_code,
      shipping_address, shipping_city, shipping_state, shipping_pin_code,
      shipping_same_as_billing,
      warehouse_id
    } = req.body;

    if (!name || !warehouse_id) {
      client.release();
      return res.status(400).json({ error: 'Name and warehouse_id are required' });
    }

    await client.query('BEGIN');

    // Check duplicate name in same warehouse (inside txn for atomicity)
    const checkSql = `
      SELECT id FROM customers 
      WHERE LOWER(name) = LOWER($1) AND warehouse_id = $2
    `;
    const checkResult = await client.query(checkSql, [name, warehouse_id]);

    if (checkResult.rows.length > 0) {
      await client.query('ROLLBACK');
      client.release();
      return res.status(409).json({ error: 'Customer name already exists in this warehouse' });
    }

    const sql = `
      INSERT INTO customers (
        name, contact_person, phone, email, address,
        gst_number, pin_code, city, state,
        billing_address, billing_city, billing_state, billing_pin_code,
        shipping_address, shipping_city, shipping_state, shipping_pin_code,
        shipping_same_as_billing,
        warehouse_id
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
      RETURNING *
    `;

    const result = await client.query(sql, [
      name,
      contact_person || null,
      phone || null,
      email || null,
      address || null,
      gst_number || null,
      pin_code || null,
      city || null,
      state || null,
      billing_address || null,
      billing_city || null,
      billing_state || null,
      billing_pin_code || null,
      shipping_address || null,
      shipping_city || null,
      shipping_state || null,
      shipping_pin_code || null,
      shipping_same_as_billing !== undefined ? shipping_same_as_billing : true,
      warehouse_id
    ]);

    // Log change before commit
    await logChange(client, 'customers', 'INSERT', {
      id: result.rows[0].id, newData: result.rows[0]
    }, { userId: (req as any).user?.id, userName: (req as any).user?.name, warehouseId: warehouse_id });

    await client.query('COMMIT');
    res.status(201).json(result.rows[0]);
  } catch (error: any) {
    await client.query('ROLLBACK').catch(() => {});
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Customer name already exists in this warehouse' });
    }
    console.error('Create customer error:', error);
    res.status(500).json({ error: safeError(error) });
  } finally {
    client.release();
  }
};

// ====== UPDATE CUSTOMER ======
export const updateCustomer = async (req: Request, res: Response) => {
  const client = await getPool().connect();
  try {
    const { id } = req.params;
    const {
      name, contact_person, phone, email, address, gst_number, pin_code, city, state,
      billing_address, billing_city, billing_state, billing_pin_code,
      shipping_address, shipping_city, shipping_state, shipping_pin_code,
      shipping_same_as_billing
    } = req.body;

    if (!name) {
      client.release();
      return res.status(400).json({ error: 'Name is required' });
    }

    await client.query('BEGIN');

    // Check if customer exists
    const checkSql = `SELECT warehouse_id FROM customers WHERE id = $1`;
    const checkResult = await client.query(checkSql, [id]);

    if (checkResult.rows.length === 0) {
      await client.query('ROLLBACK');
      client.release();
      return res.status(404).json({ error: 'Customer not found' });
    }

    const warehouseId = checkResult.rows[0].warehouse_id;

    // Check duplicate name (excluding current customer)
    const dupSql = `
      SELECT id FROM customers 
      WHERE LOWER(name) = LOWER($1) AND warehouse_id = $2 AND id != $3
    `;
    const dupResult = await client.query(dupSql, [name, warehouseId, id]);

    if (dupResult.rows.length > 0) {
      await client.query('ROLLBACK');
      client.release();
      return res.status(409).json({ error: 'Customer name already exists in this warehouse' });
    }

    const sql = `
      UPDATE customers SET
        name = $1,
        contact_person = $2,
        phone = $3,
        email = $4,
        address = $5,
        gst_number = $6,
        pin_code = $7,
        city = $8,
        state = $9,
        billing_address = $10,
        billing_city = $11,
        billing_state = $12,
        billing_pin_code = $13,
        shipping_address = $14,
        shipping_city = $15,
        shipping_state = $16,
        shipping_pin_code = $17,
        shipping_same_as_billing = $18,
        updated_at = NOW()
      WHERE id = $19
      RETURNING *
    `;

    const result = await client.query(sql, [
      name,
      contact_person || null,
      phone || null,
      email || null,
      address || null,
      gst_number || null,
      pin_code || null,
      city || null,
      state || null,
      billing_address || null,
      billing_city || null,
      billing_state || null,
      billing_pin_code || null,
      shipping_address || null,
      shipping_city || null,
      shipping_state || null,
      shipping_pin_code || null,
      shipping_same_as_billing !== undefined ? shipping_same_as_billing : true,
      id
    ]);

    // Log change before commit
    await logChange(client, 'customers', 'UPDATE', {
      id: result.rows[0].id, oldData: checkResult.rows[0], newData: result.rows[0]
    }, { userId: (req as any).user?.id, userName: (req as any).user?.name, warehouseId: String(warehouseId) });

    await client.query('COMMIT');
    res.json(result.rows[0]);
  } catch (error: any) {
    await client.query('ROLLBACK').catch(() => {});
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Customer name already exists in this warehouse' });
    }
    console.error('Update customer error:', error);
    res.status(500).json({ error: safeError(error) });
  } finally {
    client.release();
  }
};

// ====== DELETE CUSTOMER ======
export const deleteCustomer = async (req: Request, res: Response) => {
  const client = await getPool().connect();
  try {
    const { id } = req.params;

    await client.query('BEGIN');

    // Check if customer has any outbound entries (inside txn to prevent TOCTOU)
    const checkOutbound = `
      SELECT COUNT(*) as count FROM outbound WHERE customer_name = (
        SELECT name FROM customers WHERE id = $1
      )
    `;
    const checkResult = await client.query(checkOutbound, [id]);

    if (parseInt(checkResult.rows[0].count) > 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({
        error: 'Cannot delete customer with existing outbound entries'
      });
    }

    const sql = `DELETE FROM customers WHERE id = $1 RETURNING *`;
    const result = await client.query(sql, [id]);

    if (result.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Customer not found' });
    }

    // Log deletion before commit
    await logChange(client, 'customers', 'DELETE', {
      id: result.rows[0].id, oldData: result.rows[0]
    }, { userId: (req as any).user?.id, userName: (req as any).user?.name, warehouseId: String(result.rows[0].warehouse_id) });

    await client.query('COMMIT');
    res.json({ message: 'Customer deleted successfully' });
  } catch (error: any) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('Delete customer error:', error);
    res.status(500).json({ error: safeError(error) });
  } finally {
    client.release();
  }
};

// ====== GET CUSTOMER NAMES FOR DROPDOWN ======
export const getCustomerNames = async (req: Request, res: Response) => {
  try {
    const { warehouseId } = req.query;

    if (!warehouseId) {
      return res.status(400).json({ error: 'Warehouse ID required' });
    }

    const sql = `
      SELECT DISTINCT name 
      FROM customers 
      WHERE warehouse_id = $1 
      ORDER BY name ASC
      LIMIT 1000
    `;

    const result = await query(sql, [warehouseId]);
    res.json(result.rows.map((r: any) => r.name));
  } catch (error: any) {
    console.error('Get customer names error:', error);
    res.status(500).json({ error: safeError(error) });
  }
};

// ====== LOOKUP PINCODE FOR CITY/STATE AUTO-FILL ======
export const lookupPincode = async (req: Request, res: Response) => {
  try {
    const { pincode } = req.params;

    if (!pincode || pincode.length !== 6 || !/^\d{6}$/.test(pincode)) {
      return res.status(400).json({ error: 'Invalid pincode. Must be 6 digits.' });
    }

    // Use India Post API (free, no API key required)
    const response = await fetch(`https://api.postalpincode.in/pincode/${pincode}`, {
      signal: AbortSignal.timeout(5000)
    });
    const data = await response.json();

    if (!data || !Array.isArray(data) || data.length === 0) {
      return res.status(404).json({ error: 'Pincode not found' });
    }

    const result = data[0];

    if (result.Status !== 'Success' || !result.PostOffice || result.PostOffice.length === 0) {
      return res.status(404).json({ error: 'Pincode not found or invalid' });
    }

    // Get the first post office entry (usually the main one)
    const postOffice = result.PostOffice[0];

    res.json({
      success: true,
      pincode: pincode,
      city: postOffice.District || postOffice.Block || postOffice.Name,
      state: postOffice.State,
      district: postOffice.District,
      region: postOffice.Region,
      country: postOffice.Country || 'India',
      postOffices: result.PostOffice.map((po: any) => ({
        name: po.Name,
        branchType: po.BranchType,
        deliveryStatus: po.DeliveryStatus
      }))
    });
  } catch (error: any) {
    console.error('Pincode lookup error:', error);
    res.status(500).json({ error: 'Failed to lookup pincode. Please enter city and state manually.' });
  }
};

// ====== LOOKUP GST NUMBER FOR COMPANY DETAILS AUTO-FILL ======
export const lookupGSTNumber = async (req: Request, res: Response) => {
  try {
    const { gstin } = req.params;

    // Validate GST format
    const gstRegex = /^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$/;
    if (!gstin || !gstRegex.test(gstin.toUpperCase())) {
      return res.status(400).json({ error: 'Invalid GSTIN format' });
    }

    // Extract state code from GST (first 2 digits)
    const stateCode = gstin.substring(0, 2);
    const stateMap: Record<string, string> = {
      '01': 'Jammu and Kashmir', '02': 'Himachal Pradesh', '03': 'Punjab', '04': 'Chandigarh',
      '05': 'Uttarakhand', '06': 'Haryana', '07': 'Delhi', '08': 'Rajasthan',
      '09': 'Uttar Pradesh', '10': 'Bihar', '11': 'Sikkim', '12': 'Arunachal Pradesh',
      '13': 'Nagaland', '14': 'Manipur', '15': 'Mizoram', '16': 'Tripura',
      '17': 'Meghalaya', '18': 'Assam', '19': 'West Bengal', '20': 'Jharkhand',
      '21': 'Odisha', '22': 'Chhattisgarh', '23': 'Madhya Pradesh', '24': 'Gujarat',
      '25': 'Daman and Diu', '26': 'Dadra and Nagar Haveli', '27': 'Maharashtra', '28': 'Andhra Pradesh',
      '29': 'Karnataka', '30': 'Goa', '31': 'Lakshadweep', '32': 'Kerala',
      '33': 'Tamil Nadu', '34': 'Puducherry', '35': 'Andaman and Nicobar Islands', '36': 'Telangana',
      '37': 'Andhra Pradesh', '38': 'Ladakh'
    };

    const state = stateMap[stateCode] || '';
    const gstinUpper = gstin.toUpperCase();

    // Try multiple GST APIs in sequence
    const gstAPIs = [
      {
        name: 'gstincheck',
        url: `https://sheet.gstincheck.co.in/check/${gstinUpper}`,
        parse: (data: any) => {
          if (data && data.flag && data.data) {
            return {
              company_name: data.data.tradeNam || data.data.lgnm || '',
              legal_name: data.data.lgnm || '',
              state: data.data.pradr?.addr?.stcd || state,
              city: data.data.pradr?.addr?.dst || '',
              address: data.data.pradr?.adr || '',
              pincode: data.data.pradr?.addr?.pncd || ''
            };
          }
          return null;
        }
      },
      {
        name: 'appspot',
        url: `https://gst-verification.p.rapidapi.com/v3/task/sync/verify_with_source/ind_gst_certificate?gstin=${gstinUpper}`,
        headers: {
          'X-RapidAPI-Key': process.env.RAPIDAPI_KEY || '',
          'X-RapidAPI-Host': 'gst-verification.p.rapidapi.com'
        },
        parse: (data: any) => {
          if (data && data.result) {
            const r = data.result;
            return {
              company_name: r.trade_name || r.legal_name || '',
              legal_name: r.legal_name || '',
              state: r.principal_place_of_business?.state || state,
              city: r.principal_place_of_business?.city || '',
              address: r.principal_place_of_business?.address || '',
              pincode: r.principal_place_of_business?.pincode || ''
            };
          }
          return null;
        }
      }
    ];

    // Try each API
    for (const api of gstAPIs) {
      try {
        // Skip RapidAPI if no key configured
        if (api.name === 'appspot' && !process.env.RAPIDAPI_KEY) continue;

        const response = await fetch(api.url, {
          signal: AbortSignal.timeout(5000),
          headers: {
            'Accept': 'application/json',
            ...(api.headers || {})
          }
        });

        if (response.ok) {
          const data: any = await response.json();
          console.log(`GST API (${api.name}) response:`, JSON.stringify(data, null, 2));

          const parsed = api.parse(data);
          if (parsed && (parsed.company_name || parsed.address)) {
            return res.json({
              success: true,
              gstin: gstinUpper,
              ...parsed,
              source: api.name
            });
          }
        }
      } catch (apiError: any) {
        console.log(`GST API (${api.name}) failed:`, apiError.message);
      }
    }

    // Fallback: Return just the state based on GST code
    console.log('All GST APIs failed, returning state fallback for:', gstinUpper);
    res.json({
      success: true,
      gstin: gstinUpper,
      state: state,
      company_name: '',
      legal_name: '',
      city: '',
      address: '',
      pincode: '',
      message: 'Could not fetch full details from GST portal. State derived from GSTIN.',
      source: 'fallback'
    });
  } catch (error: any) {
    console.error('GST lookup error:', error);
    res.status(500).json({ error: 'Failed to lookup GST details' });
  }
};