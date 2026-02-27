/**
 * 摩瑪建材 API - Cloudflare Workers + D1
 * 提供客戶、會員、訂單、產品的 CRUD 操作
 */

// CORS 標頭
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Content-Type': 'application/json; charset=utf-8',
};

// 回應輔助函數
function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: corsHeaders,
  });
}

function errorResponse(message, status = 400) {
  return jsonResponse({ success: false, error: message }, status);
}

function successResponse(data, message = 'success') {
  return jsonResponse({ success: true, message, data });
}

// 路由處理
export default {
  async fetch(request, env, ctx) {
    // 處理 CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    try {
      // ========== 客戶 API ==========
      if (path === '/api/customers' && method === 'GET') {
        return await getCustomers(env.DB, url.searchParams);
      }
      if (path === '/api/customers' && method === 'POST') {
        return await createCustomer(env.DB, await request.json());
      }
      if (path.match(/^\/api\/customers\/[\w-]+$/) && method === 'GET') {
        const id = path.split('/').pop();
        return await getCustomer(env.DB, id);
      }
      if (path.match(/^\/api\/customers\/[\w-]+$/) && method === 'PUT') {
        const id = path.split('/').pop();
        return await updateCustomer(env.DB, id, await request.json());
      }
      if (path.match(/^\/api\/customers\/[\w-]+$/) && method === 'DELETE') {
        const id = path.split('/').pop();
        return await deleteCustomer(env.DB, id);
      }

      // ========== 統一帳號 API (accounts) ==========
      if (path === '/api/accounts' && method === 'GET') {
        return await getAccounts(env.DB, url.searchParams);
      }
      if (path === '/api/accounts' && method === 'POST') {
        return await createAccount(env.DB, await request.json());
      }
      if (path.match(/^\/api\/accounts\/[\w-]+$/) && method === 'GET') {
        const id = path.split('/').pop();
        return await getAccount(env.DB, id);
      }
      if (path.match(/^\/api\/accounts\/[\w-]+$/) && method === 'PUT') {
        const id = path.split('/').pop();
        return await updateAccount(env.DB, id, await request.json());
      }
      if (path.match(/^\/api\/accounts\/[\w-]+$/) && method === 'DELETE') {
        const id = path.split('/').pop();
        return await deleteAccount(env.DB, id);
      }
      if (path === '/api/accounts/login' && method === 'POST') {
        return await loginAccount(env.DB, await request.json());
      }

      // ========== 會員 API (舊版，向後相容) ==========
      if (path === '/api/members' && method === 'GET') {
        return await getMembers(env.DB, url.searchParams);
      }
      if (path === '/api/members' && method === 'POST') {
        return await createMember(env.DB, await request.json());
      }
      if (path.match(/^\/api\/members\/[\w-]+$/) && method === 'GET') {
        const id = path.split('/').pop();
        return await getMember(env.DB, id);
      }
      if (path.match(/^\/api\/members\/[\w-]+$/) && method === 'PUT') {
        const id = path.split('/').pop();
        return await updateMember(env.DB, id, await request.json());
      }
      if (path.match(/^\/api\/members\/[\w-]+$/) && method === 'DELETE') {
        const id = path.split('/').pop();
        return await deleteMember(env.DB, id);
      }
      if (path === '/api/members/login' && method === 'POST') {
        return await loginAccount(env.DB, await request.json());
      }

      // ========== 產品 API ==========
      if (path === '/api/products' && method === 'GET') {
        return await getProducts(env.DB, url.searchParams);
      }
      if (path === '/api/products' && method === 'POST') {
        return await createProduct(env.DB, await request.json());
      }
      if (path.match(/^\/api\/products\/.+$/) && method === 'GET') {
        const id = decodeURIComponent(path.split('/').pop());
        return await getProduct(env.DB, id);
      }
      if (path.match(/^\/api\/products\/.+$/) && method === 'PUT') {
        const id = decodeURIComponent(path.split('/').pop());
        return await updateProduct(env.DB, id, await request.json());
      }
      if (path.match(/^\/api\/products\/.+$/) && method === 'DELETE') {
        const id = decodeURIComponent(path.split('/').pop());
        return await deleteProduct(env.DB, id);
      }

      // ========== 訂單 API ==========
      if (path === '/api/orders' && method === 'GET') {
        return await getOrders(env.DB, url.searchParams);
      }
      if (path === '/api/orders' && method === 'POST') {
        return await createOrder(env.DB, await request.json());
      }
      if (path.match(/^\/api\/orders\/[\w-]+$/) && method === 'GET') {
        const id = path.split('/').pop();
        return await getOrder(env.DB, id);
      }
      if (path.match(/^\/api\/orders\/[\w-]+$/) && method === 'PUT') {
        const id = path.split('/').pop();
        return await updateOrder(env.DB, id, await request.json());
      }

      // ========== 庫存 API ==========
      if (path === '/api/inventory' && method === 'GET') {
        return await getInventory(env.DB, url.searchParams);
      }
      if (path.match(/^\/api\/inventory\/[^/]+$/) && method === 'GET') {
        const id = decodeURIComponent(path.split('/').pop());
        return await getStock(env.DB, id);
      }
      if (path.match(/^\/api\/inventory\/[^/]+$/) && method === 'PUT') {
        const id = decodeURIComponent(path.split('/').pop());
        return await updateStock(env.DB, id, await request.json());
      }

      // ========== 庫存異動記錄 API ==========
      if (path === '/api/inventory-logs' && method === 'GET') {
        return await getInventoryLogs(env.DB, url.searchParams);
      }

      // ========== 分類 API ==========
      if (path === '/api/categories' && method === 'GET') {
        return await getCategories(env.DB);
      }

      // ========== 定價規則 API ==========
      if (path === '/api/pricing-rules' && method === 'GET') {
        return await getPricingRules(env.DB);
      }

      // ========== 健康檢查 ==========
      if (path === '/api/health' || path === '/') {
        return jsonResponse({
          success: true,
          message: '摩瑪建材 API 運行中',
          version: '1.0.0',
          endpoints: [
            'GET/POST /api/customers',
            'GET/PUT/DELETE /api/customers/:id',
            'GET/POST /api/members',
            'GET/PUT/DELETE /api/members/:id',
            'POST /api/members/login',
            'GET/POST /api/products',
            'GET/PUT /api/products/:id',
            'GET/POST /api/orders',
            'GET/PUT /api/orders/:id',
            'GET /api/categories',
            'GET /api/pricing-rules',
          ],
        });
      }

      // ========== 手動備份 API ==========
      if (path === '/api/backup' && method === 'POST') {
        return await backupToGoogleSheets(env.DB, env.GOOGLE_SCRIPT_URL);
      }

      return errorResponse('找不到此 API 路徑', 404);
    } catch (error) {
      console.error('API Error:', error);
      return errorResponse(error.message, 500);
    }
  },

  // 定時任務處理器 - 每日凌晨 02:00 執行備份
  async scheduled(event, env, ctx) {
    console.log('開始執行每日備份...');
    ctx.waitUntil(backupToGoogleSheets(env.DB, env.GOOGLE_SCRIPT_URL));
  },
};

// ==========================================
// 客戶 CRUD
// ==========================================

async function getCustomers(db, params) {
  let query = 'SELECT * FROM customers WHERE 1=1';
  const bindings = [];

  if (params.get('type')) {
    query += ' AND type = ?';
    bindings.push(params.get('type'));
  }
  if (params.get('status')) {
    query += ' AND status = ?';
    bindings.push(params.get('status'));
  }
  if (params.get('search')) {
    query += ' AND (name LIKE ? OR contact_name LIKE ?)';
    const search = `%${params.get('search')}%`;
    bindings.push(search, search);
  }

  query += ' ORDER BY created_at DESC';

  const result = await db.prepare(query).bind(...bindings).all();
  return successResponse(result.results);
}

async function getCustomer(db, customerId) {
  const result = await db
    .prepare('SELECT * FROM customers WHERE customer_id = ?')
    .bind(customerId)
    .first();

  if (!result) {
    return errorResponse('找不到此客戶', 404);
  }

  // 取得關聯會員
  const members = await db
    .prepare('SELECT member_id, name, phone, level, status FROM members WHERE customer_id = ?')
    .bind(customerId)
    .all();

  return successResponse({ ...result, members: members.results });
}

async function createCustomer(db, data) {
  // 產生客戶 ID
  const countResult = await db.prepare('SELECT COUNT(*) as count FROM customers').first();
  const customerId = `C${String(countResult.count + 1).padStart(3, '0')}`;

  const result = await db
    .prepare(
      `INSERT INTO customers (customer_id, name, type, discount_rate, parent_id, tax_id,
       contact_name, contact_phone, email, fax, payment_method, shipping_method,
       invoice_address, mailing_address, shipping_address, role, status, notes)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .bind(
      customerId,
      data.name,
      data.type || null,
      data.discount_rate || 100,
      data.parent_id || null,
      data.tax_id || null,
      data.contact_name || null,
      data.contact_phone || null,
      data.email || null,
      data.fax || null,
      data.payment_method || null,
      data.shipping_method || null,
      data.invoice_address || null,
      data.mailing_address || null,
      data.shipping_address || null,
      data.role || '會員',
      data.status || '啟用',
      data.notes || null
    )
    .run();

  return successResponse({ customer_id: customerId }, '客戶建立成功');
}

async function updateCustomer(db, customerId, data) {
  const fields = [];
  const values = [];

  const allowedFields = [
    'name', 'type', 'discount_rate', 'parent_id', 'tax_id',
    'contact_name', 'contact_phone', 'email', 'fax',
    'payment_method', 'shipping_method', 'invoice_address',
    'mailing_address', 'shipping_address', 'role', 'status', 'notes'
  ];

  for (const field of allowedFields) {
    if (data[field] !== undefined) {
      fields.push(`${field} = ?`);
      values.push(data[field]);
    }
  }

  if (fields.length === 0) {
    return errorResponse('沒有要更新的欄位');
  }

  fields.push('updated_at = CURRENT_TIMESTAMP');
  values.push(customerId);

  await db
    .prepare(`UPDATE customers SET ${fields.join(', ')} WHERE customer_id = ?`)
    .bind(...values)
    .run();

  return successResponse({ customer_id: customerId }, '客戶更新成功');
}

async function deleteCustomer(db, customerId) {
  // 檢查是否有關聯會員
  const members = await db
    .prepare('SELECT COUNT(*) as count FROM members WHERE customer_id = ?')
    .bind(customerId)
    .first();

  if (members.count > 0) {
    return errorResponse('此客戶有關聯會員，無法刪除');
  }

  await db.prepare('DELETE FROM customers WHERE customer_id = ?').bind(customerId).run();

  return successResponse({ customer_id: customerId }, '客戶刪除成功');
}

// ==========================================
// 會員 CRUD
// ==========================================

async function getMembers(db, params) {
  // 改為查詢 accounts 表，向後相容舊版會員 API
  let query = `
    SELECT a.*,
           p.name as customer_name,
           p.company_type as customer_type,
           p.discount_rate as customer_discount
    FROM accounts a
    LEFT JOIN accounts p ON a.parent_id = p.account_id
    WHERE 1=1
  `;
  const bindings = [];

  // 篩選帳號類型
  // 注意：當 search 參數存在時（通常是查詢手機號碼登入），不限制帳號類型
  // 因為公司帳號的聯絡人手機也需要能登入
  if (params.get('type')) {
    query += ' AND a.account_type = ?';
    bindings.push(params.get('type'));
  } else if (!params.get('search') && !params.get('all')) {
    // 沒有搜尋且沒有要求全部時，預設只返回個人帳號
    query += ' AND a.account_type = ?';
    bindings.push('individual');
  }
  if (params.get('customer_id') || params.get('parent_id')) {
    query += ' AND a.parent_id = ?';
    bindings.push(params.get('customer_id') || params.get('parent_id'));
  }
  if (params.get('status')) {
    query += ' AND a.status = ?';
    bindings.push(params.get('status'));
  }
  if (params.get('search')) {
    query += ' AND (a.name LIKE ? OR a.contact_name LIKE ? OR a.phone LIKE ?)';
    const search = `%${params.get('search')}%`;
    bindings.push(search, search, search);
  }

  query += ' ORDER BY a.created_at DESC';

  const result = await db.prepare(query).bind(...bindings).all();

  // 轉換為舊版會員格式（同時支援中英文欄位名稱）
  const members = result.results.map(a => ({
    // D1 格式欄位 (snake_case)
    member_id: a.account_id,
    name: a.contact_name || a.name,
    phone: a.phone,
    password: a.password,
    email: a.email,
    customer_id: a.parent_id,
    level: a.level,
    discount_rate: a.discount_rate,
    role: a.role,
    status: a.status,
    notes: a.notes,
    address: a.shipping_address,
    last_login: a.last_login,
    created_at: a.created_at,
    updated_at: a.updated_at,
    // 關聯的公司資料
    customer_name: a.customer_name,
    customer_type: a.customer_type,
    customer_discount: a.customer_discount,
    // 原始 accounts 欄位也保留
    account_id: a.account_id,
    account_type: a.account_type,

    // 中文欄位名稱（向後相容 member.js）
    '會員ID': a.account_id,
    '會員名稱': a.contact_name || a.name,
    '手機': a.phone,
    '所屬客戶ID': a.parent_id,
    '等級': a.level,
    '折數': a.discount_rate,
    '統編': a.tax_id,
    '信箱': a.email,
    '狀態': a.status,
    '備註': a.notes,

    // camelCase 格式（向後相容）
    memberId: a.account_id,
    customerId: a.parent_id,
    taxId: a.tax_id
  }));

  return successResponse(members);
}

async function getMember(db, memberId) {
  // 改為查詢 accounts 表
  const result = await db
    .prepare(
      `SELECT a.*,
              p.name as customer_name,
              p.company_type as customer_type,
              p.discount_rate as customer_discount
       FROM accounts a
       LEFT JOIN accounts p ON a.parent_id = p.account_id
       WHERE a.account_id = ?`
    )
    .bind(memberId)
    .first();

  if (!result) {
    return errorResponse('找不到此會員', 404);
  }

  // 轉換為舊版會員格式
  const member = {
    member_id: result.account_id,
    name: result.contact_name || result.name,
    phone: result.phone,
    email: result.email,
    customer_id: result.parent_id,
    level: result.level,
    discount_rate: result.discount_rate,
    role: result.role,
    status: result.status,
    notes: result.notes,
    address: result.shipping_address,
    last_login: result.last_login,
    created_at: result.created_at,
    updated_at: result.updated_at,
    customer_name: result.customer_name,
    customer_type: result.customer_type,
    customer_discount: result.customer_discount,
    account_id: result.account_id,
    account_type: result.account_type
  };

  return successResponse(member);
}

async function createMember(db, data) {
  // 改為寫入 accounts 表
  // 檢查手機是否重複
  if (data.phone) {
    const existing = await db
      .prepare('SELECT account_id FROM accounts WHERE phone = ?')
      .bind(data.phone)
      .first();

    if (existing) {
      return errorResponse('此手機號碼已被註冊');
    }
  }

  // 產生帳號 ID (個人帳號用 M 開頭)
  const countResult = await db.prepare(
    "SELECT COUNT(*) as count FROM accounts WHERE account_id LIKE 'M%'"
  ).first();
  const accountId = `M${String(countResult.count + 1).padStart(3, '0')}`;

  // 計算折數
  let discountRate = data.discount_rate || 100;
  if (data.level) {
    const levelDiscounts = { general: 100, designer: 75, bni: 60, dealer: 45, vip: 35 };
    discountRate = levelDiscounts[data.level] || discountRate;
  }

  await db
    .prepare(
      `INSERT INTO accounts (account_id, account_type, name, phone, password, email,
       contact_name, parent_id, level, discount_rate, shipping_address, role, status, notes)
       VALUES (?, 'individual', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .bind(
      accountId,
      data.name,
      data.phone || null,
      data.password || null,
      data.email || null,
      data.name, // contact_name 同 name
      data.customer_id || data.parent_id || null,
      data.level || 'general',
      discountRate,
      data.address || data.shipping_address || null,
      data.role || '會員',
      data.status || '啟用',
      data.notes || null
    )
    .run();

  return successResponse({ member_id: accountId, account_id: accountId }, '會員建立成功');
}

async function updateMember(db, memberId, data) {
  // 改為更新 accounts 表
  const fields = [];
  const values = [];

  // 欄位映射：舊版欄位 -> accounts 欄位
  const fieldMapping = {
    'phone': 'phone',
    'password': 'password',
    'name': 'contact_name',  // 個人會員的 name 存入 contact_name
    'customer_id': 'parent_id',
    'level': 'level',
    'discount_rate': 'discount_rate',
    'email': 'email',
    'address': 'shipping_address',
    'role': 'role',
    'status': 'status',
    'notes': 'notes'
  };

  for (const [oldField, newField] of Object.entries(fieldMapping)) {
    if (data[oldField] !== undefined) {
      fields.push(`${newField} = ?`);
      values.push(data[oldField]);
    }
  }

  // 如果更新 level，同時更新 discount_rate
  if (data.level && !data.discount_rate) {
    const levelDiscounts = { general: 100, designer: 75, bni: 60, dealer: 45, vip: 35 };
    if (levelDiscounts[data.level]) {
      fields.push('discount_rate = ?');
      values.push(levelDiscounts[data.level]);
    }
  }

  if (fields.length === 0) {
    return errorResponse('沒有要更新的欄位');
  }

  fields.push('updated_at = CURRENT_TIMESTAMP');
  values.push(memberId);

  await db
    .prepare(`UPDATE accounts SET ${fields.join(', ')} WHERE account_id = ?`)
    .bind(...values)
    .run();

  return successResponse({ member_id: memberId, account_id: memberId }, '會員更新成功');
}

async function deleteMember(db, memberId) {
  // 改為刪除 accounts 表
  await db.prepare('DELETE FROM accounts WHERE account_id = ?').bind(memberId).run();
  return successResponse({ member_id: memberId, account_id: memberId }, '會員刪除成功');
}

async function loginMember(db, data) {
  const { phone, password } = data;

  if (!phone || !password) {
    return errorResponse('請輸入手機號碼和密碼');
  }

  const member = await db
    .prepare(
      `SELECT m.*, c.name as customer_name, c.type as customer_type, c.discount_rate as customer_discount
       FROM members m
       LEFT JOIN customers c ON m.customer_id = c.customer_id
       WHERE m.phone = ? AND m.password = ? AND m.status = '啟用'`
    )
    .bind(phone, password)
    .first();

  if (!member) {
    return errorResponse('手機號碼或密碼錯誤', 401);
  }

  // 更新最後登入時間
  await db
    .prepare('UPDATE members SET last_login = CURRENT_TIMESTAMP WHERE member_id = ?')
    .bind(member.member_id)
    .run();

  // 不回傳密碼
  delete member.password;

  return successResponse(member, '登入成功');
}

// ==========================================
// 產品 CRUD
// ==========================================

async function getProducts(db, params) {
  let query = `
    SELECT p.*, c.name as category_name
    FROM products p
    LEFT JOIN categories c ON p.category_id = c.category_id
    WHERE 1=1
  `;
  const bindings = [];

  if (params.get('category_id')) {
    query += ' AND p.category_id = ?';
    bindings.push(params.get('category_id'));
  }
  if (params.get('status')) {
    query += ' AND p.status = ?';
    bindings.push(params.get('status'));
  }
  if (params.get('search')) {
    query += ' AND (p.name LIKE ? OR p.product_id LIKE ?)';
    const search = `%${params.get('search')}%`;
    bindings.push(search, search);
  }

  query += ' ORDER BY p.created_at DESC';

  const result = await db.prepare(query).bind(...bindings).all();
  return successResponse(result.results);
}

async function getProduct(db, productId) {
  const result = await db
    .prepare(
      `SELECT p.*, c.name as category_name
       FROM products p
       LEFT JOIN categories c ON p.category_id = c.category_id
       WHERE p.product_id = ?`
    )
    .bind(productId)
    .first();

  if (!result) {
    return errorResponse('找不到此產品', 404);
  }

  return successResponse(result);
}

async function createProduct(db, data) {
  const countResult = await db.prepare('SELECT COUNT(*) as count FROM products').first();
  const productId = `P${String(countResult.count + 1).padStart(4, '0')}`;

  await db
    .prepare(
      `INSERT INTO products (product_id, category_id, name, original_model, list_price,
       cost_price, stock_qty, low_stock_threshold, unit, image_path, description, specs, status)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .bind(
      productId,
      data.category_id,
      data.name,
      data.original_model || null,
      data.list_price || 0,
      data.cost_price || 0,
      data.stock_qty || 0,
      data.low_stock_threshold || 5,
      data.unit || '組',
      data.image_path || null,
      data.description || null,
      data.specs ? JSON.stringify(data.specs) : null,
      data.status || '上架'
    )
    .run();

  return successResponse({ product_id: productId }, '產品建立成功');
}

async function updateProduct(db, productId, data) {
  const fields = [];
  const values = [];

  const allowedFields = [
    'category_id', 'name', 'original_model', 'list_price', 'cost_price',
    'stock_qty', 'low_stock_threshold', 'unit', 'image_path', 'description', 'specs', 'status'
  ];

  for (const field of allowedFields) {
    if (data[field] !== undefined) {
      fields.push(`${field} = ?`);
      if (field === 'specs' && typeof data[field] === 'object') {
        values.push(JSON.stringify(data[field]));
      } else {
        values.push(data[field]);
      }
    }
  }

  if (fields.length === 0) {
    return errorResponse('沒有要更新的欄位');
  }

  fields.push('updated_at = CURRENT_TIMESTAMP');
  values.push(productId);

  await db
    .prepare(`UPDATE products SET ${fields.join(', ')} WHERE product_id = ?`)
    .bind(...values)
    .run();

  return successResponse({ product_id: productId }, '產品更新成功');
}

async function deleteProduct(db, productId) {
  // 檢查產品是否存在
  const product = await db
    .prepare('SELECT product_id FROM products WHERE product_id = ?')
    .bind(productId)
    .first();

  if (!product) {
    return errorResponse('產品不存在', 404);
  }

  await db.prepare('DELETE FROM products WHERE product_id = ?').bind(productId).run();

  return successResponse({ product_id: productId }, '產品刪除成功');
}

// ==========================================
// 訂單 CRUD
// ==========================================

async function getOrders(db, params) {
  let query = `
    SELECT o.*, m.name as member_name, c.name as customer_name
    FROM orders o
    LEFT JOIN members m ON o.member_id = m.member_id
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    WHERE 1=1
  `;
  const bindings = [];

  if (params.get('member_id')) {
    query += ' AND o.member_id = ?';
    bindings.push(params.get('member_id'));
  }
  if (params.get('customer_id')) {
    query += ' AND o.customer_id = ?';
    bindings.push(params.get('customer_id'));
  }
  if (params.get('status')) {
    query += ' AND o.status = ?';
    bindings.push(params.get('status'));
  }

  query += ' ORDER BY o.order_date DESC';

  const result = await db.prepare(query).bind(...bindings).all();
  return successResponse(result.results);
}

async function getOrder(db, orderId) {
  const order = await db
    .prepare(
      `SELECT o.*, m.name as member_name, m.phone as member_phone,
              c.name as customer_name
       FROM orders o
       LEFT JOIN members m ON o.member_id = m.member_id
       LEFT JOIN customers c ON o.customer_id = c.customer_id
       WHERE o.order_id = ?`
    )
    .bind(orderId)
    .first();

  if (!order) {
    return errorResponse('找不到此訂單', 404);
  }

  // 取得訂單明細
  const items = await db
    .prepare('SELECT * FROM order_items WHERE order_id = ?')
    .bind(orderId)
    .all();

  return successResponse({ ...order, items: items.results });
}

async function createOrder(db, data) {
  // 產生訂單編號
  const today = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const countResult = await db
    .prepare("SELECT COUNT(*) as count FROM orders WHERE order_id LIKE ?")
    .bind(`O${today}%`)
    .first();
  const orderId = `O${today}${String(countResult.count + 1).padStart(3, '0')}`;

  // 建立訂單
  await db
    .prepare(
      `INSERT INTO orders (order_id, member_id, customer_id, status, subtotal,
       identity_discount, amount_discount, total_amount, payment_method,
       shipping_method, shipping_address, contact_name, contact_phone, notes)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .bind(
      orderId,
      data.member_id || null,
      data.customer_id || null,
      data.status || '待確認',
      data.subtotal || 0,
      data.identity_discount || 0,
      data.amount_discount || 0,
      data.total_amount || 0,
      data.payment_method || null,
      data.shipping_method || null,
      data.shipping_address || null,
      data.contact_name || null,
      data.contact_phone || null,
      data.notes || null
    )
    .run();

  // 建立訂單明細並扣減庫存
  if (data.items && Array.isArray(data.items)) {
    for (const item of data.items) {
      // 先用產品名稱查找 D1 的 product (需要 id 和 product_id)
      const product = await db
        .prepare('SELECT id, product_id, stock_qty FROM products WHERE name = ?')
        .bind(item.product_name)
        .first();

      // order_items 使用 product_id (字串如 電子鎖_發卡機)，若查不到則用傳入的或 null
      const productIdStr = product ? product.product_id : (item.product_id || null);

      // 若找不到產品，跳過此項目但記錄警告
      if (!productIdStr) {
        console.warn(`找不到產品: ${item.product_name}`);
        continue;
      }

      // 建立訂單明細
      try {
        await db
          .prepare(
            `INSERT INTO order_items (order_id, product_id, product_name, quantity,
             unit_price, discount_rate, final_price, subtotal)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
          )
          .bind(
            orderId,
            productIdStr,
            item.product_name,
            item.quantity || 1,
            item.unit_price || 0,
            item.discount_rate || 100,
            item.final_price || item.unit_price || 0,
            item.subtotal || 0
          )
          .run();
      } catch (orderItemError) {
        throw new Error(`訂單明細插入失敗 [order_id=${orderId}, product_id=${productIdStr}, name=${item.product_name}]: ${orderItemError.message}`);
      }

      // 扣減庫存
      if (product) {
        const beforeQty = product.stock_qty || 0;
        const newStock = Math.max(0, beforeQty - (item.quantity || 1));
        const changeQty = newStock - beforeQty;

        // 更新庫存
        await db
          .prepare('UPDATE products SET stock_qty = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?')
          .bind(newStock, product.id)
          .run();

        // 記錄庫存異動 (使用字串 product_id)
        await db
          .prepare(
            `INSERT INTO inventory_logs (product_id, change_type, change_qty, before_qty, after_qty, reference_id, operator, notes)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
          )
          .bind(
            product.product_id,
            '銷售出貨',
            changeQty,
            beforeQty,
            newStock,
            orderId,
            data.contact_name || '系統',
            `訂單 ${orderId} 扣減庫存`
          )
          .run();
      }
    }
  }

  return successResponse({ order_id: orderId }, '訂單建立成功');
}

async function updateOrder(db, orderId, data) {
  const fields = [];
  const values = [];

  const allowedFields = [
    'status', 'payment_method', 'shipping_method',
    'shipping_address', 'contact_name', 'contact_phone', 'notes'
  ];

  for (const field of allowedFields) {
    if (data[field] !== undefined) {
      fields.push(`${field} = ?`);
      values.push(data[field]);
    }
  }

  if (fields.length === 0) {
    return errorResponse('沒有要更新的欄位');
  }

  fields.push('updated_at = CURRENT_TIMESTAMP');
  values.push(orderId);

  await db
    .prepare(`UPDATE orders SET ${fields.join(', ')} WHERE order_id = ?`)
    .bind(...values)
    .run();

  return successResponse({ order_id: orderId }, '訂單更新成功');
}

// ==========================================
// 庫存 API
// ==========================================

async function getInventory(db, params) {
  let query = `
    SELECT
      p.product_id as '產品ID',
      CASE
        WHEN p.category_id = 'door-hardware' THEN '門用五金'
        WHEN p.category_id = 'electronic-lock' THEN '電子鎖'
        WHEN p.category_id = 'basic-hardware' THEN '基礎五金'
        WHEN p.category_id = 'luxury-stone' THEN '輕奢石'
        ELSE p.category_id
      END as '產品類別',
      p.name as '產品名稱',
      p.stock_qty as '庫存數量',
      p.low_stock_threshold as '安全庫存',
      p.updated_at as '最後更新',
      CASE
        WHEN p.stock_qty = 0 THEN '缺貨'
        WHEN p.stock_qty <= p.low_stock_threshold THEN '低庫存'
        ELSE '正常'
      END as '狀態',
      p.description as '備註'
    FROM products p
    WHERE 1=1
  `;
  const bindings = [];

  if (params.get('category')) {
    const catMap = {
      '門用五金': 'door-hardware',
      '電子鎖': 'electronic-lock',
      '基礎五金': 'basic-hardware',
      '輕奢石': 'luxury-stone'
    };
    const catId = catMap[params.get('category')] || params.get('category');
    query += ' AND p.category_id = ?';
    bindings.push(catId);
  }

  query += ' ORDER BY p.product_id';

  const result = await db.prepare(query).bind(...bindings).all();
  return jsonResponse({
    success: true,
    products: result.results,
    count: result.results.length
  });
}

async function getStock(db, productId) {
  const result = await db
    .prepare('SELECT product_id, name, stock_qty, low_stock_threshold FROM products WHERE product_id = ?')
    .bind(productId)
    .first();

  if (!result) {
    return errorResponse('找不到此產品', 404);
  }

  return jsonResponse({
    success: true,
    stock: result.stock_qty,
    product: result
  });
}

async function updateStock(db, productId, data) {
  const { newStock, type, operator, orderId, notes } = data;

  // 取得目前庫存
  const current = await db
    .prepare('SELECT stock_qty FROM products WHERE product_id = ?')
    .bind(productId)
    .first();

  if (!current) {
    return errorResponse('找不到此產品', 404);
  }

  const beforeQty = current.stock_qty;
  const changeQty = newStock - beforeQty;

  // 更新庫存
  await db
    .prepare('UPDATE products SET stock_qty = ?, updated_at = CURRENT_TIMESTAMP WHERE product_id = ?')
    .bind(newStock, productId)
    .run();

  // 記錄異動
  await db
    .prepare(
      `INSERT INTO inventory_logs (product_id, change_type, change_qty, before_qty, after_qty, reference_id, operator, notes)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .bind(
      productId,
      type || '調整',
      changeQty,
      beforeQty,
      newStock,
      orderId || null,
      operator || null,
      notes || null
    )
    .run();

  return successResponse({
    product_id: productId,
    before: beforeQty,
    after: newStock,
    change: changeQty
  }, '庫存更新成功');
}

// 查詢庫存異動記錄
async function getInventoryLogs(db, params) {
  const productId = params.get('product_id');
  const changeType = params.get('change_type');
  const startDate = params.get('start_date');
  const endDate = params.get('end_date');
  const limit = parseInt(params.get('limit')) || 100;
  const offset = parseInt(params.get('offset')) || 0;

  let sql = `
    SELECT l.*, p.name as product_name, p.category_id
    FROM inventory_logs l
    LEFT JOIN products p ON l.product_id = p.product_id
    WHERE 1=1
  `;
  const bindings = [];

  if (productId) {
    sql += ' AND l.product_id = ?';
    bindings.push(productId);
  }
  if (changeType) {
    sql += ' AND l.change_type = ?';
    bindings.push(changeType);
  }
  if (startDate) {
    sql += ' AND l.created_at >= ?';
    bindings.push(startDate);
  }
  if (endDate) {
    sql += ' AND l.created_at <= ?';
    bindings.push(endDate + ' 23:59:59');
  }

  sql += ' ORDER BY l.created_at DESC LIMIT ? OFFSET ?';
  bindings.push(limit, offset);

  const stmt = db.prepare(sql);
  const result = await stmt.bind(...bindings).all();

  return successResponse(result.results);
}

// ==========================================
// 分類 & 定價規則
// ==========================================

async function getCategories(db) {
  const result = await db
    .prepare('SELECT * FROM categories ORDER BY sort_order')
    .all();
  return successResponse(result.results);
}

async function getPricingRules(db) {
  const result = await db
    .prepare('SELECT * FROM pricing_rules WHERE status = ? ORDER BY rule_type, priority DESC')
    .bind('啟用')
    .all();
  return successResponse(result.results);
}

// ==========================================
// 統一帳號 CRUD (accounts)
// ==========================================

async function getAccounts(db, params) {
  let query = 'SELECT * FROM accounts WHERE 1=1';
  const bindings = [];

  if (params.get('type')) {
    query += ' AND account_type = ?';
    bindings.push(params.get('type'));
  }
  if (params.get('company_type')) {
    query += ' AND company_type = ?';
    bindings.push(params.get('company_type'));
  }
  if (params.get('level')) {
    query += ' AND level = ?';
    bindings.push(params.get('level'));
  }
  if (params.get('status')) {
    query += ' AND status = ?';
    bindings.push(params.get('status'));
  }
  if (params.get('search')) {
    query += ' AND (name LIKE ? OR contact_name LIKE ? OR phone LIKE ?)';
    const search = `%${params.get('search')}%`;
    bindings.push(search, search, search);
  }

  query += ' ORDER BY created_at DESC';

  const result = await db.prepare(query).bind(...bindings).all();
  return successResponse(result.results);
}

async function getAccount(db, accountId) {
  const result = await db
    .prepare('SELECT * FROM accounts WHERE account_id = ?')
    .bind(accountId)
    .first();

  if (!result) {
    return errorResponse('找不到此帳號', 404);
  }

  // 取得下層帳號
  const children = await db
    .prepare('SELECT account_id, name, account_type, phone, level FROM accounts WHERE parent_id = ?')
    .bind(accountId)
    .all();

  // 不回傳密碼
  delete result.password;

  return successResponse({ ...result, children: children.results });
}

async function createAccount(db, data) {
  // 檢查手機是否重複（如果有提供）
  if (data.phone) {
    const existing = await db
      .prepare('SELECT account_id FROM accounts WHERE phone = ?')
      .bind(data.phone)
      .first();

    if (existing) {
      return errorResponse('此手機號碼已被使用');
    }
  }

  // 產生帳號 ID
  const countResult = await db.prepare('SELECT COUNT(*) as count FROM accounts').first();
  const accountId = `A${String(countResult.count + 1).padStart(3, '0')}`;

  await db
    .prepare(
      `INSERT INTO accounts (account_id, account_type, name, phone, password, email,
       company_type, tax_id, fax, contact_name, parent_id, level, discount_rate,
       invoice_address, mailing_address, shipping_address, payment_method, shipping_method,
       role, is_admin, is_sales, status, notes)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .bind(
      accountId,
      data.account_type || 'individual',
      data.name,
      data.phone || null,
      data.password || null,
      data.email || null,
      data.company_type || null,
      data.tax_id || null,
      data.fax || null,
      data.contact_name || null,
      data.parent_id || null,
      data.level || 'general',
      data.discount_rate || 100,
      data.invoice_address || null,
      data.mailing_address || null,
      data.shipping_address || null,
      data.payment_method || null,
      data.shipping_method || null,
      data.role || '會員',
      data.is_admin ? 1 : 0,
      data.is_sales ? 1 : 0,
      data.status || '啟用',
      data.notes || null
    )
    .run();

  return successResponse({ account_id: accountId }, '帳號建立成功');
}

async function updateAccount(db, accountId, data) {
  const fields = [];
  const values = [];

  const allowedFields = [
    'account_type', 'name', 'phone', 'password', 'email',
    'company_type', 'tax_id', 'fax', 'contact_name', 'parent_id',
    'level', 'discount_rate', 'invoice_address', 'mailing_address',
    'shipping_address', 'payment_method', 'shipping_method',
    'role', 'is_admin', 'is_sales', 'status', 'notes'
  ];

  for (const field of allowedFields) {
    if (data[field] !== undefined) {
      fields.push(`${field} = ?`);
      if (field === 'is_admin' || field === 'is_sales') {
        values.push(data[field] ? 1 : 0);
      } else {
        values.push(data[field]);
      }
    }
  }

  if (fields.length === 0) {
    return errorResponse('沒有要更新的欄位');
  }

  fields.push('updated_at = CURRENT_TIMESTAMP');
  values.push(accountId);

  await db
    .prepare(`UPDATE accounts SET ${fields.join(', ')} WHERE account_id = ?`)
    .bind(...values)
    .run();

  return successResponse({ account_id: accountId }, '帳號更新成功');
}

async function deleteAccount(db, accountId) {
  // 檢查是否有下層帳號
  const children = await db
    .prepare('SELECT COUNT(*) as count FROM accounts WHERE parent_id = ?')
    .bind(accountId)
    .first();

  if (children.count > 0) {
    return errorResponse('此帳號有下層帳號，無法刪除');
  }

  await db.prepare('DELETE FROM accounts WHERE account_id = ?').bind(accountId).run();

  return successResponse({ account_id: accountId }, '帳號刪除成功');
}

async function loginAccount(db, data) {
  const { phone, password } = data;

  if (!phone || !password) {
    return errorResponse('請輸入手機號碼和密碼');
  }

  const account = await db
    .prepare(
      `SELECT * FROM accounts WHERE phone = ? AND password = ? AND status = '啟用'`
    )
    .bind(phone, password)
    .first();

  if (!account) {
    return errorResponse('手機號碼或密碼錯誤', 401);
  }

  // 更新最後登入時間
  await db
    .prepare('UPDATE accounts SET last_login = CURRENT_TIMESTAMP WHERE account_id = ?')
    .bind(account.account_id)
    .run();

  // 不回傳密碼
  delete account.password;

  // 轉換為前端預期的格式
  const response = {
    ...account,
    // 兼容舊版會員格式
    會員ID: account.account_id,
    會員名稱: account.contact_name || account.name,
    手機: account.phone,
    等級: account.level,
    折數: account.discount_rate,
    // 公司資訊
    公司名稱: account.account_type === 'company' ? account.name : null,
    公司分類: account.company_type,
    統一編號: account.tax_id,
  };

  return successResponse(response, '登入成功');
}

// ==========================================
// 備份到 Google Sheets
// ==========================================

async function backupToGoogleSheets(db, googleScriptUrl) {
  try {
    const timestamp = new Date().toISOString();
    console.log(`備份開始: ${timestamp}`);

    // 讀取所有資料表
    const [customers, members, products, orders] = await Promise.all([
      db.prepare('SELECT * FROM customers').all(),
      db.prepare('SELECT * FROM members').all(),
      db.prepare('SELECT * FROM products').all(),
      db.prepare('SELECT * FROM orders ORDER BY created_at DESC LIMIT 1000').all(),
    ]);

    // 準備備份資料
    const backupData = {
      action: 'backup',
      timestamp: timestamp,
      data: {
        customers: customers.results,
        members: members.results,
        products: products.results,
        orders: orders.results,
      },
      counts: {
        customers: customers.results.length,
        members: members.results.length,
        products: products.results.length,
        orders: orders.results.length,
      },
    };

    // 發送到 Google Apps Script
    const response = await fetch(googleScriptUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(backupData),
    });

    const result = await response.json();
    console.log('備份結果:', result);

    return jsonResponse({
      success: true,
      message: '備份完成',
      timestamp: timestamp,
      counts: backupData.counts,
      googleResult: result,
    });
  } catch (error) {
    console.error('備份失敗:', error);
    return jsonResponse({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString(),
    }, 500);
  }
}
