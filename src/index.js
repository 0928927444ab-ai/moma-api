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

      // ========== 客戶變更記錄 API ==========
      if (path === '/api/customer-logs' && method === 'GET') {
        return await getCustomerLogs(env.DB, url.searchParams);
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

      // ========== 查詢客戶關聯會員（從 members 表）==========
      if (path.match(/^\/api\/customer-members\/[\w-]+$/) && method === 'GET') {
        const customerId = path.split('/').pop();
        return await getCustomerMembers(env.DB, customerId);
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
      // 取消訂單（回補庫存）
      if (path.match(/^\/api\/orders\/[\w-]+\/cancel$/) && method === 'POST') {
        const parts = path.split('/');
        const orderId = parts[parts.length - 2];
        return await cancelOrder(env.DB, orderId, await request.json());
      }
      // 刪除訂單（不回補庫存，用於清除測試資料）
      if (path.match(/^\/api\/orders\/[\w-]+$/) && method === 'DELETE') {
        const id = path.split('/').pop();
        return await deleteOrder(env.DB, id);
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

      // ========== 退佣記錄 API ==========
      if (path === '/api/commissions' && method === 'GET') {
        return await getCommissions(env.DB, url.searchParams);
      }
      if (path === '/api/commissions' && method === 'POST') {
        return await createCommission(env.DB, await request.json());
      }
      if (path === '/api/commissions/batch' && method === 'POST') {
        return await createCommissionBatch(env.DB, await request.json());
      }
      if (path.match(/^\/api\/commissions\/[\w-]+$/) && method === 'PUT') {
        const id = path.split('/').pop();
        return await updateCommission(env.DB, id, await request.json());
      }
      if (path.match(/^\/api\/commissions\/[\w-]+$/) && method === 'DELETE') {
        const id = path.split('/').pop();
        return await deleteCommission(env.DB, id);
      }

      // ========== 分類 API ==========
      if (path === '/api/categories' && method === 'GET') {
        return await getCategories(env.DB);
      }

      // ========== 定價規則 API ==========
      if (path === '/api/pricing-rules' && method === 'GET') {
        return await getPricingRules(env.DB);
      }

      // ========== 名片辨識 API ==========
      if (path === '/api/ocr/business-card' && method === 'POST') {
        return await analyzeBusinessCard(env.AI, await request.json());
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
            'GET /api/customer-logs',
            'GET/POST /api/members',
            'GET/PUT/DELETE /api/members/:id',
            'POST /api/members/login',
            'GET/POST /api/products',
            'GET/PUT /api/products/:id',
            'GET/POST /api/orders',
            'GET/PUT /api/orders/:id',
            'GET /api/inventory-logs',
            'GET/POST /api/commissions',
            'POST /api/commissions/batch',
            'PUT/DELETE /api/commissions/:id',
            'GET /api/categories',
            'GET /api/pricing-rules',
          ],
        });
      }

      // ========== 手動備份 API ==========
      if (path === '/api/backup' && method === 'POST') {
        return await backupToGoogleSheets(env.DB, env.GOOGLE_SCRIPT_URL);
      }

      // ========== 匯出 API (GitHub 備份用) ==========
      if (path === '/api/export/customers' && method === 'GET') {
        return await exportCustomers(env.DB);
      }
      if (path === '/api/export/all' && method === 'GET') {
        return await exportAllData(env.DB);
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
  // 客戶分類對應表：編號開頭 + 預設折數
  const typeConfig = {
    'M-子公司':          { prefix: 'M', discount: null },   // 折數另外設定
    'M-注資經銷商':      { prefix: 'O', discount: 35 },
    'M-無底薪業務':      { prefix: 'R', discount: 55 },
    '全品項經銷商':      { prefix: 'A', discount: 35 },
    '單品項經銷商':      { prefix: 'S', discount: 35 },
    '建築師/營造廠':     { prefix: 'E', discount: 75 },
    '設計師/工程行':     { prefix: 'E', discount: 75 },
    'BNI綠燈會員':       { prefix: 'N', discount: 75 },
    '大台中南區綠燈會員': { prefix: 'T', discount: 60 },
    '建材行':            { prefix: 'B', discount: 50 },
    '專案':              { prefix: 'P', discount: 50 },
    '一般':              { prefix: 'G', discount: 100 }     // 牌價 = 100 (10折)
  };

  const config = typeConfig[data.type] || { prefix: 'C', discount: 100 };
  const prefix = config.prefix;

  // 如果沒有指定折數，使用分類預設折數
  if (!data.discount_rate && config.discount) {
    data.discount_rate = config.discount;
  }

  // 產生客戶 ID - 找到該開頭最大的編號 +1
  const maxIdResult = await db.prepare(
    `SELECT customer_id FROM customers WHERE customer_id LIKE '${prefix}%' ORDER BY customer_id DESC LIMIT 1`
  ).first();

  let nextNum = 1;
  if (maxIdResult && maxIdResult.customer_id) {
    const currentNum = parseInt(maxIdResult.customer_id.substring(1), 10);
    if (!isNaN(currentNum)) {
      nextNum = currentNum + 1;
    }
  }
  const customerId = `${prefix}${String(nextNum).padStart(3, '0')}`;

  // 取得當前台灣時間
  const now = new Date();
  const twOffset = 8 * 60; // UTC+8
  const twTime = new Date(now.getTime() + twOffset * 60 * 1000);
  const createdAt = twTime.toISOString().slice(0, 19).replace('T', ' ');

  const result = await db
    .prepare(
      `INSERT INTO customers (customer_id, name, short_name, type, discount_rate, parent_id, tax_id,
       contact_name, contact_phone, password, email, fax, payment_method, shipping_method,
       invoice_address, mailing_address, shipping_address, role, status, notes, contacts, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .bind(
      customerId,
      data.name,
      data.short_name || null,
      data.type || null,
      data.discount_rate || 100,
      data.parent_id || null,
      data.tax_id || null,
      data.contact_name || null,
      data.contact_phone || null,
      data.password || null,
      data.email || null,
      data.fax || null,
      data.payment_method || null,
      data.shipping_method || null,
      data.invoice_address || null,
      data.mailing_address || null,
      data.shipping_address || null,
      data.role || '會員',
      data.status || '啟用',
      data.notes || null,
      data.contacts || null,  // 多聯絡人 JSON
      createdAt  // 建立時間
    )
    .run();

  // 記錄變更日誌
  await logCustomerChange(db, customerId, 'CREATE', null, data, data.operator || '系統');

  return successResponse({ customer_id: customerId }, '客戶建立成功');
}

async function updateCustomer(db, customerId, data) {
  // 取得更新前的資料
  const oldData = await db
    .prepare('SELECT * FROM customers WHERE customer_id = ?')
    .bind(customerId)
    .first();

  if (!oldData) {
    return errorResponse('找不到此客戶', 404);
  }

  // 折數固定的分類（6折及以下不可修改）
  const fixedDiscountTypes = {
    'M-注資經銷商': 35,
    'M-無底薪業務': 55,
    '全品項經銷商': 35,
    '單品項經銷商': 35,
    '大台中南區綠燈會員': 60,  // 6折固定
    '建材行': 50,
    '專案': 50
  };

  // 如果是固定折數分類，不允許修改折數
  const customerType = data.type || oldData.type;
  if (fixedDiscountTypes[customerType] && data.discount_rate !== undefined) {
    const fixedDiscount = fixedDiscountTypes[customerType];
    if (data.discount_rate !== fixedDiscount) {
      // 強制使用固定折數
      data.discount_rate = fixedDiscount;
    }
  }

  const fields = [];
  const values = [];
  const changedFields = [];

  const allowedFields = [
    'name', 'short_name', 'type', 'discount_rate', 'parent_id', 'tax_id',
    'contact_name', 'contact_phone', 'password', 'email', 'fax',
    'payment_method', 'shipping_method', 'invoice_address',
    'mailing_address', 'shipping_address', 'role', 'status', 'notes',
    'contacts'  // 多聯絡人 JSON
  ];

  for (const field of allowedFields) {
    if (data[field] !== undefined) {
      fields.push(`${field} = ?`);
      values.push(data[field]);
      // 記錄實際變更的欄位
      if (oldData[field] !== data[field]) {
        changedFields.push(field);
      }
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

  // 記錄變更日誌（只有實際變更才記錄）
  if (changedFields.length > 0) {
    await logCustomerChange(db, customerId, 'UPDATE', oldData, data, data.operator || '系統', changedFields);
  }

  return successResponse({ customer_id: customerId }, '客戶更新成功');
}

async function deleteCustomer(db, customerId) {
  // 取得刪除前的資料
  const oldData = await db
    .prepare('SELECT * FROM customers WHERE customer_id = ?')
    .bind(customerId)
    .first();

  if (!oldData) {
    return errorResponse('找不到此客戶', 404);
  }

  // 檢查是否有關聯會員
  const members = await db
    .prepare('SELECT COUNT(*) as count FROM members WHERE customer_id = ?')
    .bind(customerId)
    .first();

  if (members.count > 0) {
    return errorResponse('此客戶有關聯會員，無法刪除');
  }

  await db.prepare('DELETE FROM customers WHERE customer_id = ?').bind(customerId).run();

  // 記錄變更日誌
  await logCustomerChange(db, customerId, 'DELETE', oldData, null, '系統');

  return successResponse({ customer_id: customerId }, '客戶刪除成功');
}

// 記錄客戶變更
async function logCustomerChange(db, customerId, action, oldData, newData, operator, changedFields = []) {
  await db
    .prepare(
      `INSERT INTO customer_logs (customer_id, action, old_data, new_data, changed_fields, operator)
       VALUES (?, ?, ?, ?, ?, ?)`
    )
    .bind(
      customerId,
      action,
      oldData ? JSON.stringify(oldData) : null,
      newData ? JSON.stringify(newData) : null,
      changedFields.length > 0 ? changedFields.join(',') : null,
      operator
    )
    .run();
}

// 查詢客戶變更記錄
async function getCustomerLogs(db, params) {
  let query = 'SELECT * FROM customer_logs WHERE 1=1';
  const bindings = [];

  if (params.get('customer_id')) {
    query += ' AND customer_id = ?';
    bindings.push(params.get('customer_id'));
  }
  if (params.get('action')) {
    query += ' AND action = ?';
    bindings.push(params.get('action'));
  }
  if (params.get('start_date')) {
    query += ' AND created_at >= ?';
    bindings.push(params.get('start_date'));
  }
  if (params.get('end_date')) {
    query += ' AND created_at <= ?';
    bindings.push(params.get('end_date') + ' 23:59:59');
  }

  const limit = parseInt(params.get('limit')) || 100;
  const offset = parseInt(params.get('offset')) || 0;

  query += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
  bindings.push(limit, offset);

  const result = await db.prepare(query).bind(...bindings).all();
  return successResponse(result.results);
}

// ==========================================
// 查詢客戶關聯會員（從 members 表）
// ==========================================

async function getCustomerMembers(db, customerId) {
  const result = await db
    .prepare('SELECT * FROM members WHERE customer_id = ?')
    .bind(customerId)
    .all();
  return successResponse(result.results);
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
    'stock_qty', 'low_stock_threshold', 'unit', 'image_path', 'description', 'specs', 'status',
    'model', 'sub_category'
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

  // 檢查外鍵是否存在（避免 FOREIGN KEY constraint failed）
  let validMemberId = null;
  let validCustomerId = null;

  if (data.member_id) {
    const memberExists = await db
      .prepare('SELECT member_id FROM members WHERE member_id = ?')
      .bind(data.member_id)
      .first();
    validMemberId = memberExists ? data.member_id : null;
  }

  if (data.customer_id) {
    const customerExists = await db
      .prepare('SELECT customer_id FROM customers WHERE customer_id = ?')
      .bind(data.customer_id)
      .first();
    validCustomerId = customerExists ? data.customer_id : null;
  }

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
      validMemberId,
      validCustomerId,
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
    'shipping_address', 'contact_name', 'contact_phone', 'notes',
    'customer_id', 'customer_name'
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

// 取消訂單並回補庫存
async function cancelOrder(db, orderId, data) {
  // 1. 檢查訂單是否存在
  const order = await db
    .prepare('SELECT * FROM orders WHERE order_id = ?')
    .bind(orderId)
    .first();

  if (!order) {
    return errorResponse('找不到此訂單', 404);
  }

  // 2. 檢查訂單狀態（已取消的訂單不能再次取消）
  if (order.status === '已取消') {
    return errorResponse('此訂單已經取消');
  }

  // 3. 取得訂單明細
  const items = await db
    .prepare('SELECT * FROM order_items WHERE order_id = ?')
    .bind(orderId)
    .all();

  const operator = data.operator || '系統';

  // 4. 回補每個商品的庫存
  for (const item of items.results) {
    if (!item.product_id) continue;

    // 查詢產品（用 product_id 字串）
    const product = await db
      .prepare('SELECT id, product_id, stock_qty FROM products WHERE product_id = ?')
      .bind(item.product_id)
      .first();

    if (product) {
      const beforeQty = product.stock_qty || 0;
      const restoreQty = item.quantity || 1;
      const newStock = beforeQty + restoreQty;

      // 更新庫存
      await db
        .prepare('UPDATE products SET stock_qty = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?')
        .bind(newStock, product.id)
        .run();

      // 記錄庫存異動（訂單取消回補）
      await db
        .prepare(
          `INSERT INTO inventory_logs (product_id, change_type, change_qty, before_qty, after_qty, reference_id, operator, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .bind(
          product.product_id,
          '訂單取消',
          restoreQty,
          beforeQty,
          newStock,
          orderId,
          operator,
          `訂單 ${orderId} 取消，回補庫存`
        )
        .run();
    }
  }

  // 5. 更新訂單狀態為「已取消」
  await db
    .prepare("UPDATE orders SET status = '已取消', updated_at = CURRENT_TIMESTAMP WHERE order_id = ?")
    .bind(orderId)
    .run();

  return successResponse({
    order_id: orderId,
    restored_items: items.results.length
  }, '訂單已取消，庫存已回補');
}

// 刪除訂單（不回補庫存）
async function deleteOrder(db, orderId) {
  // 檢查訂單是否存在
  const order = await db
    .prepare('SELECT * FROM orders WHERE order_id = ?')
    .bind(orderId)
    .first();

  if (!order) {
    return errorResponse('找不到此訂單', 404);
  }

  // 刪除訂單明細
  await db
    .prepare('DELETE FROM order_items WHERE order_id = ?')
    .bind(orderId)
    .run();

  // 刪除訂單
  await db
    .prepare('DELETE FROM orders WHERE order_id = ?')
    .bind(orderId)
    .run();

  return successResponse({ order_id: orderId }, '訂單已刪除（未調整庫存）');
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
// 名片辨識 API (Workers AI)
// ==========================================

async function analyzeBusinessCard(ai, data) {
  try {
    const { image } = data;

    if (!image) {
      return errorResponse('請提供名片圖片');
    }

    // 移除 base64 前綴並轉換為 Uint8Array
    let base64Data = image;
    if (image.includes(',')) {
      base64Data = image.split(',')[1];
    }

    // 將 base64 轉換為 Uint8Array (LLaVA 需要的格式)
    const binaryString = atob(base64Data);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
      bytes[i] = binaryString.charCodeAt(i);
    }

    console.log('圖片大小:', bytes.length, 'bytes');

    // 使用 Workers AI 的 LLaVA 視覺模型分析名片
    let response;
    try {
      response = await ai.run('@cf/llava-hf/llava-1.5-7b-hf', {
        image: [...bytes],
        prompt: `This is a business card image. Please read all the text on this card carefully.

Extract the following information and return ONLY a JSON object (no other text):
{
  "company": "company name",
  "name": "person name",
  "phone": "phone number",
  "email": "email address",
  "address": "address",
  "tax_id": "tax ID number (8 digits)"
}

If a field is not found, set it to empty string "".`,
        max_tokens: 512
      });
    } catch (aiError) {
      console.error('Workers AI 錯誤:', aiError);
      return errorResponse(`AI 模型錯誤: ${aiError.message}`, 500);
    }

    console.log('AI 原始回應:', JSON.stringify(response));

    // 解析 AI 回應
    let parsedData = {
      company: '',
      name: '',
      phone: '',
      email: '',
      address: '',
      tax_id: ''
    };

    // 從回應中取得文字 (支援不同回應格式)
    let responseText = '';
    if (response) {
      if (typeof response === 'string') {
        responseText = response;
      } else if (response.description) {
        // LLaVA 回傳格式
        responseText = response.description;
      } else if (response.response) {
        responseText = response.response;
      } else if (response.result) {
        responseText = response.result;
      } else if (response.choices && response.choices[0]) {
        responseText = response.choices[0].message?.content || response.choices[0].text || '';
      }
    }

    console.log('AI 回應:', responseText);

    if (responseText) {
      try {
        // 嘗試從回應中提取 JSON
        const jsonMatch = responseText.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
          const parsed = JSON.parse(jsonMatch[0]);
          parsedData = {
            company: parsed.company || '',
            name: parsed.name || '',
            phone: parsed.phone || '',
            email: parsed.email || '',
            address: parsed.address || '',
            tax_id: parsed.tax_id || ''
          };
        }
      } catch (parseError) {
        console.error('解析 AI 回應失敗:', parseError);
        // 嘗試用正則表達式提取資訊
        const text = responseText;

        // 提取手機號碼
        const phoneMatch = text.match(/09\d{8}/);
        if (phoneMatch) parsedData.phone = phoneMatch[0];

        // 提取 email
        const emailMatch = text.match(/[\w.-]+@[\w.-]+\.\w+/);
        if (emailMatch) parsedData.email = emailMatch[0];

        // 提取統編
        const taxMatch = text.match(/\d{8}/);
        if (taxMatch && taxMatch[0] !== parsedData.phone?.slice(0, 8)) {
          parsedData.tax_id = taxMatch[0];
        }
      }
    }

    return successResponse(parsedData, '名片辨識完成');

  } catch (error) {
    console.error('名片辨識錯誤:', error);
    return errorResponse('名片辨識失敗: ' + error.message, 500);
  }
}

// ==========================================
// 匯出 API (GitHub 備份用)
// ==========================================

async function exportCustomers(db) {
  const customers = await db.prepare('SELECT * FROM customers ORDER BY customer_id').all();
  const logs = await db.prepare('SELECT * FROM customer_logs ORDER BY created_at DESC LIMIT 1000').all();

  const exportData = {
    exportTime: new Date().toISOString(),
    version: '1.0',
    customers: customers.results,
    customerLogs: logs.results,
    counts: {
      customers: customers.results.length,
      logs: logs.results.length
    }
  };

  return jsonResponse(exportData);
}

async function exportAllData(db) {
  const [customers, customerLogs, products, orders, inventoryLogs] = await Promise.all([
    db.prepare('SELECT * FROM customers ORDER BY customer_id').all(),
    db.prepare('SELECT * FROM customer_logs ORDER BY created_at DESC').all(),
    db.prepare('SELECT * FROM products ORDER BY product_id').all(),
    db.prepare('SELECT * FROM orders ORDER BY created_at DESC LIMIT 1000').all(),
    db.prepare('SELECT * FROM inventory_logs ORDER BY created_at DESC LIMIT 1000').all()
  ]);

  const exportData = {
    exportTime: new Date().toISOString(),
    version: '1.0',
    data: {
      customers: customers.results,
      customerLogs: customerLogs.results,
      products: products.results,
      orders: orders.results,
      inventoryLogs: inventoryLogs.results
    },
    counts: {
      customers: customers.results.length,
      customerLogs: customerLogs.results.length,
      products: products.results.length,
      orders: orders.results.length,
      inventoryLogs: inventoryLogs.results.length
    }
  };

  return jsonResponse(exportData);
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

// ==========================================
// 退佣記錄 CRUD
// ==========================================

async function getCommissions(db, params) {
  let query = 'SELECT * FROM commission_records WHERE 1=1';
  const bindings = [];

  // 篩選月份
  if (params.get('month')) {
    query += ' AND settlement_month = ?';
    bindings.push(params.get('month'));
  }

  // 篩選收佣人
  if (params.get('recipient_id')) {
    query += ' AND recipient_id = ?';
    bindings.push(params.get('recipient_id'));
  }

  // 篩選狀態
  if (params.get('status')) {
    query += ' AND status = ?';
    bindings.push(params.get('status'));
  }

  query += ' ORDER BY created_at DESC';

  const result = await db.prepare(query).bind(...bindings).all();
  return successResponse(result.results);
}

async function createCommission(db, data) {
  // 產生記錄 ID
  const today = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const countResult = await db
    .prepare("SELECT COUNT(*) as count FROM commission_records WHERE record_id LIKE ?")
    .bind(`CR${today}%`)
    .first();
  const recordId = `CR${today}${String(countResult.count + 1).padStart(3, '0')}`;

  const result = await db
    .prepare(
      `INSERT INTO commission_records (
        record_id, settlement_month, recipient_id, recipient_name,
        source_customer_id, source_customer_name, order_id,
        list_price, from_discount, to_discount,
        commission_before_tax, commission_after_tax, status, notes
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .bind(
      recordId,
      data.settlement_month,
      data.recipient_id,
      data.recipient_name,
      data.source_customer_id,
      data.source_customer_name,
      data.order_id || null,
      data.list_price || 0,
      data.from_discount || 100,
      data.to_discount || 100,
      data.commission_before_tax || 0,
      data.commission_after_tax || 0,
      data.status || 'pending',
      data.notes || null
    )
    .run();

  return successResponse({ record_id: recordId }, '退佣記錄已建立');
}

async function createCommissionBatch(db, data) {
  // 批次建立退佣記錄
  if (!data.records || !Array.isArray(data.records)) {
    return errorResponse('請提供 records 陣列');
  }

  const today = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const countResult = await db
    .prepare("SELECT COUNT(*) as count FROM commission_records WHERE record_id LIKE ?")
    .bind(`CR${today}%`)
    .first();
  let nextNum = countResult.count + 1;

  const createdIds = [];

  for (const record of data.records) {
    const recordId = `CR${today}${String(nextNum++).padStart(3, '0')}`;

    await db
      .prepare(
        `INSERT INTO commission_records (
          record_id, settlement_month, recipient_id, recipient_name,
          source_customer_id, source_customer_name, order_id,
          list_price, from_discount, to_discount,
          commission_before_tax, commission_after_tax, status, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .bind(
        recordId,
        record.settlement_month,
        record.recipient_id,
        record.recipient_name,
        record.source_customer_id,
        record.source_customer_name,
        record.order_id || null,
        record.list_price || 0,
        record.from_discount || 100,
        record.to_discount || 100,
        record.commission_before_tax || 0,
        record.commission_after_tax || 0,
        record.status || 'pending',
        record.notes || null
      )
      .run();

    createdIds.push(recordId);
  }

  return successResponse({ created_ids: createdIds, count: createdIds.length }, '批次建立完成');
}

async function updateCommission(db, recordId, data) {
  // 允許更新的欄位
  const allowedFields = ['status', 'paid_date', 'notes'];
  const updates = [];
  const bindings = [];

  for (const field of allowedFields) {
    if (data[field] !== undefined) {
      updates.push(`${field} = ?`);
      bindings.push(data[field]);
    }
  }

  if (updates.length === 0) {
    return errorResponse('沒有可更新的欄位');
  }

  updates.push("updated_at = datetime('now')");
  bindings.push(recordId);

  const query = `UPDATE commission_records SET ${updates.join(', ')} WHERE record_id = ?`;
  const result = await db.prepare(query).bind(...bindings).run();

  if (result.changes === 0) {
    return errorResponse('找不到此記錄', 404);
  }

  return successResponse({ record_id: recordId }, '退佣記錄已更新');
}

async function deleteCommission(db, recordId) {
  const result = await db
    .prepare('DELETE FROM commission_records WHERE record_id = ?')
    .bind(recordId)
    .run();

  if (result.changes === 0) {
    return errorResponse('找不到此記錄', 404);
  }

  return successResponse({ record_id: recordId }, '退佣記錄已刪除');
}
