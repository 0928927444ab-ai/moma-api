/**
 * 產品資料匯入腳本 - 將 Google Sheets 產品轉為 D1 SQL
 * 執行: node import-products.js
 */

const fs = require('fs');

// 讀取 JSON 檔案
const data = JSON.parse(fs.readFileSync('inventory-raw.json', 'utf8'));
const products = data.products;

// 分類對應
const categoryMap = {
    '門用五金': 'door-hardware',
    '電子鎖': 'electronic-lock',
    '基礎五金': 'basic-hardware',
    '輕奢石': 'luxury-stone'
};

// 轉義 SQL 字串
function escapeSQL(str) {
    if (!str) return '';
    return str.replace(/'/g, "''");
}

// 生成 SQL
let sql = `-- 產品資料匯入 D1
-- 從 Google Sheets 庫存匯出
-- 日期: ${new Date().toISOString().split('T')[0]}
-- 共 ${products.length} 筆產品

-- 先清空現有產品資料
DELETE FROM products;

-- 匯入產品資料
`;

const values = products.map(p => {
    const categoryId = categoryMap[p.產品類別] || 'basic-hardware';
    const productId = escapeSQL(p.產品ID);
    const name = escapeSQL(p.產品名稱);
    const stockQty = p.庫存數量 || 0;
    const lowStockThreshold = p.安全庫存 || 5;
    const notes = escapeSQL(p.備註);

    return `('${productId}', '${categoryId}', '${name}', ${stockQty}, ${lowStockThreshold}, '${notes}')`;
});

sql += `INSERT INTO products (product_id, category_id, name, stock_qty, low_stock_threshold, description) VALUES
${values.join(',\n')};
`;

// 輸出 SQL 檔案
fs.writeFileSync('import-products.sql', sql, 'utf8');

console.log(`已生成 import-products.sql`);
console.log(`共 ${products.length} 筆產品資料`);
console.log(`\n分類統計:`);

// 統計各分類數量
const stats = {};
products.forEach(p => {
    const cat = p.產品類別;
    stats[cat] = (stats[cat] || 0) + 1;
});
Object.entries(stats).forEach(([cat, count]) => {
    console.log(`  ${cat}: ${count} 筆`);
});
