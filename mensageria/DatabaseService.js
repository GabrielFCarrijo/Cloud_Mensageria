const { Client } = require('pg');
const { v4: uuidv4 } = require('uuid');

class DatabaseService {
  constructor() {
    this.client = new Client({
      user: 'postgres',
      host: 'localhost',
      database: 'subclient',
      password: 'postgres',
      port: 5432,
    });

    this.client.connect()
      .then(() => console.log('Conectado ao PostgreSQL'))
      .catch((err) => console.error('Erro ao conectar ao PostgreSQL', err));
  }

  insertCustomer(customer) {
    return this.client.query(`
      INSERT INTO customer (id, name)
      VALUES ($1, $2)
      ON CONFLICT (id) DO UPDATE
      SET name = EXCLUDED.name;
    `, [customer.id, customer.name]);
  }

  insertOrder(order, customer) {
    return this.insertCustomer(customer)
      .then(() => {
        return this.client.query(`
          INSERT INTO "order" (id, created_at, type, customer_id)
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (id) DO UPDATE
          SET created_at = EXCLUDED.created_at,
              type = EXCLUDED.type,
              customer_id = EXCLUDED.customer_id;
        `, [order.id, order.createdAt, order.type, customer.id]);
      });
  }

  insertProduct(sku, value) {
    return this.client.query(`
      INSERT INTO product (id, value)
      VALUES ($1, $2)
      ON CONFLICT (id) DO UPDATE
      SET value = EXCLUDED.value;
    `, [sku, value]);
  }

  insertOrderItem(id, orderId, sku, quantity) {
    return this.client.query(`
      INSERT INTO order_items (id, order_id, sku, quantity)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (id) DO UPDATE
      SET order_id = EXCLUDED.order_id,
          sku = EXCLUDED.sku,
          quantity = EXCLUDED.quantity;
    `, [id, orderId, sku, quantity]);
  }

  fetchOrders() {
    return this.client.query(`
      SELECT
  p.id AS order_id, p.created_at, p.type,
  c.id AS customer_id, c.name AS customer_name,
  i.id AS item_id, i.quantity,
  s.id AS sku_id, s.value AS sku_value,
  cat.id AS category_id, subcat.id AS sub_category_id
FROM "order" p
INNER JOIN customer c ON c.id = p.customer_id
INNER JOIN order_items i ON i.order_id = p.id
INNER JOIN product s ON s.id = i.sku
LEFT JOIN category cat ON cat.id = s.category_id
LEFT JOIN sub_category subcat ON subcat.id = cat.sub_category_id;
    `)
    .then((result) => {
      const orders = {};
  
      result.rows.forEach(row => {
        if (!orders[row.order_id]) {
          orders[row.order_id] = {
            uuid: row.order_id,
            created_at: row.created_at,
            type: row.type,
            customer: {
              id: row.customer_id,
              name: row.customer_name,
            },
            items: [],
          };
        }
  
        orders[row.order_id].items.push({
          id: row.item_id,
          sku: {
            id: row.sku_id,
            value: parseFloat(row.sku_value) || 0, 
          },
          quantity: row.quantity,
          category: {
            id: row.category_id || null, 
            sub_category: {
              id: row.sub_category_id || null, 
            },
          },
        });
      });
  
      return Object.values(orders);
    })
    .catch((err) => {
      console.error('Erro ao consultar o banco de dados:', err);
      throw new Error('Erro ao consultar o banco de dados');
    });
  }
  
  fetchOrderById(id) {
    return this.client.query(`
      SELECT
        p.id AS order_id, p.created_at, p.type,
        c.id AS customer_id, c.name AS customer_name,
        i.id AS item_id, i.quantity,
        s.id AS sku_id, s.value AS sku_value,
        cat.id AS category_id, subcat.id AS sub_category_id
      FROM "order" p
      INNER JOIN customer c ON c.id = p.customer_id
      INNER JOIN order_items i ON i.order_id = p.id
      INNER JOIN product s ON s.id = i.sku
      LEFT JOIN category cat ON cat.id = s.category_id
      LEFT JOIN sub_category subcat ON subcat.id = cat.sub_category_id
      WHERE p.id = $1;  -- Utiliza o ID do pedido
    `, [id]) 
    .then((result) => {
      if (result.rows.length === 0) return null; 
  
      const order = {
        uuid: result.rows[0].order_id,
        created_at: result.rows[0].created_at,
        type: result.rows[0].type,
        customer: {
          id: result.rows[0].customer_id,
          name: result.rows[0].customer_name,
        },
        items: [],
      };
  
      result.rows.forEach(row => {
        order.items.push({
          id: row.item_id,
          sku: {
            id: row.sku_id,
            value: parseFloat(row.sku_value) || 0,
          },
          quantity: row.quantity,
          category: {
            id: row.category_id || null,
            sub_category: {
              id: row.sub_category_id || null,
            },
          },
        });
      });
  
      return order;
    })
    .catch((err) => {
      console.error('Erro ao consultar o banco de dados:', err);
      throw new Error('Erro ao consultar o banco de dados');
    });
  }
  
}

module.exports = DatabaseService;
