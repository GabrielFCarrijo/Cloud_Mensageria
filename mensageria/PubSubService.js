const { PubSub } = require('@google-cloud/pubsub');
const { Client } = require('pg');
const path = require('path');

class PubSubService {
  constructor() {
    this.projectId = "serjava-demo";
    this.keyFile = path.join(__dirname, 'config', 'serjava-demo-841daa14ddce.json');
    this.pubSubClient = new PubSub({ projectId: this.projectId, keyFilename: this.keyFile });
    
    this.client = new Client({
      user: 'postgres',
      host: 'localhost',
      database: 'mensageria2',
      password: 'postgres',
      port: 5432,
    });
  }

  async connectDB() {
    await this.client.connect();
    console.log('Conectado ao PostgreSQL');
  }

  listenForMessages(subscriptionNameOrId, timeout) {
    const subscription = this.pubSubClient.subscription(subscriptionNameOrId);
    let messageCount = 0;

    const messageHandler = async (message) => {
      console.log(`Received message ${message.id}:`);
      const rawData = message.data.toString();
      console.log(`Raw Data: ${rawData}`);

      try {
        const sanitizedData = rawData.replace(/[\x00-\x1F\x7F-\x9F]/g, '');
        await this.testMessageHandling(sanitizedData);
        message.ack();
      } catch (error) {
        console.error('Erro ao processar mensagem:', error);
        message.nack();
      }
      messageCount += 1;
    };

    subscription.on('message', messageHandler);

    setTimeout(() => {
      subscription.removeListener('message', messageHandler);
      console.log(`${messageCount} message(s) received.`);
    }, timeout * 1000);
  }

  async testMessageHandling(messageData) {
    const receivedMessage = JSON.parse(messageData);
    console.log(receivedMessage);

    const uuid = receivedMessage.uuid;
    const created_at = receivedMessage.created_at;
    const type = receivedMessage.type;
    const customerId = receivedMessage.customer.id;
    const customerName = receivedMessage.customer.name;

    try {
      await this.client.query(`
        INSERT INTO customer (id, name)
        VALUES ($1, $2)
      `, [customerId.toString(), customerName]);
      console.log('Registro do cliente inserido com sucesso');

      await this.client.query(`
        INSERT INTO "order" (id, created_at, type, customer_id)
        VALUES ($1, $2, $3, $4)
      `, [uuid.toString(), created_at, type, customerId.toString()]);
      console.log('Registro da ordem inserido com sucesso');

      for (const item of receivedMessage.items) {
        const itemsSkuId = item.sku.id; 
        const itemsSkuValue = item.sku.value; 
        const quantity = item.quantity;
        const categoryId = item.category.id;
        const subCategoryId = item.category.sub_category.id;

        await this.client.query(`
          INSERT INTO sub_category (id) 
          VALUES ($1) 
          ON CONFLICT (id) DO NOTHING
        `, [subCategoryId.toString()]); 
        console.log('Subcategoria inserida ou já existente');

        await this.client.query(`
          INSERT INTO category (id, sub_category_id) 
          VALUES ($1, $2) 
          ON CONFLICT (id) DO NOTHING
        `, [categoryId.toString(), subCategoryId.toString()]); 
        console.log('Categoria inserida ou já existente');

        await this.client.query(`
          INSERT INTO product (id, value, category_id) 
          VALUES ($1, $2, $3) 
          ON CONFLICT (id) DO NOTHING
        `, [itemsSkuId.toString(), itemsSkuValue, categoryId.toString()]); 
        console.log('SKU inserido ou já existente');

        await this.client.query(`
          INSERT INTO order_itens (id, order_id, sku, quantity)
          VALUES ($1, $2, $3, $4)
        `, [item.id.toString(), uuid.toString(), itemsSkuId.toString(), quantity]); 
        console.log('Registro do item inserido com sucesso');
      }

      const returnedObject = await this.getOrder(uuid);
      console.log('Objeto retornado:', returnedObject);
    } catch (err) {
      console.error('Erro ao inserir registro:', err);
    }
  }

  async getOrders(filters) {
    let query = `
      SELECT o.id AS order_id, o.created_at, o.type, c.id AS customer_id, c.name AS customer_name,
             SUM(i.quantity * p.value) AS total_value,
             i.quantity, p.id AS sku_id, p.value, 
             cat.id AS category_id, sub.id AS sub_category_id
      FROM "order" o
      JOIN customer c ON o.customer_id = c.id
      LEFT JOIN order_itens i ON o.id = i.order_id
      LEFT JOIN product p ON i.sku = p.id
      LEFT JOIN category cat ON p.category_id = cat.id
      LEFT JOIN sub_category sub ON cat.sub_category_id = sub.id
      WHERE 1=1
    `;
    
    const params = [];
    const conditions = [];
  
    if (filters.uuid) {
      conditions.push(`o.id = $${conditions.length + 1}`);
      params.push(filters.uuid);
    }
    if (filters.customer_id) {
      conditions.push(`c.id = $${conditions.length + 1}`);
      params.push(filters.customer_id);
    }
    if (filters.product_id) {
      conditions.push(`p.id = $${conditions.length + 1}`);
      params.push(filters.product_id);
    }
  
    if (conditions.length > 0) {
      query += ' AND ' + conditions.join(' AND ');
    }
    
    query += ' GROUP BY o.id, c.id, i.quantity, p.id, cat.id, sub.id';
  
    const res = await this.client.query(query, params);
  
    return res.rows.map(row => ({
      order_id: row.order_id,
      created_at: row.created_at,
      type: row.type,
      customer: {
        id: row.customer_id,
        name: row.customer_name,
      },
      total_value: parseFloat(row.total_value) || 0,
      items: row.sku_id ? [{
        sku: {
          id: row.sku_id,
          value: row.value,
        },
        quantity: row.quantity,
        category: {
          id: row.category_id,
          sub_category: {
            id: row.sub_category_id,
          },
        },
      }] : [],
    }));
  }
  
  async getOrder(orderId, skuId) {
    let query = `
      SELECT o.id AS order_id, o.created_at, o.type, c.id AS customer_id, c.name AS customer_name, 
             i.quantity, p.id AS sku_id, p.value, 
             cat.id AS category_id, sub.id AS sub_category_id
      FROM "order" o
      JOIN customer c ON o.customer_id = c.id
      LEFT JOIN order_itens i ON o.id = i.order_id
      LEFT JOIN product p ON i.sku = p.id
      LEFT JOIN category cat ON p.category_id = cat.id
      LEFT JOIN sub_category sub ON cat.sub_category_id = sub.id
      WHERE o.id = $1
    `;
  
    const params = [orderId];
  
    if (skuId) {
      query += ' AND p.id = $2';
      params.push(skuId);
    }
  
    const res = await this.client.query(query, params);
  
    if (res.rows.length === 0) {
      console.error('Nenhuma ordem encontrada com o ID:', orderId);
      return null; 
    }
  
    const order = {
      uuid: res.rows[0].order_id,
      created_at: res.rows[0].created_at,
      type: res.rows[0].type,
      customer: {
        id: res.rows[0].customer_id,
        name: res.rows[0].customer_name,
      },
      items: res.rows.map(row => ({
        id: row.sku_id,
        sku: {
          id: row.sku_id,
          value: row.value,
        },
        quantity: row.quantity,
        category: {
          id: row.category_id,
          sub_category: {
            id: row.sub_category_id,
          },
        },
      })),
    };
  
    return order;
  }
  
}

module.exports = PubSubService;
