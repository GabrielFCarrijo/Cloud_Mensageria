const { client } = require('pg');
const { v4: uuidv4 } = require('uuid');

class databaseservice {
  constructor() {
    this.client = new client({
      user: 'postgres',
      host: 'localhost',
      database: 'subclient',
      password: 'postgres',
      port: 5432,
    });

    this.client.connect()
      .then(() => console.log('conectado ao postgresql'))
      .catch((err) => console.error('erro ao conectar ao postgresql', err));
  }

  insertcustomer(customer) {
    return this.client.query(`
      insert into customer (id, name)
      values ($1, $2)
      on conflict (id) do update
      set name = excluded.name;
    `, [customer.id, customer.name]);
  }

  insertorder(order, customer) {
    return this.insertcustomer(customer)
      .then(() => {
        return this.client.query(`
          insert into "order" (id, created_at, type, customer_id)
          values ($1, $2, $3, $4)
          on conflict (id) do update
          set created_at = excluded.created_at,
              type = excluded.type,
              customer_id = excluded.customer_id;
        `, [order.id, order.createdat, order.type, customer.id]);
      });
  }

  insertproduct(sku, value) {
    return this.client.query(`
      insert into product (id, value)
      values ($1, $2)
      on conflict (id) do update
      set value = excluded.value;
    `, [sku, value]);
  }

  insertorderitem(id, orderid, sku, quantity) {
    return this.client.query(`
      insert into order_items (id, order_id, sku, quantity)
      values ($1, $2, $3, $4)
      on conflict (id) do update
      set order_id = excluded.order_id,
          sku = excluded.sku,
          quantity = excluded.quantity;
    `, [id, orderid, sku, quantity]);
  }

  fetchorders() {
    return this.client.query(`
      select
  p.id as order_id, p.created_at, p.type,
  c.id as customer_id, c.name as customer_name,
  i.id as item_id, i.quantity,
  s.id as sku_id, s.value as sku_value,
  cat.id as category_id, subcat.id as sub_category_id
from "order" p
inner join customer c on c.id = p.customer_id
inner join order_items i on i.order_id = p.id
inner join product s on s.id = i.sku
left join category cat on cat.id = s.category_id
left join sub_category subcat on subcat.id = cat.sub_category_id;
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
            value: parsefloat(row.sku_value) || 0, 
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
  
      return object.values(orders);
    })
    .catch((err) => {
      console.error('erro ao consultar o banco de dados:', err);
      throw new error('erro ao consultar o banco de dados');
    });
  }
  
  fetchorderbyid(id) {
    return this.client.query(`
      select
        p.id as order_id, p.created_at, p.type,
        c.id as customer_id, c.name as customer_name,
        i.id as item_id, i.quantity,
        s.id as sku_id, s.value as sku_value,
        cat.id as category_id, subcat.id as sub_category_id
      from "order" p
      inner join customer c on c.id = p.customer_id
      inner join order_items i on i.order_id = p.id
      inner join product s on s.id = i.sku
      left join category cat on cat.id = s.category_id
      left join sub_category subcat on subcat.id = cat.sub_category_id
      where p.id = $1;  -- utiliza o id do pedido
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
            value: parsefloat(row.sku_value) || 0,
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
      console.error('erro ao consultar o banco de dados:', err);
      throw new error('erro ao consultar o banco de dados');
    });
  }
  
}

module.exports = databaseservice;
