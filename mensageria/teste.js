const { Client } = require('pg');

// Configurações do banco de dados
const client = new Client({
  user: 'postgres',
  host: 'localhost',
  database: 'mensageria2',
  password: 'postgres',
  port: 5432,
});

// Função para processar mensagens
async function testMessageHandling(messageData) {
  const receivedMessage = JSON.parse(messageData);
  console.log(receivedMessage);

  const uuid = receivedMessage.uuid;
  const created_at = receivedMessage.created_at;
  const type = receivedMessage.type;
  const customerId = receivedMessage.customer.id;
  const customerName = receivedMessage.customer.name;

  try {
    await client.query(`
      INSERT INTO customer (id, name)
      VALUES ($1, $2)
      ON CONFLICT (id) DO NOTHING
    `, [customerId.toString(), customerName]);
    console.log('Registro do cliente inserido com sucesso');

    await client.query(`
      INSERT INTO "order" (id, created_at, type, customer_id)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (id) DO NOTHING
    `, [uuid.toString(), created_at, type, customerId.toString()]);
    console.log('Registro da ordem inserido com sucesso');

    for (const item of receivedMessage.items) {
      const itemsSkuId = item.sku.id; 
      const itemsSkuValue = item.sku.value; 
      const quantity = item.quantity;
  
      const categoryId = item.category.id;
      const subCategoryId = item.category.sub_category.id;

      await client.query(`
        INSERT INTO sub_category (id) 
        VALUES ($1) 
        ON CONFLICT (id) DO NOTHING
      `, [subCategoryId.toString()]);
      console.log('Subcategoria inserida ou já existente');

      await client.query(`
        INSERT INTO category (id, sub_category_id) 
        VALUES ($1, $2) 
        ON CONFLICT (id) DO NOTHING
      `, [categoryId.toString(), subCategoryId.toString()]);
      console.log('Categoria inserida ou já existente');
  
      await client.query(`
        INSERT INTO product (id, value, category_id) 
        VALUES ($1, $2, $3) 
        ON CONFLICT (id) DO NOTHING
      `, [itemsSkuId.toString(), itemsSkuValue, categoryId.toString()]);
      console.log('SKU inserido ou já existente');
  
      await client.query(`
        INSERT INTO order_itens (id, order_id, sku, quantity)
        VALUES ($1, $2, $3, $4)
      `, [item.id.toString(), uuid.toString(), itemsSkuId.toString(), quantity]);
      console.log('Registro do item inserido com sucesso');
    }
  
    const returnedObject = await getOrder(uuid);
    console.log('Objeto retornado:', returnedObject);
  } catch (err) {
    console.error('Erro ao inserir registro:', err);
  }
}

async function getOrder(orderId) {
  const res = await client.query(`
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
  `, [orderId]);

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

// Mensagem de teste
const testMessage = `{
   "uuid": "3030-499f-39f949",
   "created_at": "2023-09-01 22:33:00",
   "type": "AB",
   "customer": {
       "id": 99494,
       "name": "joao da silva"
   },
   "items": [
       {
           "id": 1,
           "sku": {
               "id": "3939-fdfd",
               "value": 33.99
           },
           "quantity": 2,
           "category": {
               "id": "AM",
               "sub_category": {
                   "id": "BCRU"
               }
           }
       },
       {
           "id": 44,
           "sku": {
               "id": "443-fdfd",
               "value": 299.99
           },
           "quantity": 2,
           "category": {
               "id": "BA",
               "sub_category": {
                   "id": "ABCD"
               }
           }
       },
       {
           "id": 3,
           "sku": {
               "id": "234-fdfd",
               "value": 120.00
           },
           "quantity": 1,
           "category": {
               "id": "ELE",
               "sub_category": {
                   "id": "CEL"
               }
           }
       },
       {
           "id": 8,
           "sku": {
               "id": "555-fdfd",
               "value": 45.75
           },
           "quantity": 4,
           "category": {
               "id": "AL",
               "sub_category": {
                   "id": "FRU"
               }
           }
       },
       {
           "id": 10,
           "sku": {
               "id": "666-fdfd",
               "value": 89.90
           },
           "quantity": 1,
           "category": {
               "id": "MODA",
               "sub_category": {
                   "id": "BLUSAS"
               }
           }
       },
       {
           "id": 15,
           "sku": {
               "id": "777-fdfd",
               "value": 59.99
           },
           "quantity": 3,
           "category": {
               "id": "ESP",
               "sub_category": {
                   "id": "FUT"
               }
           }
       },
       {
           "id": 21,
           "sku": {
               "id": "888-fdfd",
               "value": 199.50
           },
           "quantity": 1,
           "category": {
               "id": "ELE",
               "sub_category": {
                   "id": "NOTE"
               }
           }
       },
       {
           "id": 22,
           "sku": {
               "id": "999-fdfd",
               "value": 39.99
           },
           "quantity": 2,
           "category": {
               "id": "MODA",
               "sub_category": {
                   "id": "CALCAS"
               }
           }
       },
       {
           "id": 30,
           "sku": {
               "id": "111-fdfd",
               "value": 12.50
           },
           "quantity": 10,
           "category": {
               "id": "SUP",
               "sub_category": {
                   "id": "BEB"
               }
           }
       },
       {
           "id": 31,
           "sku": {
               "id": "222-fdfd",
               "value": 300.00
           },
           "quantity": 1,
           "category": {
               "id": "ELE",
               "sub_category": {
                   "id": "TV"
               }
           }
       }
   ]
}
`;

// Conectar ao banco de dados e testar a função
client.connect()
  .then(() => {
    console.log('Conectado ao PostgreSQL');
    testMessageHandling(testMessage)
      .then(() => client.end())
      .catch(err => {
        console.error('Erro no processamento da mensagem:', err);
        client.end();
      });
  })
  .catch(err => {
    console.error('Erro ao conectar ao PostgreSQL', err);
  });
