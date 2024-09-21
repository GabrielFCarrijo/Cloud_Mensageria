const express = require('express');
const bodyParser = require('body-parser');
const PubSubService = require('./PubSubService');

const app = express();
const pubSubService = new PubSubService();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.post('/mensagem', (req, res) => {
  pubSubService.listenForMessages('projects/serjava-demo/subscriptions/carrijo-sub', 60);
  res.status(200).json({ mensagem: 'Processamento iniciado' });
});

app.get('/orders', async (req, res) => {
  const { uuid, customer_id, product_id } = req.query;

  const filters = {
    uuid,
    customer_id,
    product_id,
  };

  try {
    const orders = await pubSubService.getOrders(filters);
    res.status(200).json(orders);
  } catch (err) {
    console.error('Erro ao consultar os pedidos:', err);
    res.status(500).json({ mensagem: 'Erro ao consultar o banco de dados' });
  }
});


app.get('/orders/:id', async (req, res) => {
  const { id } = req.params;
  const { sku_id } = req.query;

  try {
    const result = await pubSubService.getOrder(id, sku_id);
    if (result) {
      res.status(200).json(result);
    } else {
      res.status(404).json({ mensagem: 'Pedido não encontrado' });
    }
  } catch (err) {
    console.error('Erro ao consultar o pedido:', err);
    res.status(500).json({ mensagem: 'Erro ao consultar o banco de dados' });
  }
});



async function startServer() {
  try {
    await pubSubService.connectDB(); 
    app.listen(3000, () => {
      console.log('Servidor rodando na porta 3000');
    });
  } catch (err) {
    console.error('Erro ao conectar ao PostgreSQL', err);
  }
}

startServer();
