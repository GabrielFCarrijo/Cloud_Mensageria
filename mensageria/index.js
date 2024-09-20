const express = require('express');
const bodyParser = require('body-parser');
const PubSubService = require('./PubSubService');
const DatabaseService = require('./DatabaseService');

const app = express();
const pubSubService = new PubSubService();
const dbService = new DatabaseService();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.post('/mensagem', (req, res) => {
  pubSubService.listenForMessages('projects/serjava-demo/subscriptions/carrijo-sub', 60);
  res.status(200).json({ mensagem: 'Processamento iniciado' });
});

app.get('/orders', (req, res) => {
  dbService.fetchOrders()
    .then((result) => res.status(200).json(result))
    .catch((err) => res.status(500).json({ mensagem: 'Erro ao consultar o banco de dados' }));
});

app.get('/orders/:id', (req, res) => {
  const { id } = req.params;
  
  dbService.fetchOrderById(id)
    .then((result) => {
      if (result) {
        res.status(200).json(result);
      } else {
        res.status(404).json({ mensagem: 'Pedido não encontrado' });
      }
    })
    .catch((err) => res.status(500).json({ mensagem: 'Erro ao consultar o banco de dados' }));
});


app.listen(3000, () => console.log('Server running on port 3000'));