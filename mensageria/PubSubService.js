const { PubSub } = require('@google-cloud/pubsub');
const DatabaseService = require('./DatabaseService');
const Customer = require('./models/Customer');
const Order = require('./models/Order');
const OrderItem = require('./models/OrderItem');
const path = require('path');

const keyFile = path.join(__dirname, 'config', 'serjava-demo-841daa14ddce.json');

class PubSubService {
  constructor() {
    this.pubSubClient = new PubSub({
      projectId: 'serjava-demo',
      keyFilename: keyFile,
    });
    this.dbService = new DatabaseService();
  }

  listenForMessages(subscriptionNameOrId, timeout) {
    const subscription = this.pubSubClient.subscription(subscriptionNameOrId);
    let messageCount = 0;

    const messageHandler = async (message) => {
      console.log(`Received message ${message.id}`);
      let receivedMessage;

      try {
        receivedMessage = JSON.parse(message.data.toString());
      } catch (error) {
        console.error('Failed to parse message data:', message.data.toString());
        message.ack();  
        return;
      }

      console.log('Received Message Data:', receivedMessage);

      if (receivedMessage.customer && receivedMessage.customer.id) {
        const customer = new Customer(receivedMessage.customer.id, receivedMessage.customer.name);
        const order = new Order(receivedMessage.uuid, receivedMessage.created_at, receivedMessage.type, customer.id);

        await this.dbService.insertOrder(order, customer);

        const productPromises = receivedMessage.items.map(item => {
          if (item.sku && item.sku.id && item.sku.value) {
            return this.dbService.insertProduct(item.sku.id, item.sku.value); 
          } else {
            console.error('Item inválido:', item);
            return Promise.resolve();  
          }
        });

        await Promise.all(productPromises);

        const orderItemPromises = receivedMessage.items.map(item => {
          return this.dbService.insertOrderItem(item.id, order.id, item.sku.id, item.quantity);
        });

        await Promise.all(orderItemPromises);
      } else {
        console.error('Invalid message structure:', receivedMessage);
      }

      message.ack();
      messageCount += 1;
    };

    subscription.on('message', messageHandler);

    setTimeout(() => {
      subscription.removeListener('message', messageHandler);
      console.log(`${messageCount} message(s) received.`);
    }, timeout * 1000);
  }
}


module.exports = PubSubService;
