const { v4: uuidv4 } = require('uuid');

class Order {
  constructor(id, createdAt, type, customerId) {
    this.id = id || uuidv4();  // Gera um UUID válido se não for fornecido
    this.createdAt = createdAt;
    this.type = type;
    this.customerId = customerId;
  }
}

module.exports = Order;
  