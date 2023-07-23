const express = require('express');
const amqp = require('amqplib');

const app = express();
const port = 3000;
const rabbitMQUrl = 'amqp://localhost'; // Update this if your RabbitMQ instance is hosted elsewhere

// Connect to RabbitMQ and create a channel
amqp.connect(rabbitMQUrl)
  .then((connection) => connection.createChannel())
  .then((channel) => {
    const inputQueue = 'tasks';
    const outputQueue = 'results';

    // Create the input queue to consume tasks
    channel.assertQueue(inputQueue, { durable: false });

    // Create the output queue to publish results
    channel.assertQueue(outputQueue, { durable: false });

    app.use(express.json());

    // Route to receive HTTP requests
    app.post('', (req, res) => {
      const task = req.body;

      // Generate a unique correlationId to match request and response
      const correlationId = generateCorrelationId();

      // Publish the task to the RabbitMQ input queue with the correlationId
      channel.sendToQueue(inputQueue, Buffer.from(JSON.stringify({ task, correlationId })));

      console.log('Task sent to RabbitMQ:', task);

      // Consume results from the output queue with the matching correlationId
      channel.consume(outputQueue, (message) => {
        const result = JSON.parse(message.content.toString());

        if (result.correlationId === correlationId) {
          console.log('Received result from RabbitMQ:', result);

          // Acknowledge the message
          channel.ack(message);

          // Send the result as the HTTP response
          res.json(result);
        }
      }, { noAck: false });
    });

    app.listen(port, () => {
      console.log(`M1 Microservice listening at http://localhost:${port}`);
    });
  })
  .catch((error) => {
    console.error('Error connecting to RabbitMQ:', error);
  });

// Function to generate a unique correlationId
function generateCorrelationId() {
  return Math.random().toString(36).slice(2);
}
