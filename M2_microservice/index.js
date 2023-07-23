const amqp = require('amqplib');

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

    console.log('M2 Microservice waiting for tasks...');

    // Consume tasks from the input queue
    channel.consume(inputQueue, (message) => {
      const task = JSON.parse(message.content.toString());
      console.log('M2 Microservice received task:', task);

      // Simulate some processing delay
      setTimeout(() => {
        const result = { correlationId: task.correlationId, status: 'completed', result: 'Some result data' };

        // Publish the result to the output queue
        channel.sendToQueue(outputQueue, Buffer.from(JSON.stringify(result)));

        console.log('M2 Microservice sent result to RabbitMQ:', result);

        // Acknowledge the task to remove it from the queue
        channel.ack(message);
      }, 3000);
    });
  })
  .catch((error) => {
    console.error('Error connecting to RabbitMQ:', error);
  });
