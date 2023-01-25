const { Server } = require('socket.io');
const http = require('http');
const { Kafka } = require('kafkajs');
const KafkaAdapter = require('../dist/index');

const port = 3000;
const server = http.createServer();
const io = new Server(server);

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});
const opts = {
    topics: ['socketio']
}
const consumer = kafka.consumer({ groupId: 'groupId' });
const producer = kafka.producer();


Promise.all([consumer.connect(), producer.connect()]).then(() => {
    io.adapter(KafkaAdapter.createAdapter(consumer, producer, opts));
    io.listen(port);
});

// io.adapter(KafkaAdapter.createAdapter(kafka, opts));
io.on('connection', async (socket) => {
    console.log(`client: ${socket.id} connected`);

    socket.on('disconnect', (reason) => {
        console.log(`client: ${socket.id} disconnected`);
        console.log('reason:', reason);
    });
});

io.on('error', (err) => {
    console.log('error:', err)
});
// server.listen(port);
console.log('start server port:', port);