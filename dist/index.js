"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaAdapter = exports.createAdapter = exports.KafkaAdapterOpts = void 0;
const socket_io_adapter_1 = require("socket.io-adapter");
// import * as msgpack from 'notepack.io';
// import * as uid2 from 'uid2';
const Debug = require("debug");
const debug = new Debug('socket.io-kafka-adapter');
const adapterStore = {};
const DEFAULT_KAFKA_TOPIC = 'socketio';
// const uid = uid2(6)
class KafkaAdapterOpts {
}
exports.KafkaAdapterOpts = KafkaAdapterOpts;
function createAdapter(consumer, producer, opts) {
    debug('create kafka adapter');
    return function (nsp) {
        debug('return kafka adapter');
        return new KafkaAdapter(nsp, consumer, producer, opts);
    };
}
exports.createAdapter = createAdapter;
class KafkaAdapter extends socket_io_adapter_1.Adapter {
    constructor(nsp, consumer, producer, opts) {
        super(nsp);
        this.consumer = consumer;
        this.consumer.subscribe({
            topics: opts.topics || [DEFAULT_KAFKA_TOPIC]
        }).then(() => this.onmessage());
        this.producer = producer;
    }
    async onmessage() {
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
                var _a, _b;
                debug('consumer recieved message', {
                    topic: topic,
                    partition: partition,
                    key: (_a = message.key) === null || _a === void 0 ? void 0 : _a.toString(),
                    value: (_b = message.value) === null || _b === void 0 ? void 0 : _b.toString(),
                    headers: message.headers,
                });
            },
        });
    }
    getConsumer() {
        return this.consumer;
    }
    getProducer() {
        return this.producer;
    }
    broadcast(packet, opts) {
        debug('broadcast message %o', packet);
    }
}
exports.KafkaAdapter = KafkaAdapter;
//# sourceMappingURL=index.js.map