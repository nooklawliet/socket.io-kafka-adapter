"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaAdapter = exports.createAdapter = exports.KafkaAdapterOpts = void 0;
const socket_io_adapter_1 = require("socket.io-adapter");
const msgpack = require("notepack.io");
const uid2 = require("uid2");
const Debug = require("debug");
const kafkajs_1 = require("kafkajs");
const debug = new Debug('socket.io-kafka-adapter');
const adapterStore = {};
const DEFAULT_KAFKA_TOPIC = 'socketio';
const uid = uid2(6);
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
        this.topics = opts.topics;
        this.consumer = consumer;
        this.consumer.subscribe({
            topics: opts.topics || [DEFAULT_KAFKA_TOPIC]
        }).then(() => this.onmessage());
        this.producer = producer;
        this.uid = uid2(6);
    }
    async onmessage() {
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                var _a, _b;
                debug('consumer recieved message', {
                    topic: topic,
                    partition: partition,
                    key: (_a = message.key) === null || _a === void 0 ? void 0 : _a.toString(),
                    value: (_b = message.value) === null || _b === void 0 ? void 0 : _b.toString(),
                    headers: message.headers,
                });
                const msg = message.value;
                debug('msg:', msg);
                const args = msgpack.decode(msg);
                const [uid, packet, opts] = args;
                opts.rooms = new Set(opts.rooms);
                opts.except = new Set(opts.except);
                debug('uid:', uid);
                debug('packet:', packet);
                debug('opts:', opts);
                super.broadcast(packet, opts);
            },
        });
    }
    broadcast(packet, opts) {
        packet.nsp = this.nsp.name;
        const rawOpts = {
            rooms: [...opts.rooms],
            except: [...new Set(opts.except)],
            flags: opts.flags,
        };
        debug('rawOpts:', rawOpts);
        const msg = msgpack.encode([this.uid, packet, rawOpts]);
        let key = this.uid;
        if (opts.rooms && opts.rooms.size === 1) {
            key = opts.rooms.keys().next().value;
        }
        const topic = this.topics[0];
        const produceMessage = {
            topic: topic,
            messages: [{
                    key: key,
                    value: msg
                }],
            acks: 0,
            timeout: 30000,
            compression: kafkajs_1.CompressionTypes.GZIP,
        };
        debug('produce message:', produceMessage);
        this.producer.send(produceMessage);
        super.broadcast(packet, opts);
    }
}
exports.KafkaAdapter = KafkaAdapter;
//# sourceMappingURL=index.js.map