"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaAdapter = exports.createAdapter = void 0;
const socket_io_adapter_1 = require("socket.io-adapter");
const msgpack = require("notepack.io");
const uid2 = require("uid2");
const Debug = require("debug");
const debug = new Debug('socket.io-kafka-adapter');
const adapterStore = {};
const DEFAULT_KAFKA_TOPIC = 'kafka_adapter';
const uid = uid2(6);
function createAdapter(consumer, producer, opts) {
    debug('create kafka adapter');
    return function (nsp) {
        return new KafkaAdapter(nsp, consumer, producer, opts);
    };
}
exports.createAdapter = createAdapter;
class KafkaAdapter extends socket_io_adapter_1.Adapter {
    constructor(nsp, consumer, producer, opts) {
        super(nsp);
        this.topic = DEFAULT_KAFKA_TOPIC;
        this.consumer = consumer;
        this.consumer.subscribe({
            topic: this.topic
        }).then(() => this.onmessage());
        this.producer = producer;
        this.uid = uid2(6);
    }
    async onmessage() {
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                try {
                    debug('consumer recieved message', {
                        topic: topic,
                        partition: partition,
                        key: message.key,
                        value: message.value,
                        headers: message.headers,
                    });
                    const msg = message.value;
                    const args = msgpack.decode(msg);
                    debug('args:', args);
                    const [uid, packet, opts] = args;
                    if (this.uid === uid) {
                        return debug('ignore same uid');
                    }
                    opts.rooms = new Set(opts.rooms);
                    opts.except = new Set(opts.except);
                    super.broadcast(packet, opts);
                }
                catch (error) {
                    return debug('error:', error);
                }
            },
        });
    }
    broadcast(packet, opts) {
        try {
            packet.nsp = this.nsp.name;
            const rawOpts = {
                rooms: [...opts.rooms],
                except: [...new Set(opts.except)],
                flags: opts.flags,
            };
            debug('broadcast opt:', rawOpts);
            const msg = msgpack.encode([this.uid, packet, rawOpts]);
            let key = this.uid;
            if (opts.rooms && opts.rooms.size === 1) {
                key = opts.rooms.keys().next().value;
            }
            const produceMessage = {
                topic: this.topic,
                messages: [{
                        key: key,
                        value: msg
                    }],
            };
            debug('producer send message:', produceMessage);
            this.producer.send(produceMessage);
            super.broadcast(packet, opts);
        }
        catch (error) {
            return debug('error:', error);
        }
    }
}
exports.KafkaAdapter = KafkaAdapter;
//# sourceMappingURL=index.js.map