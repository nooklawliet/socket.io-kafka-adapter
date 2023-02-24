"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaAdapter = exports.createAdapter = void 0;
const socket_io_adapter_1 = require("socket.io-adapter");
const msgpack = require("notepack.io");
const uid2 = require("uid2");
const Debug = require("debug");
const debug = new Debug('socket.io-kafka-adapter');
const DEFAULT_KAFKA_ADAPTER_TOPIC = 'kafka_adapter';
var RequestType;
(function (RequestType) {
    RequestType[RequestType["SOCKETS"] = 0] = "SOCKETS";
    RequestType[RequestType["ALL_ROOMS"] = 1] = "ALL_ROOMS";
    RequestType[RequestType["REMOTE_JOIN"] = 2] = "REMOTE_JOIN";
    RequestType[RequestType["REMOTE_LEAVE"] = 3] = "REMOTE_LEAVE";
    RequestType[RequestType["REMOTE_DISCONNECT"] = 4] = "REMOTE_DISCONNECT";
    RequestType[RequestType["REMOTE_FETCH"] = 5] = "REMOTE_FETCH";
    RequestType[RequestType["SERVER_SIDE_EMIT"] = 6] = "SERVER_SIDE_EMIT";
})(RequestType || (RequestType = {}));
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
        this.topic = opts.topic || DEFAULT_KAFKA_ADAPTER_TOPIC;
        this.consumer = consumer;
        this.consumer.subscribe({
            topic: this.topic
        }).then(() => this.onmessage());
        this.producer = producer;
        this.uid = uid2(6);
        process.on("SIGINT", this.close.bind(this));
        process.on("SIGQUIT", this.close.bind(this));
        process.on("SIGTERM", this.close.bind(this));
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
                    const args = msgpack.decode(message.value);
                    debug('args:', args);
                    const [uid, packet, opts] = args;
                    if (!(uid && packet && opts))
                        return debug('invalid params');
                    if (this.uid === uid)
                        return debug('ignore same uid');
                    if (packet.nsp === undefined)
                        packet.nsp = '/';
                    if (packet.nsp !== this.nsp.name) {
                        return debug('ignore different namespace');
                    }
                    if (opts.rooms && opts.rooms.length === 1) {
                        const room = opts.rooms[0];
                        debug('room:', room);
                        debug('this.rooms:', this.rooms);
                        debug('has.room:', this.rooms.has(room));
                        if (room !== '' && !this.rooms.has(room)) {
                            return debug('ignore unknown room:', room);
                        }
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
            let onlyLocal = opts && opts.flags && opts.flags.local;
            debug('onlyLocal:', onlyLocal);
            if (!onlyLocal && this.producer) {
                const rawOpts = {
                    rooms: [...opts.rooms],
                    except: [...new Set(opts.except)],
                    flags: opts.flags,
                };
                debug('uid:', this.uid);
                debug('packet:', packet);
                debug('opts:', rawOpts);
                const msg = msgpack.encode([this.uid, packet, rawOpts]);
                let key = this.uid;
                if (opts.rooms && opts.rooms.size === 1) {
                    key = opts.rooms.keys().next().value;
                }
                const pMessage = {
                    topic: this.topic,
                    messages: [{
                            key: key,
                            value: msg
                        }]
                };
                debug('producer send message:', pMessage);
                this.producer.send(pMessage);
            }
            // const rawOpts = {
            //     rooms: [...opts.rooms],
            //     except: [...new Set(opts.except)],
            //     flags: opts.flags,
            // };
            // debug('broadcast opt:', rawOpts);
            // const msg = msgpack.encode([this.uid, packet, rawOpts]);
            // let key = this.uid;
            // if (opts.rooms && opts.rooms.size === 1) {
            //     key = opts.rooms.keys().next().value;
            // }
            // const produceMessage = {
            //     topic: this.topic,
            //     messages: [{
            //         key: key,
            //         value: msg
            //     }],
            // }
            // debug('producer send message:', produceMessage);
            // this.producer.send(produceMessage);
            super.broadcast(packet, opts);
        }
        catch (error) {
            return debug('error:', error);
        }
    }
    async close() {
        debug('close adapter');
        if (this.consumer) {
            await this.consumer.stop();
            await this.consumer.disconnect();
        }
        if (this.producer)
            await this.producer.disconnect();
        process.exit(0);
    }
}
exports.KafkaAdapter = KafkaAdapter;
//# sourceMappingURL=index.js.map