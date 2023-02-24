import { Adapter, BroadcastOptions } from 'socket.io-adapter';
import * as msgpack from 'notepack.io';
import * as uid2 from 'uid2';
import * as Debug from 'debug';
import { Kafka, Consumer, Producer, CompressionTypes } from 'kafkajs';
import { Namespace } from 'socket.io';

const debug = new Debug('socket.io-kafka-adapter');
const DEFAULT_KAFKA_ADAPTER_TOPIC = 'kafka_adapter';

enum RequestType {
    SOCKETS = 0,
    ALL_ROOMS = 1,
    REMOTE_JOIN = 2,
    REMOTE_LEAVE = 3,
    REMOTE_DISCONNECT = 4,
    REMOTE_FETCH = 5,
    SERVER_SIDE_EMIT = 6,
}
export interface KafkaAdapterOpts {
    topic: string
}

export function createAdapter(consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts) {
    debug('create kafka adapter');
    return function(nsp: Namespace) {
        return new KafkaAdapter(nsp, consumer, producer, opts);
    }
}

export class KafkaAdapter extends Adapter {

    private consumer: Consumer;
    private producer: Producer;
    private topic: string;
    private uid: any;

    constructor(nsp: Namespace, consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts) {
        super(nsp)
        this.topic = opts.topic || DEFAULT_KAFKA_ADAPTER_TOPIC;
        this.consumer = consumer;
        this.consumer.subscribe({
            topic: this.topic
        }).then(() => this.onmessage())
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
                    if (!(uid && packet && opts)) return debug('invalid params');
                    if (this.uid === uid) return debug('ignore same uid');

                    if (packet.nsp === undefined) packet.nsp = '/';
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
                } catch (error) {
                    return debug('error:', error);
                }
            },
        });
    }

    broadcast(packet: any, opts: BroadcastOptions) {
        try {
            packet.nsp = this.nsp.name;
            let local = opts && opts.flags && opts.flags.local;
            debug('local:', local);
            if (!local && this.producer) {
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
                }
                debug('producer send message:', pMessage);
                this.producer.send(pMessage);
            }
            super.broadcast(packet, opts); 
        } catch (error) {
            return debug('error:', error);
        }
    }

    async close() {
        debug('close adapter');
        if (this.consumer) {
            await this.consumer.stop();
            await this.consumer.disconnect();
        }
        if (this.producer) await this.producer.disconnect();
        process.exit(0);
    }

}