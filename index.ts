import { Adapter, BroadcastOptions } from 'socket.io-adapter';
import * as msgpack from 'notepack.io';
import * as uid2 from 'uid2';
import * as Debug from 'debug';
import { Kafka, Consumer, Producer, CompressionTypes } from 'kafkajs';
import { Namespace } from 'socket.io';

const debug = new Debug('socket.io-kafka-adapter')
const adapterStore: { [ns: string]: Adapter } = {}
const DEFAULT_KAFKA_TOPIC = 'socketio';
const uid = uid2(6);

export class KafkaAdapterOpts {
    topics: Array<string>
    groupId: string
    // fromBeginning: boolean
}

export function createAdapter(consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts) {
    debug('create kafka adapter');
    return function(nsp: Namespace) {
        debug('return kafka adapter');
        return new KafkaAdapter(nsp, consumer, producer, opts);
    }
}

export class KafkaAdapter extends Adapter {

    private consumer: Consumer;
    private producer: Producer;
    topics: Array<string> | string;
    uid: any;

    constructor(nsp: Namespace, consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts) {
        super(nsp)
        this.topics = opts.topics;
        this.consumer = consumer;
        this.consumer.subscribe({
            topics: opts.topics || [DEFAULT_KAFKA_TOPIC]
        }).then(() => this.onmessage())
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
                    debug('msg:', msg);
                    debug('msg:', msg?.toString());
                    const args = msgpack.decode(msg);
                    debug('args:', args);
                    const [uid, packet, opts] = args;
                    debug('uid:', uid);
                    debug('this.uid:', this.uid);
                    debug('packet:', packet);
                    debug('opts:', opts);

                    if (this.uid === uid) {
                        return debug('ignore same uid');
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
                compression: CompressionTypes.GZIP,
            }
            debug('produce message:', produceMessage);
            this.producer.send(produceMessage);
            super.broadcast(packet, opts); 
        } catch (error) {
            return debug('error:', error);
        }
    }
}