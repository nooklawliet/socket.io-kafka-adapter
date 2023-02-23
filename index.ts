import { Adapter, BroadcastOptions } from 'socket.io-adapter';
import * as msgpack from 'notepack.io';
import * as uid2 from 'uid2';
import * as Debug from 'debug';
import { Kafka, Consumer, Producer, CompressionTypes } from 'kafkajs';
import { Namespace } from 'socket.io';

const debug = new Debug('socket.io-kafka-adapter');
const KAFKA_ADAPTER_TOPIC = 'kafka_adapter';
const KAFKA_ADAPTER_SOCKET_TOPIC = 'kafka_adapter_socket';

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
    // topics: Array<string>
    groupId: string
    // fromBeginning: boolean
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
    topic: string | Array<string>;
    uid: any;

    constructor(nsp: Namespace, consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts) {
        super(nsp)
        this.consumer = consumer;
        this.consumer.subscribe({
            topics: [KAFKA_ADAPTER_TOPIC, KAFKA_ADAPTER_SOCKET_TOPIC]
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
                    const args = msgpack.decode(msg);
                    debug('args:', args);
                    const [uid, packet, opts] = args;
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
            debug('broadcast opt:', rawOpts);
            const msg = msgpack.encode([this.uid, packet, rawOpts]);
            let key = this.uid;
            if (opts.rooms && opts.rooms.size === 1) {
                key = opts.rooms.keys().next().value;
            }
            const produceMessage = {
                topic: KAFKA_ADAPTER_TOPIC,
                messages: [{
                    key: key,
                    value: msg
                }],
            }
            debug('producer send message:', produceMessage);
            this.producer.send(produceMessage);
            super.broadcast(packet, opts); 
        } catch (error) {
            return debug('error:', error);
        }
    }

}