import { Adapter, BroadcastOptions } from 'socket.io-adapter';
// import * as msgpack from 'notepack.io';
// import * as uid2 from 'uid2';
import * as Debug from 'debug';
import { Kafka, Consumer, Producer } from 'kafkajs';
import { Namespace } from 'socket.io';

const debug = new Debug('socket.io-kafka-adapter')
const adapterStore: { [ns: string]: Adapter } = {}
const DEFAULT_KAFKA_TOPIC = 'socketio';
// const uid = uid2(6)

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

    constructor(nsp: Namespace, consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts) {
        super(nsp)
        this.consumer = consumer;
        this.consumer.subscribe({
            topics: opts.topics || [DEFAULT_KAFKA_TOPIC]
        }).then(() => this.onmessage())
        this.producer = producer;
    }

    async onmessage() {
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
                debug('consumer recieved message', {
                    topic: topic,
                    partition: partition,
                    key: message.key?.toString(),
                    value: message.value?.toString(),
                    headers: message.headers,
                });
            },
        });
    }

    getConsumer(): Consumer {
        return this.consumer;
    }

    getProducer(): Producer {
        return this.producer;
    }

    broadcast(packet: any, opts: BroadcastOptions) {
        debug('broadcast message %o', packet);
    }

}