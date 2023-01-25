import { Adapter, BroadcastOptions } from 'socket.io-adapter';
import { Consumer, Producer } from 'kafkajs';
import { Namespace } from 'socket.io';
export declare class KafkaAdapterOpts {
    topics: Array<string>;
    groupId: string;
}
export declare function createAdapter(consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts): (nsp: Namespace) => KafkaAdapter;
export declare class KafkaAdapter extends Adapter {
    private consumer;
    private producer;
    constructor(nsp: Namespace, consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts);
    onmessage(): Promise<void>;
    getConsumer(): Consumer;
    getProducer(): Producer;
    broadcast(packet: any, opts: BroadcastOptions): void;
}
