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
    topics: Array<string> | string;
    uid: any;
    constructor(nsp: Namespace, consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts);
    onmessage(): Promise<void>;
    broadcast(packet: any, opts: BroadcastOptions): void;
}
