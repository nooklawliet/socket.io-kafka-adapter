import { Adapter, BroadcastOptions } from 'socket.io-adapter';
import { Consumer, Producer } from 'kafkajs';
import { Namespace } from 'socket.io';
export interface KafkaAdapterOpts {
    topic: string;
}
export declare function createAdapter(consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts): (nsp: Namespace) => KafkaAdapter;
export declare class KafkaAdapter extends Adapter {
    private consumer;
    private producer;
    private topic;
    private uid;
    constructor(nsp: Namespace, consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts);
    onmessage(): Promise<void>;
    broadcast(packet: any, opts: BroadcastOptions): any;
    close(): Promise<void>;
}
