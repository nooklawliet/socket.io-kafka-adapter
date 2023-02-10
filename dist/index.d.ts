import { Adapter, BroadcastOptions } from 'socket.io-adapter';
import { Consumer, Producer } from 'kafkajs';
import { Namespace } from 'socket.io';
export interface KafkaAdapterOpts {
    groupId: string;
}
export declare function createAdapter(consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts): (nsp: Namespace) => KafkaAdapter;
export declare class KafkaAdapter extends Adapter {
    private consumer;
    private producer;
    topic: string | Array<string>;
    uid: any;
    constructor(nsp: Namespace, consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts);
    onmessage(): Promise<void>;
    broadcast(packet: any, opts: BroadcastOptions): any;
    addSockets(opts: any, rooms: any): void;
    delSockets(opts: any, rooms: any): void;
    disconnectSockets(opts: any, close: any): void;
    serverSideEmit(packet: any): void;
}
