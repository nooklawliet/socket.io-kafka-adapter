import { Adapter, BroadcastOptions, Room } from 'socket.io-adapter';
import { Consumer, Producer } from 'kafkajs';
import { Namespace } from 'socket.io';
export interface KafkaAdapterOpts {
    topic: string;
}
export declare function createAdapter(consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts): (nsp: Namespace) => KafkaAdapter;
export declare class KafkaAdapter extends Adapter {
    private consumer;
    private producer;
    private adapterTopic;
    private requestTopic;
    private responseTopic;
    private uid;
    private requests;
    private ackRequests;
    constructor(nsp: Namespace, consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts);
    private onmessage;
    private onrequest;
    private onresponse;
    private publishResponse;
    broadcast(packet: any, opts: BroadcastOptions): any;
    addSockets(opts: BroadcastOptions, rooms: Room[]): void;
    delSockets(opts: BroadcastOptions, rooms: Room[]): void;
    disconnectSockets(opts: BroadcastOptions, close: boolean): void;
    serverSideEmit(packet: any[]): void;
    private serverSideEmitWithAck;
    close(): Promise<void>;
}
