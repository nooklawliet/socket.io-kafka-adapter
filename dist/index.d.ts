import { Adapter, BroadcastOptions, Room } from 'socket.io-adapter';
import { Kafka } from 'kafkajs';
import { Namespace } from 'socket.io';
export interface KafkaAdapterOpts {
    topic: string;
    groupId: string;
    requestsTimeout: number;
}
export declare function createAdapter(kafka: Kafka, opts: KafkaAdapterOpts): (nsp: Namespace) => KafkaAdapter;
export declare class KafkaAdapter extends Adapter {
    private consumer;
    private producer;
    private admin;
    private groupId;
    private adapterTopic;
    private requestTopic;
    private responseTopic;
    private uid;
    private requestsTimeout;
    private requests;
    private ackRequests;
    constructor(nsp: Namespace, kafka: Kafka, opts: KafkaAdapterOpts);
    private initConsumer;
    private initProducer;
    private initAdmin;
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
    fetchSockets(opts: BroadcastOptions): Promise<any[]>;
    private fetchAdapter;
    private getNumSub;
    close(): Promise<void>;
}
