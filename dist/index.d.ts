import { Adapter, BroadcastOptions, Room } from 'socket.io-adapter';
import { Kafka } from 'kafkajs';
import { Namespace } from 'socket.io';
export interface KafkaAdapterOpts {
    topic: string;
    groupId: string;
}
export declare function createAdapter(kafka: Kafka, opts: KafkaAdapterOpts): (nsp: Namespace) => KafkaAdapter;
export declare class KafkaAdapter extends Adapter {
    private consumer;
    private producer;
    private admin;
    private adapterTopic;
    private requestTopic;
    private responseTopic;
    private uid;
    private requests;
    private ackRequests;
    constructor(nsp: Namespace, kafka: Kafka, opts: KafkaAdapterOpts);
    initConsumer(kafka: Kafka, opts: KafkaAdapterOpts): Promise<void>;
    initProducer(kafka: Kafka): Promise<void>;
    initAdmin(kafka: Kafka): Promise<void>;
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
    private getNumSub;
    serverCount(): Promise<number>;
    close(): Promise<void>;
}
