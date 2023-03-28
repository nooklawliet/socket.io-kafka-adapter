import { Adapter, BroadcastOptions, Room } from 'socket.io-adapter';
import { Consumer, Producer } from 'kafkajs';
import { Namespace } from 'socket.io';
export interface KafkaAdapterOpts {
    /**
     * topics are the categories used to organize messages.
     */
    topic: string;
    /**
     * after this timeout the adapter will stop waiting from responses to request.
     * @default 5000
     */
    requestsTimeout: number;
}
export declare function createAdapter(consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts): (nsp: Namespace) => KafkaAdapter;
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
    /**
     * Adapter constructor.
     *
     * @param nsp - the namespace
     * @param consumer - a client that reads data from one or more Kafka topics
     * @param producer - a client that writes data to one or more Kafka topics
     * @param opts - additional options
     *
     * @public
     */
    constructor(nsp: Namespace, consumer: Consumer, producer: Producer, opts: KafkaAdapterOpts);
    /**
    * Called with a subscription message
    *
    * @private
    */
    private onmessage;
    /**
     * Called on request from another node
     *
     * @private
     */
    private onrequest;
    /**
     * Called on response from another node
     *
     * @private
     */
    private onresponse;
    /**
     * Send the response to the requesting node
     * @param response
     *
     * @private
     */
    private publishResponse;
    /**
     * Broadcasts a packet.
     *
     * @param {Object} packet - packet to emit
     * @param {Object} opts - options
     *
     * @public
     */
    broadcast(packet: any, opts: BroadcastOptions): any;
    broadcastWithAck(packet: any, opts: BroadcastOptions, clientCountCallback: (clientCount: number) => void, ack: (...args: any[]) => void): void;
    /**
     * Makes the matching socket instances join the specified rooms
     *
     * @param opts - the filters to apply
     * @param rooms - the rooms to join
     */
    addSockets(opts: BroadcastOptions, rooms: Room[]): void;
    /**
     * Makes the matching socket instances leave the specified rooms
     *
     * @param opts - the filters to apply
     * @param rooms - the rooms to leave
     */
    delSockets(opts: BroadcastOptions, rooms: Room[]): void;
    /**
     * Makes the matching socket instances disconnect
     *
     * @param opts - the filters to apply
     * @param close - whether to close the underlying connection
     */
    disconnectSockets(opts: BroadcastOptions, close: boolean): void;
    /**
     * Send a packet to the other Socket.IO servers in the cluster
     * @param packet - an array of arguments
     */
    serverSideEmit(packet: any[]): void;
    /**
    * Send a packet to the other Socket.IO servers in the cluster
    * @param packet - an array of arguments, which may include an acknowledgement callback at the end
    */
    private serverSideEmitWithAck;
    /**
     * Gets the list of all rooms (across every node)
     *
     * @public
     */
    allRooms(): Promise<Set<Room>>;
    /**
     * Returns the matching socket instances
     *
     * @param opts - the filters to apply
     */
    fetchSockets(opts: BroadcastOptions): Promise<any[]>;
    /**
     * Returns lists of Socket.IO servers with kafka adapter
     */
    private fetchAdapter;
    /**
     * Returns number of Socket.IO servers with kafka adapter
     */
    private getNumSub;
    serverCount(): Promise<number>;
    close(): Promise<void>;
}
