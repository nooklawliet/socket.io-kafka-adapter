import { Adapter, BroadcastOptions, Room } from 'socket.io-adapter';
import * as msgpack from 'notepack.io';
import * as uid2 from 'uid2';
import * as Debug from 'debug';
import { Kafka, Consumer, Producer, Partitioners, Admin } from 'kafkajs';
import { Namespace } from 'socket.io';

const debug = new Debug('socket.io-kafka-adapter');
const DEFAULT_KAFKA_ADAPTER_TOPIC = 'kafka_adapter';
const DEFAULT_REQUEST_TIMEOUT = 5000;

enum RequestType {
    SOCKETS = 0,
    ALL_ROOMS = 1,
    REMOTE_JOIN = 2,
    REMOTE_LEAVE = 3,
    REMOTE_DISCONNECT = 4,
    REMOTE_FETCH = 5,
    SERVER_SIDE_EMIT = 6,
    REMOTE_FETCH_ADAPTER = 7,
    BROADCAST,
    BROADCAST_CLIENT_COUNT,
    BROADCAST_ACK,
}

interface Request {
    type: RequestType;
    resolve: Function;
    timeout: NodeJS.Timeout;
    numSub?: number;
    msgCount?: number;
    adapters?: any;
    [other: string]: any;
}

interface AckRequest {
    clientCountCallback: (clientCount: number) => void;
    ack: (...args: any[]) => void;
}
export interface KafkaAdapterOpts {
    topic: string;
    groupId: string;
    requestsTimeout: number;
}

export function createAdapter(kafka: Kafka, opts: KafkaAdapterOpts) {
    debug('create kafka adapter');
    return function(nsp: Namespace) {
        return new KafkaAdapter(nsp, kafka, opts);
    }
}

export class KafkaAdapter extends Adapter {

    private consumer: Consumer;
    private producer: Producer;
    private admin: Admin;
    private groupId: string;
    private adapterTopic: string;
    private requestTopic: string;
    private responseTopic: string;
    private uid: any;
    private requestsTimeout: number;
    private requests: Map<string, Request> = new Map();
    private ackRequests: Map<string, AckRequest> = new Map();

    constructor(nsp: Namespace, kafka: Kafka, opts: KafkaAdapterOpts) {
        super(nsp)
        this.uid = uid2(6);
        this.groupId = opts.groupId;
        this.adapterTopic = opts.topic || DEFAULT_KAFKA_ADAPTER_TOPIC;
        this.requestTopic = this.adapterTopic + '_request';
        this.responseTopic = this.adapterTopic + '_response';
        this.requestsTimeout = opts.requestsTimeout || DEFAULT_REQUEST_TIMEOUT;
        
        this.initConsumer(kafka, opts);
        this.initProducer(kafka);
        this.initAdmin(kafka);
       
        process.on("SIGINT", this.close.bind(this));
        process.on("SIGQUIT", this.close.bind(this));
        process.on("SIGTERM", this.close.bind(this));
    }

    private async initConsumer(kafka: Kafka, opts: KafkaAdapterOpts) {
        this.consumer = kafka.consumer({ groupId: opts.groupId });
        await this.consumer.connect();
        await this.consumer.subscribe({
            topics: [this.adapterTopic, this.requestTopic, this.responseTopic]
        });
        await this.consumer.run({
            eachMessage: async (payload) => {
                // debug('consumer recieved message:', payload);
                if (payload.topic === this.adapterTopic) {
                    this.onmessage(payload);
                } else if (payload.topic === this.requestTopic) {
                    this.onrequest(payload);
                } else if (payload.topic === this.responseTopic) {
                    this.onresponse(payload);
                }
            }
        });
    }

    private async initProducer(kafka: Kafka) {
        this.producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });
        await this.producer.connect();
    }

    private async initAdmin(kafka: Kafka) {
        this.admin = kafka.admin();
        await this.admin.connect();
    }

    private async onmessage({ message }) {
        try {
            debug('---------- ON Message ----------');
            const args = msgpack.decode(message.value);
            debug('args:', args);
            const [uid, packet, opts] = args;
            if (!(uid && packet && opts)) return debug('invalid params');
            if (this.uid === uid) return debug('ignore same uid');

            if (packet.nsp === undefined) packet.nsp = '/';
            if (packet.nsp !== this.nsp.name) {
                return debug('ignore different namespace');
            }

            if (opts.rooms && opts.rooms.length === 1) {
                const room = opts.rooms[0];
                debug('room:', room);
                debug('this.rooms:', this.rooms);
                debug('has.room:', this.rooms.has(room));
                if (room !== '' && !this.rooms.has(room)) {
                    return debug('ignore unknown room:', room);
                }
            }
            opts.rooms = new Set(opts.rooms);
            opts.except = new Set(opts.except);
            super.broadcast(packet, opts);
        } catch (error) {
            return debug('error:', error);
        }
    }

    private async onrequest({ message }) {
        try {
            debug('---------- ON Request ----------');
            debug('message:', message);

            let msg;
            try {
                // if the buffer starts with a "{" character
                if (message.value[0] === 0x7b) {
                    msg = JSON.parse(message.value.toString());
                } else {
                    msg = msgpack.decode(message.value);
                }
            } catch (err) {
                debug("ignoring malformed request");
                return;
            }

            let [uid, request] = msg;
            if (!(uid && request)) return debug('invalid params');
            request = JSON.parse(request);
            debug('JSON request:', request);
            
            let socket, response;
            switch (request.type) {

                case RequestType.SOCKETS:
                    debug('---------- RequestType: SOCKETS ----------');
                    if (this.requests.has(request.requestId)) {
                        return;
                    }
                    const sockets = await super.sockets(new Set(request.rooms));
    
                    response = JSON.stringify({
                        requestId: request.requestId,
                        sockets: [...sockets],
                    });
                    debug('response:', response)
                    this.publishResponse(response);
                    break;
                
                case RequestType.ALL_ROOMS:
                    debug('---------- RequestType: ALL_ROOMS ----------');
                    if (this.requests.has(request.requestId)) {
                        return;
                    }
                    response = JSON.stringify({
                        requestId: request.requestId,
                        rooms: [...this.rooms.keys()],
                    });
                    debug('response:', response)
                    this.publishResponse(response);
                    break;

                case RequestType.REMOTE_JOIN:
                    debug('---------- RequestType: REMOTE_JOIN ----------');
                    debug('this.room:', this.rooms);
                    if (request.opts) {
                        const opts = {
                            rooms: new Set<Room>(request.opts.rooms),
                            except: new Set<Room>(request.opts.except),
                        };
                        debug('opts:', opts);
                        debug('request.rooms:', request.rooms);
                        debug('this.room:', this.rooms);
                        return super.addSockets(opts, request.rooms);
                    }
                    debug('request.sid:', request.sid);
                    socket = this.nsp.sockets.get(request.sid);
                    debug('socket:', socket);
                    if (!socket) {
                        return;
                    }
                    socket.join(request.room);
                    response = JSON.stringify({
                        requestId: request.requestId,
                    });
                    debug('response:', response);

                    this.publishResponse(response);
                    break;

                case RequestType.REMOTE_LEAVE:
                    debug('---------- RequestType: REMOTE_LEAVE ----------');
                    if (request.opts) {
                        const opts = {
                            rooms: new Set<Room>(request.opts.rooms),
                            except: new Set<Room>(request.opts.except),
                        };
                        debug('opts:', opts)
                        return super.delSockets(opts, request.rooms);
                    }
                    debug('request.sid:', request.sid);
                    socket = this.nsp.sockets.get(request.sid);
                    debug('socket:', socket)
                    if (!socket) {
                        return;
                    }
                    socket.leave(request.room);
                    response = JSON.stringify({
                        requestId: request.requestId,
                    });
                    this.publishResponse(response);
                    break;

                case RequestType.REMOTE_DISCONNECT:
                    debug('---------- RequestType: REMOTE_DISCONNECT ----------');
                    if (request.opts) {
                        const opts = {
                            rooms: new Set<Room>(request.opts.rooms),
                            except: new Set<Room>(request.opts.except),
                        };
                        debug('opts:', opts)
                        return super.disconnectSockets(opts, request.close);
                    }
                    debug('request.sid:', request.sid);
                    socket = this.nsp.sockets.get(request.sid);
                    debug('socket:', socket)
                    if (!socket) {
                        return;
                    }
                    socket.disconnect(request.close);
                    response = JSON.stringify({
                        requestId: request.requestId,
                    });
                    this.publishResponse(response);
                    break;

                case RequestType.REMOTE_FETCH:   
                    debug('---------- RequestType: REMOTE_FETCH ----------');
                    debug('request.requestId:', request.requestId);
                    // debug('this.requests:', this.requests);
                    if (this.requests.has(request.requestId)) {
                        debug('ignore self');
                        return;
                    }

                    const opts1 = {
                        rooms: new Set<Room>(request.opts.rooms),
                        except: new Set<Room>(request.opts.except),
                    };
                    debug('opts1:', opts1);
                    const localSockets = await super.fetchSockets(opts1);

                    response = JSON.stringify({
                        requestId: request.requestId,
                        sockets: localSockets.map((socket) => {
                            // remove sessionStore from handshake, as it may contain circular references
                            const { sessionStore, ...handshake } = socket.handshake;
                            return {
                                id: socket.id,
                                handshake,
                                rooms: [...socket.rooms],
                                data: socket.data,
                            };
                        }),
                    });
                    debug('response:', response);

                    this.publishResponse(response);
                    break; 

                case RequestType.REMOTE_FETCH_ADAPTER:   
                    debug('---------- RequestType: REMOTE_FETCH_ADAPTER ----------');
                    debug('request.requestId:', request.requestId);
                    // debug('this.requests:', this.requests);
                    if (this.requests.has(request.requestId)) {
                        debug('ignore self');
                        return;
                    }

                    debug('this:', this);
                    response = JSON.stringify({
                        requestId: request.requestId,
                        adapters: {
                            uid: this.uid,
                            groupId: this.groupId,
                        }
                    });
                    debug('response:', response);

                    this.publishResponse(response);
                    break; 

                case RequestType.SERVER_SIDE_EMIT:
                    debug('---------- RequestType: SERVER_SIDE_EMIT ----------');
                    if (request.uid === this.uid) {
                        debug('ignore same uid');
                        return;
                    }
                    const requestId = request.requestId;
                    const withAck = requestId !== undefined;
                    debug('withAck', withAck);
                    if (!withAck) {
                        this.nsp._onServerSideEmit(request.data);
                        return;
                    }
                    let called = false;
                    const callback = (arg) => {
                        // only one argument is expected
                        if (called) {
                            return;
                        }
                        called = true;
                        debug('calling acknowledgement with %j', arg);

                        const request = JSON.stringify({
                            type: RequestType.SERVER_SIDE_EMIT,
                            requestId: requestId,
                            data: arg,
                        });
                        debug('SERVER_SIDE_EMIT callback request:', request);
                        const msg = msgpack.encode([this.uid, request]);
                        const pMessage = {
                            topic: this.responseTopic,
                            messages: [{
                                key: this.uid,
                                value: msg
                            }]
                        }
                        debug('producer send message:', pMessage);
                        this.producer.send(pMessage);
                    };
                    request.data.push(callback);
                    this.nsp._onServerSideEmit(request.data);
                    break;

                case RequestType.BROADCAST: 
                    debug('---------- RequestType: BROADCAST ----------');
                    if (this.ackRequests.has(request.requestId)) {
                        // ignore self
                        return;
                    }
    
                    const opts = {
                        rooms: new Set<Room>(request.opts.rooms),
                        except: new Set<Room>(request.opts.except),
                    };
    
                    super.broadcastWithAck(request.packet, opts, (clientCount) => {
                            debug('waiting for %d client acknowledgements', clientCount);
                            this.publishResponse(JSON.stringify({
                                type: RequestType.BROADCAST_CLIENT_COUNT,
                                requestId: request.requestId,
                                clientCount,
                            }));
                        }, (arg) => {
                            debug('received acknowledgement with value %j', arg);
                            this.publishResponse(msgpack.encode({
                                type: RequestType.BROADCAST_ACK,
                                requestId: request.requestId,
                                packet: arg,
                            }));
                        }
                    );
                    break;
                    
                default:
                    debug('ignoring unknown request type: %s', request.type);
                    break;
            }

        } catch (error) {
            return debug('error:', error);
        }
    }

    private onresponse({ message }) {
        debug('---------- ON Response ----------');
        debug('message:', message);
        // let response = JSON.parse(message.value);
        let request, msg;
        try {
            // if the buffer starts with a "{" character
            if (message.value[0] === 0x7b) {
                msg = JSON.parse(message.value.toString());
            } else {
                msg = msgpack.decode(message.value);
            }
        } catch (err) {
            debug("ignoring malformed response");
            return;
        }

        let [uid, response] = msg;
        if (!(uid && response)) return debug('invalid params');
        response = JSON.parse(response);
        debug('JSON response:', response);
    
        const requestId = response.requestId;
        debug('requestId:', requestId);
        if (this.ackRequests.has(requestId)) {
            const ackRequest = this.ackRequests.get(requestId);
            switch (response.type) {
                case RequestType.BROADCAST_CLIENT_COUNT: {
                    ackRequest?.clientCountCallback(response.clientCount);
                    break;
                }
                case RequestType.BROADCAST_ACK: {
                    ackRequest?.ack(response.packet);
                    break;
                }
            }
            return;
        }
    
        if (!requestId || !(this.requests.has(requestId) || this.ackRequests.has(requestId))) {
            return debug("ignoring unknown request");
        }
    
        debug("received response %j", response);
    
        request = this.requests.get(requestId);
        debug('request:', request);
    
        switch (request.type) {
            case RequestType.SOCKETS:
            case RequestType.REMOTE_FETCH:
                debug('---------- ResponseType: SOCKETS, REMOTE_FETCH ----------');

                request.msgCount++;
        
                // ignore if response does not contain 'sockets' key
                if (!response.sockets || !Array.isArray(response.sockets)) return;
        
                if (request.type === RequestType.SOCKETS) {
                    response.sockets.forEach((s) => request.sockets.add(s));
                } else {
                    response.sockets.forEach((s) => request.sockets.push(s));
                }
        
                if (request.msgCount === request.numSub) {
                    clearTimeout(request.timeout);
                    if (request.resolve) {
                        request.resolve(request.sockets);
                    }
                    this.requests.delete(requestId);
                }
                break;

            case RequestType.REMOTE_FETCH_ADAPTER:
                debug('---------- ResponseType: REMOTE_FETCH_ADAPTER ----------');
                debug('response.adapter:', response);
                if (response.adapters) {
                    request.adapters.push(response.adapters);
                }
                break;
        
            case RequestType.ALL_ROOMS:
                debug('---------- ResponseType: ALL_ROOMS ----------');
                request.msgCount++;
        
                // ignore if response does not contain 'rooms' key
                if (!response.rooms || !Array.isArray(response.rooms)) return;
        
                response.rooms.forEach((s) => request.rooms.add(s));
        
                if (request.msgCount === request.numSub) {
                    clearTimeout(request.timeout);
                    if (request.resolve) {
                        request.resolve(request.rooms);
                    }
                    this.requests.delete(requestId);
                }
                break;
        
            case RequestType.REMOTE_JOIN:
            case RequestType.REMOTE_LEAVE:
            case RequestType.REMOTE_DISCONNECT:
                debug('---------- ResponseType: REMOTE_JOIN, REMOTE_LEAVE, REMOTE_DISCONNECT ----------');
                clearTimeout(request.timeout);
                if (request.resolve) {
                    request.resolve();
                }
                this.requests.delete(requestId);
                break;
        
            case RequestType.SERVER_SIDE_EMIT:
                debug('---------- ResponseType: SERVER_SIDE_EMIT ----------');
                request.responses.push(response.data);
        
                debug("serverSideEmit: got %d responses out of %d", request.responses.length, request.numSub);
                if (request.responses.length === request.numSub) {
                    clearTimeout(request.timeout);
                    if (request.resolve) {
                        request.resolve(null, request.responses);
                    }
                    this.requests.delete(requestId);
                }
                break;
        
            default:
                debug("ignoring unknown request type: %s", request.type);
        }
    }

    private publishResponse(response) {
        debug('publishing response:', response);
        const msg = msgpack.encode([this.uid, response]);
        const pMessage = {
            topic: this.responseTopic,
            messages: [{
                key: this.uid,
                value: msg
            }]
        }
        debug('producer send message:', pMessage);
        this.producer.send(pMessage);
    }

    broadcast(packet: any, opts: BroadcastOptions) {
        try {
            debug('---------- Func: broadcast ----------');
            packet.nsp = this.nsp.name;
            let local = opts && opts.flags && opts.flags.local;
            debug('local:', local);
            if (!local && this.producer) {
                const rawOpts = {
                    rooms: [...opts.rooms],
                    except: [...new Set(opts.except)],
                    flags: opts.flags,
                };
                debug('uid:', this.uid);
                debug('packet:', packet);
                debug('opts:', rawOpts);
                const msg = msgpack.encode([this.uid, packet, rawOpts]);
                let key = this.uid;
                if (opts.rooms && opts.rooms.size === 1) {
                    key = opts.rooms.keys().next().value;
                }
                const pMessage = {
                    topic: this.adapterTopic,
                    messages: [{
                        key: key,
                        value: msg
                    }]
                }
                debug('producer send message:', pMessage);
                this.producer.send(pMessage);
            }
            super.broadcast(packet, opts); 
        } catch (error) {
            return debug('error:', error);
        }
    }

    public addSockets(opts: BroadcastOptions, rooms: Room[]) {
        debug('---------- Func: addSockets ----------');
        if (opts.flags?.local) {
            return super.addSockets(opts, rooms);
        }
        const request = JSON.stringify({
            type: RequestType.REMOTE_JOIN,
            opts: {
                rooms: [...opts.rooms],
                except: [opts.except],
            },
            rooms: [...rooms],
        });
        debug('add sockets request:', request);
        const msg = msgpack.encode([this.uid, request]);
        const pMessage = {
            topic: this.requestTopic,
            messages: [{
                key: this.uid,
                value: msg
            }]
        }
        debug('producer send message:', pMessage);
        this.producer.send(pMessage);
    }

    public delSockets(opts: BroadcastOptions, rooms: Room[]) {
        debug('---------- Func: delSockets ----------');
        if (opts.flags?.local) {
            return super.delSockets(opts, rooms);
        }
        const request = JSON.stringify({
            uid: this.uid,
            type: RequestType.REMOTE_LEAVE,
            opts: {
                rooms: [...opts.rooms],
                except: [opts.except],
            },
            rooms: [...rooms],
        });
        debug('del sockets request:', request);
        const msg = msgpack.encode([this.uid, request]);
        const pMessage = {
            topic: this.requestTopic,
            messages: [{
                key: this.uid,
                value: msg
            }]
        }
        debug('producer send message:', pMessage);
        this.producer.send(pMessage);
    }
    
    public disconnectSockets(opts: BroadcastOptions, close: boolean) {
        debug('---------- Func: disconnectSockets ----------');
        if (opts.flags?.local) {
            return super.disconnectSockets(opts, close);
        }
        const request = JSON.stringify({
            uid: this.uid,
            type: RequestType.REMOTE_DISCONNECT,
            opts: {
                rooms: [...opts.rooms],
                except: [opts.except],
            },
            close,
        });
        debug('dicconnect sockets request:', request);
        const msg = msgpack.encode([this.uid, request]);
        const pMessage = {
            topic: this.requestTopic,
            messages: [{
                key: this.uid,
                value: msg
            }]
        }
        debug('producer send message:', pMessage);
        this.producer.send(pMessage);
    }

    public serverSideEmit(packet: any[]): void {
        debug('---------- Func: serverSideEmit ----------');
        const withAck = typeof packet[packet.length - 1] === "function";
        if (withAck) {
            this.serverSideEmitWithAck(packet).catch(() => {
                // ignore errors
            });
            return;
        }
        const request = JSON.stringify({
            uid: this.uid,
            type: RequestType.SERVER_SIDE_EMIT,
            data: packet,
        });
        debug('server side emit request:', request);
        const msg = msgpack.encode([this.uid, request]);
        const pMessage = {
            topic: this.requestTopic,
            messages: [{
                key: this.uid,
                value: msg
            }]
        }
        debug('producer send message:', pMessage);
        this.producer.send(pMessage);
    }

    private async serverSideEmitWithAck(packet: any[]) {
        debug('---------- Func: serverSideEmitWithAck ----------');
        const ack = packet.pop();
        const numSub = (await this.getNumSub()) - 1; // ignore self
        debug('waiting for %d responses to "serverSideEmit" request', numSub);

        if (numSub <= 0) {
            return ack(null, []);
        }

        const requestId = uid2(6);
        const request = JSON.stringify({
            uid: this.uid,
            requestId, // the presence of this attribute defines whether an acknowledgement is needed
            type: RequestType.SERVER_SIDE_EMIT,
            data: packet,
        });

        const timeout = setTimeout(() => {
        const storedRequest = this.requests.get(requestId);
            if (storedRequest) {
                ack(new Error(`timeout reached: only ${storedRequest.responses.length} responses received out of ${storedRequest.numSub}`), storedRequest.responses);
                this.requests.delete(requestId);
            }
        }, this.requestsTimeout);

        this.requests.set(requestId, {
            type: RequestType.SERVER_SIDE_EMIT,
            numSub,
            timeout,
            resolve: ack,
            responses: [],
        });

        debug('request:', request);
        const msg = msgpack.encode([this.uid, request]);
        const pMessage = {
            topic: this.requestTopic,
            messages: [{
                key: this.uid,
                value: msg
            }]
        }
        debug('producer send message:', pMessage);
        this.producer.send(pMessage);
    }

    public async fetchSockets(opts: BroadcastOptions): Promise<any[]> {
        debug('---------- Func: fetchSockets ----------');
        const localSockets = await super.fetchSockets(opts);
        if (opts.flags?.local) {
            return localSockets;
        }
        const numSub = await this.getNumSub();
        debug('waiting for %d responses to "fetchSockets" request', numSub);
        if (numSub <= 1) {
            return localSockets;
        }
        const requestId = uid2(6);
        const request = JSON.stringify({
            uid: this.uid,
            requestId,
            type: RequestType.REMOTE_FETCH,
            opts: {
                rooms: [...opts.rooms],
                except: [opts.except],
            },
        });

        return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                if (this.requests.has(requestId)) {
                    reject(new Error("timeout reached while waiting for fetchSockets response"));
                    this.requests.delete(requestId);
                }
            }, this.requestsTimeout);

            this.requests.set(requestId, {
                type: RequestType.REMOTE_FETCH,
                numSub,
                resolve,
                timeout,
                msgCount: 1,
                sockets: localSockets,
            });

            debug('this.requests:', this.requests);

            debug('REMOTE_FETCH sockets request:', request);
            const msg = msgpack.encode([this.uid, request]);
            const pMessage = {
                topic: this.requestTopic,
                messages: [{
                    key: this.uid,
                    value: msg
                }]
            }
            debug('producer send message:', pMessage);
            this.producer.send(pMessage);
        });
    }

    private async fetchAdapter(): Promise<any[]> {
        debug('---------- Func: fetchAdapter ----------');

        const requestId = uid2(6);
        const request = JSON.stringify({
            uid: this.uid,
            requestId,
            type: RequestType.REMOTE_FETCH_ADAPTER
        });

        return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                debug('ON timeout');
                const request = this.requests.get(requestId);
                if (request) {
                    debug('request:', request);
                    resolve(request.adapters);
                    this.requests.delete(requestId);
                } else {
                    reject(new Error('Something wrong!!'))
                }
            }, 1000);

            this.requests.set(requestId, {
                type: RequestType.REMOTE_FETCH_ADAPTER,
                resolve,
                timeout,
                // msgCount: 1,
                adapters: [{
                    uid: this.uid,
                    groupId: this.groupId,
                }]
            });

            debug('this.requests:', this.requests);

            debug('REMOTE_FETCH_ADAPTER sockets request:', request);
            const msg = msgpack.encode([this.uid, request]);
            const pMessage = {
                topic: this.requestTopic,
                messages: [{
                    key: this.uid,
                    value: msg
                }]
            }
            debug('producer send message:', pMessage);
            this.producer.send(pMessage);
        });
    }

    private async getNumSub(): Promise<number> {
        const adapters = await this.fetchAdapter();
        return adapters.length;
    }

    async close() {
        debug('---------- Func: close ----------');
        if (this.consumer) {
            await this.consumer.stop();
            await this.consumer.disconnect();
        }
        if (this.producer) await this.producer.disconnect();
        process.exit(0);
    }

}