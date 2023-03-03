"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaAdapter = exports.createAdapter = void 0;
const socket_io_adapter_1 = require("socket.io-adapter");
const msgpack = require("notepack.io");
const uid2 = require("uid2");
const Debug = require("debug");
const debug = new Debug('socket.io-kafka-adapter');
const DEFAULT_KAFKA_ADAPTER_TOPIC = 'kafka_adapter';
var RequestType;
(function (RequestType) {
    RequestType[RequestType["SOCKETS"] = 0] = "SOCKETS";
    RequestType[RequestType["ALL_ROOMS"] = 1] = "ALL_ROOMS";
    RequestType[RequestType["REMOTE_JOIN"] = 2] = "REMOTE_JOIN";
    RequestType[RequestType["REMOTE_LEAVE"] = 3] = "REMOTE_LEAVE";
    RequestType[RequestType["REMOTE_DISCONNECT"] = 4] = "REMOTE_DISCONNECT";
    RequestType[RequestType["REMOTE_FETCH"] = 5] = "REMOTE_FETCH";
    RequestType[RequestType["SERVER_SIDE_EMIT"] = 6] = "SERVER_SIDE_EMIT";
    RequestType[RequestType["BROADCAST"] = 7] = "BROADCAST";
    RequestType[RequestType["BROADCAST_CLIENT_COUNT"] = 8] = "BROADCAST_CLIENT_COUNT";
    RequestType[RequestType["BROADCAST_ACK"] = 9] = "BROADCAST_ACK";
})(RequestType || (RequestType = {}));
function createAdapter(consumer, producer, opts) {
    debug('create kafka adapter');
    return function (nsp) {
        return new KafkaAdapter(nsp, consumer, producer, opts);
    };
}
exports.createAdapter = createAdapter;
class KafkaAdapter extends socket_io_adapter_1.Adapter {
    constructor(nsp, consumer, producer, opts) {
        super(nsp);
        this.requests = new Map();
        this.ackRequests = new Map();
        this.adapterTopic = opts.topic || DEFAULT_KAFKA_ADAPTER_TOPIC;
        this.requestTopic = this.adapterTopic + '_request';
        this.responseTopic = this.responseTopic + '_response';
        this.consumer = consumer;
        this.consumer.subscribe({
            topics: [this.adapterTopic, this.requestTopic, this.responseTopic]
        }).then(async () => {
            await this.consumer.run({
                eachMessage: async (payload) => {
                    debug('consumer recieved message:', payload);
                    if (payload.topic === this.adapterTopic) {
                        this.onmessage(payload);
                    }
                    else if (payload.topic === this.requestTopic) {
                        this.onrequest(payload);
                    }
                    else if (payload.topic === this.responseTopic) {
                        this.onresponse(payload);
                    }
                }
            });
        });
        this.producer = producer;
        this.uid = uid2(6);
        process.on("SIGINT", this.close.bind(this));
        process.on("SIGQUIT", this.close.bind(this));
        process.on("SIGTERM", this.close.bind(this));
    }
    async onmessage({ message }) {
        try {
            const args = msgpack.decode(message.value);
            debug('args:', args);
            const [uid, packet, opts] = args;
            if (!(uid && packet && opts))
                return debug('invalid params');
            if (this.uid === uid)
                return debug('ignore same uid');
            if (packet.nsp === undefined)
                packet.nsp = '/';
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
        }
        catch (error) {
            return debug('error:', error);
        }
    }
    async onrequest({ message }) {
        try {
            const args = msgpack.decode(message.value);
            debug('args:', args);
            // let request;
            // try {
            //     // if the buffer starts with a "{" character
            //     if (msg[0] === 0x7b) {
            //         request = JSON.parse(msg.toString());
            //     } else {
            //         request = msgpack.decode(msg);
            //     }
            // } catch (err) {
            //     debug("ignoring malformed request");
            //     return;
            // }
            let [uid, request] = args;
            if (!(uid && request))
                return debug('invalid params');
            // const request = JSON.parse(args[1]);
            request = JSON.parse(request);
            debug('json request:', request);
            let socket, response;
            switch (request.type) {
                case RequestType.SOCKETS:
                    debug('RequestType: SOCKETS');
                    if (this.requests.has(request.requestId)) {
                        return;
                    }
                    const sockets = await super.sockets(new Set(request.rooms));
                    response = JSON.stringify({
                        requestId: request.requestId,
                        sockets: [...sockets],
                    });
                    debug('response:', response);
                    // this.publishResponse(request, response);
                    break;
                case RequestType.ALL_ROOMS:
                    debug('RequestType: ALL_ROOMS');
                    if (this.requests.has(request.requestId)) {
                        return;
                    }
                    response = JSON.stringify({
                        requestId: request.requestId,
                        rooms: [...this.rooms.keys()],
                    });
                    debug('response:', response);
                    // this.publishResponse(request, response);
                    break;
                case RequestType.REMOTE_JOIN:
                    debug('RequestType: REMOTE_JOIN');
                    debug('this.room:', this.rooms);
                    if (request.opts) {
                        const opts = {
                            rooms: new Set(request.opts.rooms),
                            except: new Set(request.opts.except),
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
                    // this.publishResponse(request, response);
                    break;
                case RequestType.REMOTE_LEAVE:
                    debug('RequestType: REMOTE_LEAVE');
                    if (request.opts) {
                        const opts = {
                            rooms: new Set(request.opts.rooms),
                            except: new Set(request.opts.except),
                        };
                        debug('opts:', opts);
                        return super.delSockets(opts, request.rooms);
                    }
                    debug('request.sid:', request.sid);
                    socket = this.nsp.sockets.get(request.sid);
                    debug('socket:', socket);
                    if (!socket) {
                        return;
                    }
                    socket.leave(request.room);
                    response = JSON.stringify({
                        requestId: request.requestId,
                    });
                    // this.publishResponse(request, response);
                    break;
                case RequestType.REMOTE_DISCONNECT:
                    debug('RequestType: REMOTE_DISCONNECT');
                    if (request.opts) {
                        const opts = {
                            rooms: new Set(request.opts.rooms),
                            except: new Set(request.opts.except),
                        };
                        debug('opts:', opts);
                        return super.disconnectSockets(opts, request.close);
                    }
                    debug('request.sid:', request.sid);
                    socket = this.nsp.sockets.get(request.sid);
                    debug('socket:', socket);
                    if (!socket) {
                        return;
                    }
                    socket.disconnect(request.close);
                    response = JSON.stringify({
                        requestId: request.requestId,
                    });
                    // this.publishResponse(request, response);
                    break;
                case RequestType.REMOTE_FETCH:
                    break;
                case RequestType.SERVER_SIDE_EMIT:
                    debug('RequestType: SERVER_SIDE_EMIT');
                    if (request.uid === this.uid) {
                        debug('ignore same uid');
                        return;
                    }
                    const withAck = request.requestId !== undefined;
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
                        // this.pubClient.publish(this.responseChannel, JSON.stringify({
                        //     type: RequestType.SERVER_SIDE_EMIT,
                        //     requestId: request.requestId,
                        //     data: arg,
                        // }));
                    };
                    request.data.push(callback);
                    this.nsp._onServerSideEmit(request.data);
                    break;
                case RequestType.BROADCAST:
                    if (this.ackRequests.has(request.requestId)) {
                        // ignore self
                        return;
                    }
                    const opts = {
                        rooms: new Set(request.opts.rooms),
                        except: new Set(request.opts.except),
                    };
                    super.broadcastWithAck(request.packet, opts, (clientCount) => {
                        debug('waiting for %d client acknowledgements', clientCount);
                        this.publishResponse(request, JSON.stringify({
                            type: RequestType.BROADCAST_CLIENT_COUNT,
                            requestId: request.requestId,
                            clientCount,
                        }));
                    }, (arg) => {
                        debug('received acknowledgement with value %j', arg);
                        this.publishResponse(request, msgpack.encode({
                            type: RequestType.BROADCAST_ACK,
                            requestId: request.requestId,
                            packet: arg,
                        }));
                    });
                    break;
                default:
                    debug('ignoring unknown request type: %s', request.type);
                    break;
            }
        }
        catch (error) {
            return debug('error:', error);
        }
    }
    onresponse({ message }) {
        //     let response = JSON.parse(message.value);
        //     // let request;
        //     // try {
        //     //     // if the buffer starts with a "{" character
        //     //     if (msg[0] === 0x7b) {
        //     //         response = JSON.parse(msg.toString());
        //     //     } else {
        //     //         response = this.parser.decode(msg);
        //     //     }
        //     // } catch (err) {
        //     //     debug("ignoring malformed response");
        //     //     return;
        //     // }
        //     const requestId = response.requestId;
        //     if (this.ackRequests.has(requestId)) {
        //         const ackRequest = this.ackRequests.get(requestId);
        //         switch (response.type) {
        //             case RequestType.BROADCAST_CLIENT_COUNT: {
        //                 ackRequest?.clientCountCallback(response.clientCount);
        //                 break;
        //             }
        //             case RequestType.BROADCAST_ACK: {
        //                 ackRequest?.ack(response.packet);
        //                 break;
        //             }
        //         }
        //         return;
        //     }
        //     if (!requestId || !(this.requests.has(requestId) || this.ackRequests.has(requestId))) {
        //         return debug("ignoring unknown request");
        //     }
        //     debug("received response %j", response);
        //     const request = this.requests.get(requestId);
        //     switch (request.type) {
        //         case RequestType.SOCKETS:
        //         case RequestType.REMOTE_FETCH:
        //             request.msgCount++;
        //             // ignore if response does not contain 'sockets' key
        //             if (!response.sockets || !Array.isArray(response.sockets)) return;
        //             if (request.type === RequestType.SOCKETS) {
        //                 response.sockets.forEach((s) => request.sockets.add(s));
        //             } else {
        //                 response.sockets.forEach((s) => request.sockets.push(s));
        //             }
        //             if (request.msgCount === request.numSub) {
        //                 clearTimeout(request.timeout);
        //                 if (request.resolve) {
        //                     request.resolve(request.sockets);
        //                 }
        //                 this.requests.delete(requestId);
        //             }
        //             break;
        //         case RequestType.ALL_ROOMS:
        //             request.msgCount++;
        //             // ignore if response does not contain 'rooms' key
        //             if (!response.rooms || !Array.isArray(response.rooms)) return;
        //             response.rooms.forEach((s) => request.rooms.add(s));
        //             if (request.msgCount === request.numSub) {
        //                 clearTimeout(request.timeout);
        //                 if (request.resolve) {
        //                     request.resolve(request.rooms);
        //                 }
        //                 this.requests.delete(requestId);
        //             }
        //             break;
        //         case RequestType.REMOTE_JOIN:
        //         case RequestType.REMOTE_LEAVE:
        //         case RequestType.REMOTE_DISCONNECT:
        //             clearTimeout(request.timeout);
        //             if (request.resolve) {
        //                 request.resolve();
        //             }
        //             this.requests.delete(requestId);
        //             break;
        //         case RequestType.SERVER_SIDE_EMIT:
        //             request.responses.push(response.data);
        //             debug("serverSideEmit: got %d responses out of %d", request.responses.length, request.numSub);
        //             if (request.responses.length === request.numSub) {
        //                 clearTimeout(request.timeout);
        //                 if (request.resolve) {
        //                     request.resolve(null, request.responses);
        //                 }
        //                 this.requests.delete(requestId);
        //             }
        //             break;
        //         default:
        //             debug("ignoring unknown request type: %s", request.type);
        //     }
    }
    publishResponse(request, response) {
        // const responseChannel = this.publishOnSpecificResponseChannel
        //   ? `${this.responseChannel}${request.uid}#`
        //   : this.responseChannel;
        // debug("publishing response to channel %s", responseChannel);
        // this.pubClient.publish(responseChannel, response);
    }
    broadcast(packet, opts) {
        try {
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
                };
                debug('producer send message:', pMessage);
                this.producer.send(pMessage);
            }
            super.broadcast(packet, opts);
        }
        catch (error) {
            return debug('error:', error);
        }
    }
    addSockets(opts, rooms) {
        var _a;
        debug('add sockets');
        if ((_a = opts.flags) === null || _a === void 0 ? void 0 : _a.local) {
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
        };
        debug('producer send message:', pMessage);
        this.producer.send(pMessage);
    }
    delSockets(opts, rooms) {
        var _a;
        if ((_a = opts.flags) === null || _a === void 0 ? void 0 : _a.local) {
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
        // this.pubClient.publish(this.requestChannel, request);
    }
    disconnectSockets(opts, close) {
        var _a;
        if ((_a = opts.flags) === null || _a === void 0 ? void 0 : _a.local) {
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
        // this.pubClient.publish(this.requestChannel, request);
    }
    serverSideEmit(packet) {
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
        // this.pubClient.publish(this.requestChannel, request);
    }
    async serverSideEmitWithAck(packet) {
        // const ack = packet.pop();
        // const numSub = (await this.getNumSub()) - 1; // ignore self
        // debug('waiting for %d responses to "serverSideEmit" request', numSub);
        // if (numSub <= 0) {
        //     return ack(null, []);
        // }
        // const requestId = uid2(6);
        // const request = JSON.stringify({
        //     uid: this.uid,
        //     requestId, // the presence of this attribute defines whether an acknowledgement is needed
        //     type: RequestType.SERVER_SIDE_EMIT,
        //     data: packet,
        // });
        // const timeout = setTimeout(() => {
        // const storedRequest = this.requests.get(requestId);
        //     if (storedRequest) {
        //         ack(
        //         new Error(
        //             `timeout reached: only ${storedRequest.responses.length} responses received out of ${storedRequest.numSub}`
        //         ),
        //         storedRequest.responses
        //         );
        //         this.requests.delete(requestId);
        //     }
        // }, this.requestsTimeout);
        // this.requests.set(requestId, {
        //     type: RequestType.SERVER_SIDE_EMIT,
        //     numSub,
        //     timeout,
        //     resolve: ack,
        //     responses: [],
        // });
        // this.pubClient.publish(this.requestChannel, request);
    }
    async close() {
        debug('close adapter');
        if (this.consumer) {
            await this.consumer.stop();
            await this.consumer.disconnect();
        }
        if (this.producer)
            await this.producer.disconnect();
        process.exit(0);
    }
}
exports.KafkaAdapter = KafkaAdapter;
//# sourceMappingURL=index.js.map