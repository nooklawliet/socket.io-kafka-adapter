"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaAdapter = exports.createAdapter = void 0;
const socket_io_adapter_1 = require("socket.io-adapter");
const msgpack = require("notepack.io");
const uid2 = require("uid2");
const Debug = require("debug");
const debug = new Debug('socket.io-kafka-adapter');
const adapterStore = {};
const KAFKA_ADAPTER_TOPIC = 'kafka_adapter';
const KAFKA_ADAPTER_SOCKET_TOPIC = 'kafka_adapter_socket';
const uid = uid2(6);
var RequestType;
(function (RequestType) {
    RequestType[RequestType["SOCKETS"] = 0] = "SOCKETS";
    RequestType[RequestType["ALL_ROOMS"] = 1] = "ALL_ROOMS";
    RequestType[RequestType["REMOTE_JOIN"] = 2] = "REMOTE_JOIN";
    RequestType[RequestType["REMOTE_LEAVE"] = 3] = "REMOTE_LEAVE";
    RequestType[RequestType["REMOTE_DISCONNECT"] = 4] = "REMOTE_DISCONNECT";
    RequestType[RequestType["REMOTE_FETCH"] = 5] = "REMOTE_FETCH";
    RequestType[RequestType["SERVER_SIDE_EMIT"] = 6] = "SERVER_SIDE_EMIT";
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
        this.consumer = consumer;
        this.consumer.subscribe({
            topics: [KAFKA_ADAPTER_TOPIC, KAFKA_ADAPTER_SOCKET_TOPIC]
        }).then(() => this.onmessage());
        this.producer = producer;
        this.uid = uid2(6);
    }
    async onmessage() {
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                try {
                    debug('consumer recieved message', {
                        topic: topic,
                        partition: partition,
                        key: message.key,
                        value: message.value,
                        headers: message.headers,
                    });
                    if (topic === KAFKA_ADAPTER_TOPIC) {
                        const msg = message.value;
                        const args = msgpack.decode(msg);
                        debug('args:', args);
                        const [uid, packet, opts] = args;
                        if (this.uid === uid) {
                            return debug('ignore same uid');
                        }
                        opts.rooms = new Set(opts.rooms);
                        opts.except = new Set(opts.except);
                        super.broadcast(packet, opts);
                    }
                    else if (topic === KAFKA_ADAPTER_SOCKET_TOPIC) {
                        const msg = message.value;
                        const args = msgpack.decode(msg);
                        debug('args:', args);
                        // const [uid, request] = args;
                        const request = JSON.parse(args[1]);
                        debug('json request:', request);
                        let socket;
                        switch (request.type) {
                            case RequestType.REMOTE_JOIN:
                                debug('RequestType: REMOTE_JOIN');
                                if (request.opts) {
                                    const opts = {
                                        rooms: new Set(request.opts.rooms),
                                        except: new Set(request.opts.except),
                                    };
                                    debug('opts:', opts);
                                    return super.addSockets(opts, request.rooms);
                                }
                                debug('request.sid:', request.sid);
                                socket = this.nsp.sockets.get(request.sid);
                                debug('socket:', socket);
                                if (!socket) {
                                    return;
                                }
                                socket.join(request.room);
                                // response = JSON.stringify({
                                //     requestId: request.requestId,
                                // });
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
                                // response = JSON.stringify({
                                //     requestId: request.requestId,
                                // });
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
                                // response = JSON.stringify({
                                //     requestId: request.requestId,
                                // });
                                // this.publishResponse(request, response);
                                break;
                            case RequestType.SERVER_SIDE_EMIT:
                                debug('RequestType: SERVER_SIDE_EMIT');
                                if (request.uid === this.uid) {
                                    debug('ignore same uid');
                                    return;
                                }
                                const withAck = request.requestId !== undefined;
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
                                    debug("calling acknowledgement with %j", arg);
                                    // this.pubClient.publish(this.responseChannel, JSON.stringify({
                                    //     type: RequestType.SERVER_SIDE_EMIT,
                                    //     requestId: request.requestId,
                                    //     data: arg,
                                    // }));
                                };
                                request.data.push(callback);
                                this.nsp._onServerSideEmit(request.data);
                                break;
                            default:
                                break;
                        }
                    }
                }
                catch (error) {
                    return debug('error:', error);
                }
            },
        });
    }
    broadcast(packet, opts) {
        try {
            packet.nsp = this.nsp.name;
            const rawOpts = {
                rooms: [...opts.rooms],
                except: [...new Set(opts.except)],
                flags: opts.flags,
            };
            debug('broadcast opt:', rawOpts);
            const msg = msgpack.encode([this.uid, packet, rawOpts]);
            let key = this.uid;
            if (opts.rooms && opts.rooms.size === 1) {
                key = opts.rooms.keys().next().value;
            }
            const produceMessage = {
                topic: KAFKA_ADAPTER_TOPIC,
                messages: [{
                        key: key,
                        value: msg
                    }],
            };
            debug('producer send message:', produceMessage);
            this.producer.send(produceMessage);
            super.broadcast(packet, opts);
        }
        catch (error) {
            return debug('error:', error);
        }
    }
    addSockets(opts, rooms) {
        var _a;
        if ((_a = opts.flags) === null || _a === void 0 ? void 0 : _a.local) {
            return super.addSockets(opts, rooms);
        }
        const request = JSON.stringify({
            uid: this.uid,
            type: RequestType.REMOTE_JOIN,
            opts: {
                rooms: [...opts.rooms],
                except: [...opts.except],
            },
            rooms: [...rooms],
        });
        debug('add sockets request:', request);
        // this.pubClient.publish(this.requestChannel, request);
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
                except: [...opts.except],
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
                except: [...opts.except],
            },
            close,
        });
        debug('dicconnect sockets request:', request);
        // this.pubClient.publish(this.requestChannel, request);
    }
    serverSideEmit(packet) {
        // const withAck = typeof packet[packet.length - 1] === "function";
        // if (withAck) {
        //     this.serverSideEmitWithAck(packet).catch(() => {
        //         // ignore errors
        //     });
        //     return;
        // }
        const request = JSON.stringify({
            uid: this.uid,
            type: RequestType.SERVER_SIDE_EMIT,
            data: packet,
        });
        debug('server side emit request:', request);
        // this.pubClient.publish(this.requestChannel, request);
    }
}
exports.KafkaAdapter = KafkaAdapter;
//# sourceMappingURL=index.js.map