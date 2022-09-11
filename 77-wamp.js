module.exports = function (RED) {
    "use strict";
    const events = require("events");
    const autobahn = require("autobahn");

    // #region wamp-client
    function WampClientNode(config) {
        RED.nodes.createNode(this, config);

        this.address = config.address;
        this.realm = config.realm;
        this.authmethod = config.authmethod;
        this.authid = config.authid;
        this.secret = config.secret;
        this.maxretrydelay = config.maxretrydelay;

        this.wampClient = function () {
            return wampClientPool.get(this.address, this.realm, this.authmethod, this.authid, this.secret, this.maxretrydelay);
        };

        this.on = function (a, b) {
            this.wampClient().on(a, b);
        };
        this.close = function (done) {
            wampClientPool.close(this.address, this.realm, done);
        }
    }
    RED.nodes.registerType("wamp-client", WampClientNode);
    // #endregion

    // #region wamp-out
    function WampClientOutNode(config) {
        RED.nodes.createNode(this, config);
        this.router = config.router;
        this.role = config.role;
        this.topic = config.topic;
        this.topicType = config.topicType;

        this.clientNode = RED.nodes.getNode(this.router);

        if (this.clientNode) {
            const node = this;
            node.wampClient = this.clientNode.wampClient();

            this.clientNode.on("ready", function () {
                node.status({
                    fill: "green",
                    shape: "dot",
                    text: "node-red:common.status.connected"
                });
            });
            this.clientNode.on("closed", function () {
                node.status({
                    fill: "red",
                    shape: "ring",
                    text: "node-red:common.status.not-connected"
                });
            });

            node.on("input", async function (msg) {
                if (msg.hasOwnProperty("payload")) {
                    var defTopic = autobahn.when.defer();
                    defTopic.resolver = (error, value) => {
                        if (error) {
                            defTopic.reject(error);
                        } else {
                            defTopic.resolve(value);
                        }
                    };

                    var syncTopic = RED.util.evaluateNodeProperty(node.topic, node.topicType, node, msg, defTopic.resolver);
                    var topic = syncTopic || await defTopic.promise;

                    var payload = msg.payload;
                    switch (this.role) {
                        case "publisher":
                            RED.log.info("wamp client publish: topic=" + topic + ", payload=" + JSON.stringify(payload));
                            var args = [], kwargs = {};
                            if (typeof payload === 'object' && !Array.isArray(payload) && payload !== null) {
                                kwargs = payload;
                            } else if (Array.isArray(payload)) {
                                args = payload;
                            } else {
                                args = [payload];
                            }
                            node.wampClient.publish(topic, args, kwargs);
                            break;
                        case "calleeResponse":
                            msg._d && msg._d.resolve(payload);
                            break;
                        default:
                            RED.log.error("the role [" + this.role + "] is not recognized.");
                            break;
                    }
                }
            });
        } else {
            RED.log.error("wamp client config is missing!");
        }

        this.on("close", function (done) {
            if (this.clientNode) {
                this.clientNode.close(done);
            } else {
                done();
            }
        });
    }
    RED.nodes.registerType("wamp out", WampClientOutNode);
    // #endregion

    // #region wamp-in
    function WampClientInNode(config) {
        RED.nodes.createNode(this, config);
        this.role = config.role;
        this.router = config.router;
        this.topic = config.topic;
        this.match = config.match || "exact";

        this.clientNode = RED.nodes.getNode(this.router);

        if (this.clientNode) {
            const node = this;
            node.wampClient = this.clientNode.wampClient();

            this.clientNode.on("ready", function () {
                node.status({
                    fill: "green",
                    shape: "dot",
                    text: "node-red:common.status.connected"
                });
            });
            this.clientNode.on("closed", function () {
                node.status({
                    fill: "red",
                    shape: "ring",
                    text: "node-red:common.status.not-connected"
                });
            });

            switch (this.role) {
                case "subscriber":
                    node.wampClient.subscribe(
                        this.topic,
                        function (args, kwargs, details) {
                            var msg = {
                                topic: details.topic,
                                payload: {
                                    args: args,
                                    kwargs: kwargs
                                }
                            };
                            node.send(msg);
                        },
                        { match: this.match }, node.id
                    );
                    break;
                case "calleeReceiver":
                    node.wampClient.registerProcedure(this.topic, (args, kwargs, details) => {
                        const d = autobahn.when.defer(); // create a deferred
                        const msg = {
                            procedure: this.topic,
                            payload: {
                                args: args,
                                kwargs: kwargs
                            },
                            details: details,
                            _d: d
                        };
                        node.send(msg);
                        return d.promise;
                    }, { "match": this.match }, node.id);
                    break;
                default:
                    RED.log.error("the role [" + this.role + "] is not recognized.");
                    break;
            }
        } else {
            RED.log.error("wamp client config is missing!");
        }

        this.on("close", function (done) {
            if (this.clientNode) {
                this.clientNode.close(done);
            } else {
                done();
            }
        });
    }
    RED.nodes.registerType("wamp in", WampClientInNode);
    // #endregion

    // #region wamp-call
    function WampClientCallNode(config) {
        RED.nodes.createNode(this, config);
        this.router = config.router;
        this.procedure = config.procedure;

        this.clientNode = RED.nodes.getNode(this.router)

        if (this.clientNode) {
            const node = this;
            node.wampClient = this.clientNode.wampClient();

            this.clientNode.on("ready", function () {
                node.status({
                    fill: "green",
                    shape: "dot",
                    text: "node-red:common.status.connected"
                });
            });
            this.clientNode.on("closed", function () {
                node.status({
                    fill: "red",
                    shape: "ring",
                    text: "node-red:common.status.not-connected"
                });
            });

            node.on("input", function (msg) {
                if (this.procedure) {
                    var args = msg.payload.args === undefined ? [] : msg.payload.args;
                    var kwargs = msg.payload.kwargs === undefined ? {} : msg.payload.kwargs;
                    const d = node.wampClient.callProcedure(this.procedure, args, kwargs, msg.options);
                    if (d) {
                        d.then(
                            function (resp) {
                                RED.log.debug("call result: " + JSON.stringify(resp));
                                node.send({
                                    payload: resp
                                });
                            },
                            function (err) {
                                RED.log.warn(JSON.stringify(err));
                            }
                        )
                    }
                }
            });
        } else {
            RED.log.error("wamp client config is missing!");
        }

        this.on("close", function (done) {
            if (this.clientNode) {
                this.clientNode.close(done);
            } else {
                done();
            }
        });
    }
    RED.nodes.registerType("wamp call", WampClientCallNode);
    // #endregion

    // #region wamp-client-pool
    const wampClientPool = (function () {
        const connections = {};
        return {
            get: function (address, realm, authmethod, authid, secret, maxretrydelay) {
                const uri = realm + "@" + address;
                if (!connections[uri]) {
                    connections[uri] = (function () {
                        const obj = {
                            _emitter: new events.EventEmitter(),
                            wampConnection: null,
                            wampSession: null,
                            _connecting: false,
                            _connected: false,
                            _closing: false,
                            _subscribeReqMap: {},
                            _subscribeMap: {},
                            _procedureReqMap: {},
                            _procedureMap: {},
                            on: function (a, b) {
                                this._emitter.on(a, b);
                            },
                            close: function () {
                                _disconnect();
                            },
                            publish: function (topic, args, kwargs, options) {
                                if (this.wampSession) {
                                    this.wampSession.publish(topic, args, kwargs, options)
                                } else {
                                    RED.log.warn("publish failed, wamp is not connected.");
                                }
                            },
                            subscribe: function (topic, handler, options, id) {
                                RED.log.debug("add to wamp subscribe request for topic: " + topic);
                                this._subscribeReqMap[id] = { topic, handler, options };

                                if (this._connected && this.wampSession) {
                                    this.wampSession.subscribe(topic, handler, options).then(
                                        function (subscription) {
                                            this._subscribeMap[id] = subscription; 
                                            RED.log.info("wamp subscribe node [" + id + "], topic [" + topic + " (" + options.match + ")] success.");
                                        },
                                        function (err) {
                                            RED.log.warn("wamp subscribe node [" + id + "], topic [" + topic + " (" + options.match + ")] failed: " + err);
                                        }
                                    )
                                }
                            },
                            // unsubscribe: function (topic) {
                            // if (this._subscribeReqMap[topic]) {
                            //     delete this._subscribeReqMap[topic];
                            // }
                            //
                            // if (this._subscribeMap[topic]) {
                            //     if (this.wampSession) {
                            //         this.wampSession.unsubscribe(this._subscribeMap[topic]);
                            //         RED.log.info("unsubscribed wamp topic: ", topic);
                            //     }
                            //     delete this._subscribeMap[topic];
                            // }
                            // },
                            registerProcedure: function (procedure, handler, options, id) {
                                this._procedureReqMap[id] = { procedure, handler, options };

                                if (this._connected && this.wampSession) {
                                    this._procedureMap[id] = this.wampSession.subscribe(procedure, handler, options);
                                }
                            },
                            callProcedure: function (procedure, args, kwargs, options) {
                                if (this.wampSession) {
                                    return this.wampSession.call(procedure, args, kwargs, options);
                                } else {
                                    RED.log.warn("call failed, wamp is not connected.");
                                }
                            }
                        };

                        const _disconnect = function () {
                            if (obj.wampConnection) {
                                obj.wampConnection.close();
                            }
                        };

                        const setupWampClient = function () {
                            obj._connecting = true;
                            obj._connected = false;
                            obj._emitter.emit("closed");
                            let options = {
                                transports: [{
                                    url: address,
                                    type: 'websocket'
                                }],
                                realm: realm,
                                retry_if_unreachable: true,
                                max_retries: -1,
                                max_retry_delay: maxretrydelay,
                            };
                            if (authmethod !== "none") {
                                options.authmethods = [authmethod];
                                options.authid = authid;
                                options.onchallenge = function () {
                                    return secret;
                                };
                            }
                            obj.wampConnection = new autobahn.Connection(options);

                            obj.wampConnection.onopen = function (session) {
                                RED.log.info("wamp client " + uri + " connected.");
                                obj.wampSession = session;
                                obj._connected = true;
                                obj._emitter.emit("ready");

                                obj._subscribeMap = {};
                                for (const id in obj._subscribeReqMap) {
                                    var subscribeReq = obj._subscribeReqMap[id];
                                    obj.wampSession.subscribe(subscribeReq.topic, subscribeReq.handler, subscribeReq.options).then(
                                        function (subscription) {
                                            obj._subscribeMap[id] = subscription;
                                            RED.log.info("wamp subscribe node [" + id + "], topic [" + subscribeReq.topic + " (" + subscribeReq.options.match + ")] success.");
                                        },
                                        function (err) {
                                            RED.log.warn("wamp subscribe node [" + id + "], topic [" + subscribeReq.topic + " (" + subscribeReq.options.match + ")] failed: " + err);
                                        }
                                    )
                                }

                                obj._procedureMap = {};
                                for (const id in obj._procedureReqMap) {
                                    var procedureReq = obj._procedureReqMap[id];
                                    obj.wampSession.register(procedureReq.procedure, procedureReq.handler, procedureReq.options).then(
                                        function (registration) {
                                            obj._procedureMap[id] = registration;
                                            RED.log.info("wamp register node [" + id + "], procedure [" + procedureReq.procedure + " (" + procedureReq.options.match + ")] success.");
                                        },
                                        function (err) {
                                            RED.log.warn("wamp register node [" + id + "], procedure [" + procedureReq.procedure + " (" + procedureReq.options.match + ")] failed: " + err.error);
                                        }
                                    )
                                }

                                obj._connecting = false;
                            };

                            obj.wampConnection.onclose = function (reason, details) {
                                obj._connecting = false;
                                obj._connected = false;
                                if (!obj._closing) {
                                    // RED.log.error("unexpected close", {uri:uri});
                                    obj._emitter.emit("closed");
                                }
                                obj._subscribeMap = {};
                                RED.log.info("wamp client closed: " + reason);
                            };

                            obj.wampConnection.open();
                        };

                        setupWampClient();
                        return obj;
                    }());
                }
                return connections[uri];
            },
            close: function (address, realm, done) {
                const uri = realm + "@" + address;
                if (connections[uri]) {
                    RED.log.info("ready to close wamp client [" + uri + "]");
                    connections[uri]._closing = true;
                    connections[uri].close();
                    (typeof (done) == 'function') && done();
                    delete connections[uri];
                } else {
                    (typeof (done) == 'function') && done();
                }
            }
        }
    }());
    // #endregion
}
