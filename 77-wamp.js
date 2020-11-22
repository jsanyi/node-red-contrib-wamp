module.exports = function (RED) {
    "use strict";
    var events = require("events");
    var autobahn = require("autobahn");
    var settings = RED.settings;

    function WampClientNode(config) {
        RED.nodes.createNode(this, config);

        this.address = config.address;
        this.realm = config.realm;
        this.authmethod = config.authmethod;
        this.authid = config.authid;
        this.secret = config.secret;

        this.wampClient = function () {
            return wampClientPool.get(this.address, this.realm, this.authmethod, this.authid, this.secret);
        };

        this.on = function (a, b) {
            this.wampClient().on(a, b);
        };
        this.close = function (done) {
            wampClientPool.close(this.address, this.realm, done);
        }
    }
    RED.nodes.registerType("wamp-client", WampClientNode);

    function WampClientOutNode(config) {
        RED.nodes.createNode(this, config);
        this.router = config.router;
        this.role = config.role;
        this.topic = config.topic;
        this.clientNode = RED.nodes.getNode(this.router);

        if (this.clientNode) {
            var node = this;
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
                if (msg.hasOwnProperty("payload")) {
                    var args = msg.payload.args
                    var kwargs = msg.payload.kwargs
                    var topic = msg.topic || this.topic
                    var options = msg.options

                    switch (this.role) {
                        case "publisher":
                            node.wampClient.publish(topic, args, kwargs, options);
                            break;
                        case "calleeResponse":
                            msg._d && msg._d.resolve(msg.payload);
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

    function WampClientInNode(config) {
        RED.nodes.createNode(this, config);
        this.role = config.role;
        this.router = config.router;
        this.topic = config.topic;

        this.clientNode = RED.nodes.getNode(this.router);

        if (this.clientNode) {
            var node = this;
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
                    node.wampClient.subscribe(this.topic, function (args, kwargs) {
                        var msg = {
                            topic: this.topic,
                            payload: {
                                args: args,
                                kwargs: kwargs
                            }
                        };
                        node.send(msg);
                    });
                    break;
                case "calleeReceiver":
                    node.wampClient.registerProcedure(this.topic, function (args, kwargs, details) {
                        var d = autobahn.when.defer(); // create a deferred
                        var msg = {
                            procedure: this.topic,
                            payload: {
                                args: args,
                                kwargs: kwargs,
                                details: details
                            },
                            _d: d
                        };
                        node.send(msg);
                        return d.promise;
                    });
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


    function WampClientCallNode(config) {
        RED.nodes.createNode(this, config);
        this.router = config.router;
        this.procedure = config.procedure;

        this.clientNode = RED.nodes.getNode(this.router)

        if (this.clientNode) {
            var node = this;
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
                    var d = node.wampClient.callProcedure(this.procedure, msg.payload.args, msg.payload.kwargs, msg.options);
                    if (d) {
                        d.then(
                            function (resp) {
                                RED.log.debug("call result: " + JSON.stringify(resp));
                                node.send({
                                    payload: resp
                                });
                            },
                            function (err) {
                                RED.log.warn("call response failed: " + err.error);
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

    var wampClientPool = (function () {
        var connections = {};
        return {
            get: function (address, realm, authmethod, authid, secret) {
                var uri = realm + "@" + address;
                if (!connections[uri]) {
                    connections[uri] = (function () {
                        var obj = {
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
                            subscribe: function (topic, handler) {
                                RED.log.debug("add to wamp subscribe request for topic: " + topic);
                                this._subscribeReqMap[topic] = handler;

                                if (this._connected && this.wampSession) {
                                    this._subscribeMap[topic] = this.wampSession.subscribe(topic, handler);
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
                            registerProcedure: function (procedure, handler) {
                                RED.log.debug("add to wamp request for procedure: " + procedure);
                                this._procedureReqMap[procedure] = handler;

                                if (this._connected && this.wampSession) {
                                    this._procedureMap[procedure] = this.wampSession.subscribe(procedure, handler);
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

                        var _disconnect = function () {
                            if (obj.wampConnection) {
                                obj.wampConnection.close();
                            }
                        };

                        var setupWampClient = function () {
                            obj._connecting = true;
                            obj._connected = false;
                            obj._emitter.emit("closed");
                            if (authmethod !== "none") {
                                var options = {
                                    transports: [{
                                        url: address,
                                        type: 'websocket'
                                    }],
                                    realm: realm,
                                    retry_if_unreachable: true,
                                    max_retries: -1,
                                    authmethods: [authmethod],
                                    authid: authid,
                                    onchallenge: function () {
                                        return secret;
                                    }
                                };
                            } else {
                                var options = {
                                    transports: [{
                                        url: address,
                                        type: 'websocket'
                                    }],
                                    realm: realm,
                                    retry_if_unreachable: true,
                                    max_retries: -1,
                                };
                            }
                            obj.wampConnection = new autobahn.Connection(options);

                            obj.wampConnection.onopen = function (session) {
                                RED.log.info("wamp client " + uri + " connected.");
                                obj.wampSession = session;
                                obj._connected = true;
                                obj._emitter.emit("ready");

                                obj._subscribeMap = {};
                                for (var topic in obj._subscribeReqMap) {
                                    obj.wampSession.subscribe(topic, obj._subscribeReqMap[topic]).then(
                                        function (subscription) {
                                            obj._subscribeMap[topic] = subscription;
                                            RED.log.debug("wamp subscribe topic [" + topic + "] success.");
                                        },
                                        function (err) {
                                            RED.log.warn("wamp subscribe topic [" + topic + "] failed: " + err);
                                        }
                                    )
                                }

                                obj._procedureMap = {};
                                for (var procedure in obj._procedureReqMap) {
                                    obj.wampSession.register(procedure, obj._procedureReqMap[procedure]).then(
                                        function (registration) {
                                            obj._procedureMap[procedure] = registration;
                                            RED.log.debug("wamp register procedure [" + procedure + "] success.");
                                        },
                                        function (err) {
                                            RED.log.warn("wamp register procedure [" + procedure + "] failed: " + err.error);
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
                var uri = realm + "@" + address;
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
}