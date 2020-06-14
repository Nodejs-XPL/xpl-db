const express = require('express');
const http = require('http');
const Debug = require('debug');
const async = require('async');
const bodyParser = require('body-parser');
const serve_static = require('serve-static');
const compression = require('compression');
const UUID = require('uuid');
const EventListener = require('events');
const SocketIO = require('socket.io');

const debug = Debug('xpl-db:server');

const NO_CACHE_CONTROL = "no-cache, private, no-store, must-revalidate, max-stale=0, max-age=1,post-check=0, pre-check=0";

class Server extends EventListener {
	constructor(configuration, store, xpl, memcached, deviceAliases) {
		super();

		this.configuration = configuration || {};
		this.store = store;
		this.xpl = xpl;
		this._memcached = memcached;
		this.deviceAliases = deviceAliases;

		if (this.configuration.express) {
			this.app = this.configuration.express;

		} else {
			const app = express();
			this.app = app;
			app.enable('etag');
			app.disable('x-powered-by');
			app.use(compression());
			app.use(bodyParser.json());
			app.use(bodyParser.urlencoded({
				extended: true
			}));
		}

		const uuid = Date.now();
		let messageKey = 0;
		this.xpl.on("message", (message) => {
			const m = {
				...message,
				key: uuid + ':' + (messageKey++)
			};

			this.emit('xpl-message', m);
		});
	}

	listen(callback) {
		const app = this.app;
// app.use(noCache);

		app.get(/^\/last\/(.*)$/, (request, response) => this._getLast(request, response));
		app.get(/^\/history\/(.*)$/, (request, response) => this._getHistory(request, response));
		app.get(/^\/minMaxAvgSum\/(.*)$/, (request, response) => this._getMinMaxAvgSum(request, response));
		app.get(/^\/cumulated\/(.*)$/, (request, response) => this._getCumulated(request, response));
		app.get(/^\/lastMessage\/(.*)$/, (request, response) => this._getLastMessage(request, response));

		if (this.xpl) {
			app.get(/^\/xplCommand\/(.*)$/, (request, response) => this._xplCommand(request, response));
//            app.get(/^\/tunnelCommands(.*)$/, (request, response) => this._proxyCommands(request, response));
		}

		app.post('/last', (request, response) => this._postLast(request, response));
		app.post('/history', (request, response) => this._postHistory(request, response));
		app.post('/minMaxAvgSum', (request, response) => this._postMinMaxAvgSum(request, response));
		app.post('/cumulated', (request, response) => this._postCumulated(request, response));

		if (this.configuration.configPath) {

			var oneYear = 1000 * 60 * 60 * 24 * 365;

			app.use(express.static(__dirname + '/public', {}));

			app.use("/config", serve_static(this.configuration.configPath, {
				index: false,
				maxAge: oneYear
			}));
		}

		const httpServer = http.createServer(app);
		if (this.configuration.socketIO) {
			const io = SocketIO(httpServer, {
				path: '/ws'
			});

			console.log('Enable socketIO');

			io.on('connection', (socket) => {
				const clientUUID = UUID();
				socket.clientUUID = clientUUID;
				console.log('WS user connected uuid=', clientUUID, 'socket=', socket);

				function xplMessage(message) {
					const filter = socket.xplFilter;

					const body = message.body;
					if (!body) {
						console.log('Ignore empty body message for client', clientUUID);
						return;
					}

					let device = body.device || body.address;
					if (!device) {
						return;
					}

					if (this.deviceAliases && this.deviceAliases[device]) {
						device = this.deviceAliases[device];
						if (!device) {
							return;
						}
					}

					if (filter) {
						if (filter.type === 'devices-only' && filter.devices) {
							let d = device;
							if (message.body.type) {
								d += '/' + message.body.type;
							}

							if (!filter.devices[d]) {
								console.log('Ignore device=', d, 'for client', clientUUID);
								return;
							}
						}
					}

					console.log('Send message=', message, 'to client', clientUUID);

					const xplMessage = {
						...message,
						from: undefined,
						header: {
							...message.header,
							hop: undefined,
							target: undefined,
						},
						body: {
							...message.body,
							device,
							address: undefined,
						}
					};

					socket.emit('xpl-message', xplMessage);
				}

				socket.on('disconnect', () => {
					console.log('WS user disconnected uuid=', clientUUID);

					this.removeListener('xpl-message', xplMessage);
				});

				this.on('xpl-message', xplMessage);

				socket.on('xpl-filter', (message) => {
					console.log('set filter=', message);

					socket.xplFilter = message;
				});


				socket.on('xpl-command', (message) => {
					console.log('command=', message);
					const requestId = message.requestId || UUID();

					let command = message.c || message.cmd || message.command;
					let device = message.device;
					const target = message.target || "*";
					const bodyName = message.bodyName || "delabarre.command";
					const current = message.v || message.current;

					if (!device && !command && message.key) {
						const reg = /(.*)\/([^/]+)$/.exec(message.key);
						if (reg) {
							device = reg[1];
							command = reg[2];
						}
					}

					debug("xplCommand by socket",
						"requestId=", requestId,
						"command=", command,
						"device=", device,
						"target=", target,
						"bodyName=", bodyName,
						"current=", current);

					if (!device || !command) {
						socket.emit('xpl-error', {requestId, code: 400, message: "Device or command not found"});
						return;
					}

					const source = "ws." + socket.conn.remoteAddress;

					const params = {
						command,
						device,
					};
					if (current !== undefined) {
						params.current = current;
					}

					this.xpl.sendXplCmnd(params, bodyName, target, source, (error) => {

						if (error) {
							console.error("Command is not sent", error);
							socket.emit('xpl-error', {requestId, code: 500, message: "Internal error"});
							return;
						}

						socket.emit('xpl-result', {requestId, code: 200, message: "Command sent"});
						console.log("Command sent !");
					});
				});
			});
		}

		app.use((req, res, next) => {
			res.status(404).send('Sorry cant find that!');
		});

		httpServer.listen(this.configuration.httpPort || 8480, (error) => {
			if (error) {
				console.error("Server can not listen", error);
				return;
			}
			debug("listen", "Server is listening ", httpServer.address());

			callback(null, httpServer);
		});
	}

	/*
					_proxyCommands(request, response) {
									const hs = {
													'Content-Type': 'application/json',
													'Transfer-Encoding': 'chunked',
													'Cache-Control': NO_CACHE_CONTROL
									};
									response.writeHead(200, hs);

									const cb = (message) => {
													const json = JSON.stringify(message);
													debug("_proxyCommands", "Send json", json);
													response.write(json);
									};

									this.on("xpl-message", cb);

									response.on('error', (error) => {
													console.error("Catch error=", error);

													this.removeListener("xpl-message", cb);
									});

									response.on('close', () => {
													console.error("Catch close, remove listener !");

													this.removeListener("xpl-message", cb);
									});
					}
	*/
	_xplCommand(request, response) {
		var key = request.params[0];

		var query = request.query;
		var command = query.c || query.cmd || query.command;
		var device = query.device;
		var target = query.target || "*";
		var bodyName = query.bodyName || "delabarre.command";
		var current = query.v || query.current;

		if (!device && !command) {
			var reg = /(.*)\/([^/]+)$/.exec(key);
			if (reg) {
				device = reg[1];
				command = reg[2];
			}
		}

		debug("xplCommand",
			"key=", key,
			"command=", command,
			"device=", device,
			"target=", target,
			"bodyName=", bodyName,
			"current=", current);

		if (!device || !command) {
			response.status(404).send("Device or command not found");
			return;
		}

		var source = "http." + request.connection.remoteAddress;

		var params = {
			command: command,
			device: device
		};
		if (current !== undefined) {
			params.current = current;
		}

		this.xpl.sendXplCmnd(params, bodyName, target, source, (error) => {

			if (error) {
				console.error("Command is not sent", error);
				response.status(500).send("Internal error");
				return;
			}

			console.log("Command sent !");
			response.status(200).send("Command sent");
		});
	}

	/**
	 *
	 * @param {http.IncomingMessage} request
	 * @param {http.ServerResponse} response
	 * @private
	 */
	_getLastMessage(request, response) {
		this._get("getLastMessage", request, response, this.store.getLastMessage);
	}

	/**
	 *
	 * @param name
	 * @param request
	 * @param response
	 * @param func
	 * @private
	 */
	_set(name, request, response, func) {
		var keys = Object.keys(request.body);
		debug("_set", name, "set keys=", keys);

		var options = formatOptions(request);

		var results = {};
		var lastDate = null;

		async.eachLimit(keys, 8, (key, callback) => {
			func.call(this.store, key, options, (error, value) => {
				debug("_set", name, " set key=", key, "value=", value, "error=", error);
				if (error) {
					console.error("Can not process name=", name, "key=", key, "error=", error);
					return callback();
				}

				results[key] = value;

				if (value.date) {
					if (!lastDate || lastDate < value.date) {
						lastDate = value.date;
					}
				}

				callback();
			});

		}, (error) => {
			if (error) {
				// send 500
				if (error.code === 'NOT_FOUND') {
					response.status(404).send("Key '" + error.key + "' not found");
					return;
				}

				response.status(500).send(String(error));
				return;
			}

			if (lastDate) {
				if (!response.headersSent) {
					var since = request.headers['if-modified-since'];
					// console.log("Since=",since,"/",lastDate);

					if (since && (new Date(since)).getTime() === lastDate.getTime()) {
						response.status(304).send("Not modified");
						return;
					}
				}

				response.setHeader("Last-Modified", lastDate.toUTCString());
			}

			response.json(results);
		});
	}

	/**
	 * @param {string} name
	 * @param {http.IncomingMessage} request
	 * @param {http.ServerResponse} response
	 * @param {Function} func
	 * @param {boolean} [noMemcached]
	 * @private
	 */
	_get(name, request, response, func, noMemcached) {
		let ignoreMemcache = false;
		if (noMemcached !== true) {
			var h = request.headers['X-Memcached'];

			ignoreMemcache = (h && h.toLowerCase() !== 'true');
		}

		const key = request.params[0];
		debug("_get", "type=", name, "key=", key, "noMemcached=", noMemcached, "ignoreMemcache=", ignoreMemcache);

		if (!noMemcached && name == "getLast" && !ignoreMemcache) {
			if (this._memcached) {
				debug("_get", "Seach in memcached");

				this._memcached.getCurrent(key, (error, value) => {
					debug("_get", "Memcache returns value=", value, "error=", error);
					if (!error && value) {

						response.setHeader('X-Memcached', 'true');
						response.json(value);
						return;
					}

					this._get(name, request, response, func, true);
				});
				return;
			}
		}

		const options = formatOptions(request);

		func.call(this.store, key, options, (error, values) => {
			debug("_get", "type=", name, "key=", key, "values=", !!values);
			if (error) {
				// send 500
				if (error.code === 'NOT_FOUND') {
					response.status(404).send('Key not found');
					return;
				}

				response.status(500).send(String(error));
				return;
			}

			response.json(values);

			if (this._memcached && name === "getLast") {
				this._memcached.saveCurrent(key, values, (error) => {
					if (error) {
						console.error(error);
					}
				});
			}
		});
	}

	/**
	 *
	 * @param {http.IncomingMessage} request
	 * @param {http.ServerResponse} response
	 * @private
	 */
	_getLast(request, response) {
		this._get("getLast", request, response, this.store.getLast);
	}

	/**
	 *
	 * @param {http.IncomingMessage} request
	 * @param {http.ServerResponse} response
	 * @private
	 */
	_getHistory(request, response) {
		this._get("getHistory", request, response, this.store.getHistory);
	}

	/**
	 *
	 * @param {http.IncomingMessage} request
	 * @param {http.ServerResponse} response
	 * @private
	 */
	_getMinMaxAvgSum(request, response) {
		this._get("getMinMaxAvgSum", request, response, this.store.getMinMaxAvgSum);
	}

	/**
	 *
	 * @param {http.IncomingMessage} request
	 * @param {http.ServerResponse} response
	 * @private
	 */
	_getCumulated(request, response) {
		this._get("getCumulated", request, response, this.store.getCumulated);
	}

	_postLast(request, response) {
		this._set("getLastSet", request, response, this.store.getLast);
	}

	/**
	 *
	 * @param {} request
	 * @param {ServerResponse} response
	 * @private
	 */
	_postHistory(request, response) {
		this._set("getHistorySet", request, response, this.store.getHistory);
	}

	/**
	 *
	 * @param {http.IncomingMessage} request
	 * @param {http.ServerResponse} response
	 * @private
	 */
	_postMinMaxAvgSum(request, response) {
		this._set("getMinMaxAvgSumSet", request, response, this.store.getMinMaxAvgSum);
	}

	/**
	 *
	 * @param {http.IncomingMessage} request
	 * @param {http.ServerResponse} response
	 * @private
	 */
	_postCumulated(request, response) {
		this._set("getCumulatedSet", request, response, this.store.getCumulated);
	}
}


function formatOptions(request) {
	var ret = {};

	var query = request.query;
	if (query.limit) {
		ret.limit = parseInt(query.limit, 10);
	}

	if (query.minDate) {
		var q = /^([0-9]{4}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})/.exec(query.minDate);
		if (q) {
			ret.dateMin = new Date(q[1]);
		}

		ret.dateMin = new Date(query.minDate);
	}

	if (query.maxDate) {
		var q2 = /^([0-9]{4}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})/.exec(query.maxDate);
		if (q2) {
			ret.dateMax = new Date(q2[1]);
		}
		ret.dateMax = new Date(query.maxDate);
	}

	if (query.averageMs) {
		ret.averageMs = parseInt(query.averageMs, 10);
	}

	if (query.step === 'day') {
		ret.stepMs = query.step;

	} else if (query.step) {
		ret.stepMs = parseInt(query.step, 10) * 1000;
	}

	if (query.order) {
		switch (query.order) {
			case 'a':
			case 'asc':
			case 'ascending':
			case '+1':
			case '1':
				ret.order = 1;
				break;
			case 'd':
			case 'desc':
			case 'descending':
			case '-1':
				ret.order = -1;
				break;
		}
	}

	if (query.projection) {
		const projection = {};
		ret.projection = projection;

		query.projection.split(',').forEach((token) => (projection[token] = true));
	}

	return ret;
}

function nocache(req, res, next) {
	res.header('Cache-Control', 'private, no-cache, no-store, must-revalidate');
	res.header('Expires', '-1');
	res.header('Pragma', 'no-cache');
	next();
}

module.exports = Server;
