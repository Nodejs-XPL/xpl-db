/*jslint node: true, vars: true, nomen: true, esversion: 6 */
'use strict';

const Xpl = require("xpl-api");
const commander = require('commander');
const os = require('os');
const debug = require('debug')('xpl-db');
const Mysql = require('./lib/mysql');
const Mongo = require('./lib/mongo');
const Server = require('./lib/server');
const Async = require('async');
const ip = require('ip');

const XplDBClient = require('xpl-dbclient');
const Memcache = XplDBClient.Memcache;
const Query = XplDBClient.Query;

commander.version(require("./package.json").version);
commander.option("-a, --deviceAliases <aliases>", "Devices aliases");
commander.option("--httpPort <port>", "REST server port", parseInt);
commander.option("--configPath <path>", "Static config files of http server");
commander.option("--xplCommand", "Enable xpl commands by Http command");
commander.option("--memcached", "Store xpl values in memcache");
commander.option("--db", "Store xpl values in a DB");
commander.option("--storeType <type>", "DB type");
commander.option("--socketIO", "Enable Web Socket");

Mysql.fillCommander(commander);
Mongo.fillCommander(commander);
Xpl.fillCommander(commander);
Memcache.fillCommander(commander);
Query.fillCommander(commander);

commander.command("create").action(() => {

	let Store = getStore(commander);

	var store = new Store(commander);

	store.create(function (error) {
		if (error) {
			console.error(error);
			return;
		}
	});
});


commander.command("rest").action(() => {

	const Store = getStore(commander);

	const deviceAliases = Xpl.loadDeviceAliases(commander.deviceAliases);
	var initCbs = [];
	var store;
	var memcache;

	if (commander.db) {
		initCbs.push((callback) => {
			store = new Store(commander, deviceAliases);

			store.connect(callback);
		});
	}

	if (commander.memcached) {
		initCbs.push((callback) => {
			memcache = new Memcache(commander, deviceAliases);

			memcache.initialize((error) => {
				if (error) {
					return callback(error);
				}

				callback();
			});
		});
	}

	Async.parallel(initCbs, (error) => {
		if (error) {
			console.error(error);
			return;
		}

		var f = (xpl) => {
			const server = new Server(commander, store, xpl, memcache, deviceAliases);

			server.listen((error, server) => {
				if (error) {
					console.error(error);
					return;
				}

				if (!memcache || !store) {
					return;
				}

				process.on('exit', () => {
					debug("xpl-db", "Clearing rest server URL ...");

					memcache.saveRestServerURL('', (error) => {
						debug("xpl-db", "Reset rest server URL into memcache !");
					});
				});

				var url = "http://" + ip.address() + ":" + server.address().port;
				debug("xpl-db", "Set rest server url to", url);

				var intervalId = setInterval(() => {
					memcache.saveRestServerURL(url, (error) => {
						if (error) {
							console.error(error);

							clearInterval(intervalId);
						}
					});
				}, 1000 * 55);
			});
		};

		var xpl = new Xpl(commander);

		xpl.on("error", (error) => {
			console.error("XPL error", error);
		});

		xpl.bind((error) => {
			if (error) {
				console.error("Can not open xpl bridge ", error);
				process.exit(2);
				return;
			}

			f(xpl);
		});
	});
});

commander.command("store").action(() => {

	let Store = getStore(commander);

	var deviceAliases = Xpl.loadDeviceAliases(commander.deviceAliases);

	debug("store", "Store starting ... deviceAliases=", deviceAliases);

	var store;
	var memcache;
	var initCbs = [];

	if (commander.db) {
		initCbs.push((callback) => {
			store = new Store(commander, deviceAliases);

			store.connect((error) => {
				if (error) {
					debug("store", "Store error=", error);
					return callback(error);
				}

				debug("store", "Store connected");

				callback();
			});
		});
	}
	if (commander.memcached) {
		initCbs.push((callback) => {
			memcache = new Memcache(commander, deviceAliases);

			memcache.initialize((error) => {
				if (error) {
					debug("store", "Memcached error=", error);
					return callback(error);
				}

				debug("store", "Memcached connected");

				callback();
			});
		});
	}

	Async.parallel(initCbs, (error) => {
		if (error) {
			debug("store", "Initialization error=", error);
			console.error(error);
			return;
		}
		debug("store", "Initialization OK");

		try {
			if (!commander.xplSource) {
				var hostName = os.hostname();
				if (hostName.indexOf('.') > 0) {
					hostName = hostName.substring(0, hostName.indexOf('.'));
				}

				commander.xplSource = "db." + hostName;
			}

			var xpl = new Xpl(commander);

			xpl.on("error", (error) => {
				console.error("XPL error", error);
			});

			xpl.bind((error) => {
				if (error) {
					console.error("Can not open xpl bridge ", error);
					process.exit(2);
					return;
				}

				debug("store", "Xpl bind succeed ");

				xpl.on("message", (message) => {
					if (store) {
						store.save(message, (error) => {
							if (error) {
								console.error('error connecting: ', error, error.stack);
								return;
							}
						});
					}
					if (memcache) {
						memcache.saveMessage(message);
					}
				});
			});
		} catch (x) {
			console.error(x);
		}
	});
});

commander.command("*").action(() => {
	console.error("Unknown command", arguments);
	process.exit(1);
});

commander.parse(process.argv);

function getStore(commander) {
	switch (commander.storeType || "mysql") {
		case "mysql":
			return Mysql;

		case "mongo":
			return Mongo;
	}

	console.error("Invalid store type '" + commander.storeType + "'");
	process.exit(2);
}
