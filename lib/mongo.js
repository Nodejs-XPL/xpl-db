/*jslint node: true, vars: true, nomen: true, esversion: 6 */
'use strict';

const debug = require('debug')('xpl-db:mongo');
const MongoClient = require('mongodb').MongoClient;
const async = require('async');
const Db = require('./db');
const util = require('util');

class Mongo extends Db {
	constructor(configuration, deviceAliases) {
		super(configuration, deviceAliases);
	}

	static fillCommander(commander) {
		commander.option("--mongoURL <url>", "Mongo url (mongodb://localhost/xpl)");

	}

	create(callback) {
		callback();
	}

	connect(callback) {
		let url = this.configuration.mongoURL;

		MongoClient.connect(url, (error, db) => {
			if (error) {
				console.error("Can not connect mongodb server url=", url, "error=", error);
				return callback(error);
			}

			debug("Mongodb connected");

			var messages = db.collection('messages');

			messages.createIndex({date: 1}, (error, result)=> {
				debug("connect", "CreateIndex 1: error=", error, "result=", result);
				if (error) {
					return callback(error);
				}

				messages.createIndex({device: 1, date: 1}, {sparse: true}, (error, result)=> {
					debug("connect", "CreateIndex 2: error=", error, "result=", result);
					if (error) {
						return callback(error);
					}

					this._messagesCollection = messages;

					callback();
				});
			});
		});
	}

	save(message, callback) {
		let timestamp = message.timestamp || Date.now();
		if (typeof(timestamp) === "string" || typeof(timestamp) === "number") {
			timestamp = new Date(timestamp);
			message.timestamp = timestamp;
		}

		let bodyDate = message.body.date;
		if (typeof(bodyDate) === "string" || typeof(bodyDate) === "number") {
			bodyDate = new Date(bodyDate);
			message.body.date = bodyDate;
		}
		if (util.isDate(bodyDate)) {
			message.date = bodyDate;

		} else {
			message.date = timestamp;
		}

		let device = message.body.device || message.body.address;
		if (device) {
			if (this.deviceAliases && this.deviceAliases[device]) {
				device = this.deviceAliases[device];
			}

			if (message.body.type) {
				device += "/" + message.body.type;

				if (this.deviceAliases && this.deviceAliases[device]) {
					device = this.deviceAliases[device];
				}
			}

			message.deviceKey = device;
		}

		let current = message.body.current;
		if (current) {
			if (/(enable|enabled|on|true)/i.exec(current)) {
				current = true;

			} else if (/(disable|disabled|off|false)/i.exec(current)) {
				current = false;

			} else if (/^[+-]?\d+(\.\d+)?$/.exec(current)) {
				current = parseFloat(current);
			}

			message.body.current = current;
		}

		debug("save", "inserting message=", message);

		this._messagesCollection.insertOne(message, (error, result) => {
			debug("save", "message inserted error=", error, "result=", result);

			if (error) {
				return callback(error);
			}

			callback(null, result);
		});
	}

	_createCursor(deviceKey, options, order, limit, projectCurrent) {
		let query = {deviceKey: deviceKey};

		if (options.dateMin) {
			query.date = query.date || {};
			query.date.$gte = options.dateMin;
		}

		if (options.dateMax) {
			query.date = query.date || {};
			query.date.$lt = options.dateMax;
		}

		let cursor = this._messagesCollection.find(query);

		cursor = cursor.sort({'date': order});

		if (typeof(options.limit) === "number" || limit > 0) {
			cursor = cursor.limit(options.limit || limit);
		}

		if (projectCurrent) {
			cursor = cursor.project({"body.current": 1, "body.units": 1, "date": 1});
		}

		return cursor;
	}

	getLastMessage(deviceKey, options, callback) {

		let cursor = this._createCursor(deviceKey, options, -1, 1);

		debug("getLastMessage", "deviceKey=", deviceKey, "cursor=", cursor.cmd);
		cursor.next((error, result) => {
			if (error) {
				debug("getLastMessage", "deviceKey=", deviceKey, "error=", error);
				callback(error);
				return;
			}

			debug("getLastMessage", "deviceKey=", deviceKey, "result=", result);

			callback(null, result);
		});
	}

	getLast(deviceKey, options, callback) {

		let cursor = this._createCursor(deviceKey, options, -1, 1, true);

		debug("getLast", "deviceKey=", deviceKey, "cursor=", cursor.cmd);
		cursor.next((error, result) => {
			if (error) {
				return callback(error);
			}

			if (result) {
				result = this._normalize(result);
			}

			debug("getLast", "deviceKey=", deviceKey, "normalized result=", result);
			callback(null, result);
		});
	}

	getHistory(deviceKey, options, callback) {

		let cursor = this._createCursor(deviceKey, options, 1, -1, true);

		debug("getHistory", "deviceKey=", deviceKey, "cursor=", cursor.cmd);
		cursor.toArray((error, result) => {
			if (error) {
				debug("getHistory", "deviceKey=", deviceKey, "error=", error);
				callback(error);
				return;
			}

			debug("getHistory", "deviceKey=", deviceKey, "result=", result);

			result = result.map((result) => this._normalize(result));
			callback(null, result);
		});
	}

	_normalize(result) {
		let body = result.body;
		let ret = {
			current: body.current,
			units: body.units,
			date: body.date || result.date,
		};

		return ret;
	}
}

module.exports = Mongo;

