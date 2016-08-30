/*jslint node: true, vars: true, nomen: true, esversion: 6 */
'use strict';

const debug = require('debug')('xpl-db:mongo');
const mongo = require('mongo');
const async = require('async');
const Db = require('./db');

class Mongo extends Db {
	constructor(configuration, deviceAliases) {
		super(configuration, deviceAliases);
	}

	static fillCommander(commander) {
		commander.option("-h, --mysqlHost <host>", "Mysql host name");

	}

	create(callback) {
	}

	connect(callback) {
	}

	save(message, callback) {
	}

	getLast(deviceKey, options, callback) {
	}

	getHistory(key, options, callback) {
	}

}
