const debug = require('debug')('xpl-db:mongo');
const MongoClient = require('mongodb').MongoClient;
const async = require('async');
const Db = require('./db');
const util = require('util');

class Mongo extends Db {
    constructor(configuration, deviceAliases) {
        super(configuration, deviceAliases);

        this._storeOptimization = (this.configuration.storeOptimization === true);

        debug("constructor", "storeOptimization=", this._storeOptimization);
    }

    static fillCommander(commander) {
        commander.option("--mongoURL <url>", "Mongo url (mongodb://localhost/xpl)");
        commander.option("--storeOptimization", "Mongo: optimize stored object");
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

            debug("connect", "Mongodb connected to url=", url);

            const messages = db.collection('messages');

            const daycache = db.collection('daycache');

            messages.createIndex({date: 1}, {name: 'byDate'}, (error, result) => {
                debug("connect", "Messages.CreateIndex 1: error=", error, "result=", result);
                if (error) {
                    return callback(error);
                }

                messages.createIndex({deviceKey: 1, date: 1}, {name: 'byDeviceDate', sparse: true}, (error, result) => {
                    debug("connect", "Messages.CreateIndex 2: error=", error, "result=", result);
                    if (error) {
                        return callback(error);
                    }

                    this._messagesCollection = messages;

                    daycache.createIndex({deviceKey: 1, startDate: 1}, {
                        name: 'byDeviceDate',
                        sparse: true,
                        unique: true,

                    }, (error, result) => {
                        debug("connect", "Daycache.CreateIndex 1: error=", error, "result=", result);
                        if (error) {
                            return callback(error);
                        }

                        this._daycacheCollection = daycache;

                        callback();
                    });
                });
            });
        });
    }


    getMinMaxAvgSumForADay(deviceKey, min, max, callback) {

        const query = {
            deviceKey: deviceKey,
            startDate: {
                $gte: min,
                $lt: max,
            }
        };

        let cursor = this._daycacheCollection.find(query, {_id: 0});
        cursor = cursor.sort({'startDate': 1});

        cursor.toArray((error, result) => {

            debug('getMinMaxAvgSumForADay', 'result=', result, 'query=', query);

            callback(error, result);
        });

    }

    saveMinMaxAvgSumForADay(deviceKey, date, data, callback) {
//        debug('saveMinMaxAvgSumForADay', 'save date=', date, 'result=', result);

        data = {
            ...data,
            deviceKey,
        };

        this._daycacheCollection.insertOne(data, (error, result) => {
            debug("saveMinMaxAvgSumForADay", "message inserted error=", error, "data=", data);

            if (error) {
                // Already save
                return callback(/*error*/);
            }

            callback(null, result);
        });
    }


    save(message, callback) {
        message = Object.assign({}, message);

        let timestamp = message.timestamp || Date.now();
        if (typeof (timestamp) === "string" || typeof (timestamp) === "number") {
            timestamp = new Date(timestamp);
            message.timestamp = timestamp;
        }

        let bodyDate = message.body.date;
        if (typeof (bodyDate) === "string" || typeof (bodyDate) === "number") {
            bodyDate = new Date(bodyDate);
            message.body.date = bodyDate;
        }
        if (util.isDate(bodyDate)) {
            message.date = bodyDate;

        } else {
            message.date = timestamp;
        }

        if (this._storeOptimization) {
            if (message.date == message.timestamp) {
                delete message.timestamp;
            }
            delete message.from;
            if (message.header) {
                if (message.header.hop === "1") {
                    delete message.header.hop;
                }
                if (message.header.target === "*") {
                    delete message.header.target;
                }
            }
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
        const query = {deviceKey: deviceKey};

        if (options.dateMin) {
            query.date = query.date || {};
            query.date.$gte = options.dateMin;
        }

        if (options.dateMax) {
            query.date = query.date || {};
            query.date.$lt = options.dateMax;
        }

        if (options.order) {
            order = options.order;
        }

        let cursor = this._messagesCollection.find(query);

        cursor = cursor.sort({'date': order});

        if (typeof (options.limit) === "number" || limit > 0) {
            cursor = cursor.limit(options.limit || limit);
        }

        if (projectCurrent) {
            cursor = cursor.project({"body.current": 1, "body.units": 1, "date": 1});
        }

        if (debug.enabled) {
            debug("_createCursor", "cursor=", cursor.cmd);
        }

        return cursor;
    }

    getLastMessage(deviceKey, options, callback) {

        let cursor = this._createCursor(deviceKey, options, -1, 1);

        debug("getLastMessage", "deviceKey=", deviceKey, "cursor=", cursor.cmd);

        let now = Date.now();
        cursor.next((error, result) => {

            debug("getLastMessage", "result in ", (Date.now() - now), "ms");

            if (error) {
                debug("getLastMessage", "deviceKey=", deviceKey, "error=", error);
                callback(error);
                return;
            }

            debug("getLastMessage", "deviceKey=", deviceKey, "result=", result);

            if (result && this._storeOptimization) {
                if (result.header && !result.header.hop) {
                    result.header.hop = "1";
                }
                if (result.header && !result.header.target) {
                    result.header.target = "*";
                }
                if (!result.timestamp && result.date) {
                    result.timestamp = result.date;
                }
            }

            callback(null, result);
        });
    }

    getLast(deviceKey, options, callback) {

        let cursor = this._createCursor(deviceKey, options, -1, 1, true);

        debug("getLast", "deviceKey=", deviceKey, "cursor=", cursor.cmd);

        let now = Date.now();
        cursor.next((error, result) => {

            debug("getLast", "result in ", (Date.now() - now), "ms");

            if (error) {
                return callback(error);
            }

            if (!result) {
                let error = new Error("Can not find device=" + deviceKey);
                callback(error);
                return;
            }

            result = this._normalize(result);

            debug("getLast", "deviceKey=", deviceKey, "normalized result=", result);
            callback(null, result);
        });
    }

    getHistory(deviceKey, options = {}, callback) {
        const projection = options.projection || {
            units: true,
        };

        let cursor = this._createCursor(deviceKey, options, 1, -1, true);

        debug("getHistory", "deviceKey=", deviceKey, "cursor=", cursor.cmd);

        let now = Date.now();
        cursor.toArray((error, result) => {
            debug("getHistory", "result in ", (Date.now() - now), "ms");

            if (error) {
                debug("getHistory", "deviceKey=", deviceKey, "error=", error);
                callback(error);
                return;
            }

            if (!result) {
                let error = new Error("Can not find device=" + deviceKey);
                callback(error);
                return;
            }

            //debug("getHistory", "deviceKey=", deviceKey, "result=", result);

            if (!options || !options.hit) {
                result = result.map((result) => this._normalize(result, projection));

                return callback(null, result);
            }

            const ar = [];
            result.forEach((pt) => {
                const p = this._normalize(pt);
                options.hit(ar, p);
            });

            if (options.hitEnd) {
                options.hitEnd(ar);
            }

            callback(null, ar);
        });
    }

    _normalize(result, projection) {
        let body = result.body;
        let ret = {
            current: body.current,
            date: body.date || result.date,
            units: undefined,
        };
        if (!projection || projection.units) {
            ret.units = body.units;
        }

        return ret;
    }
}

module.exports = Mongo;

