/*jslint node: true, vars: true, nomen: true, esversion: 6 */
'use strict';

const debug = require('debug')('xpl-db:mysql');
const mysql = require('mysql');
const async = require('async');

const SOURCE_SUPPORT = false;

class Mysql {
  constructor(configuration, deviceAliases) {
    this.configuration = configuration || {};
    this.deviceAliases = deviceAliases;

    this.sqlTimeout = 1000 * 20;

    this._deviceIds = {};
    this._sourceIds = {};
    this._unitIds = {};
    this._lastCache = {};
  }

  static fillCommander(commander) {
    commander.option("-h, --mysqlHost <host>", "Mysql host name");
    commander.option("-P --mysqlPort <port>", "Mysql port", parseInt);
    commander.option("-u --mysqlUser <user>", "Mysql user name");
    commander.option("-p --mysqlPassword <password>", "Mysql password");
    commander.option("-d --mysqlDatabase <database>", "Mysql database");
    commander.option("--mysqlURL <url>", "Mysql url");
  }

  create(callback) {
    var currentBool = "CREATE TABLE `currentBool` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT, `device` int(10) unsigned NOT NULL, `current` bit(1) DEFAULT NULL, `date` timestamp NOT NULL, PRIMARY KEY (`id`), KEY `bydate` (`device`,`date`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
    var currentNumber = "CREATE TABLE `currentBool` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT, `device` int(10) unsigned NOT NULL, `unit` int(4) unsigned DEFAULT NULL, `current` double DEFAULT NULL, `date` timestamp NOT NULL, PRIMARY KEY (`id`), KEY `bydate` (`device`,`date`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
    var currentString = "CREATE TABLE `currentBool` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT, `device` int(10) unsigned NOT NULL, `unit` int(4) unsigned DEFAULT NULL, `current` varchar(256) DEFAULT NULL, `date` timestamp NOT NULL, PRIMARY KEY (`id`), KEY `bydate` (`device`,`date`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

    var devices = "CREATE TABLE `devices` ( `id` int(11) NOT NULL AUTO_INCREMENT, `name` varchar(128) NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `byname` (`name`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
    var units = "CREATE TABLE `units` ( `id` int(11) NOT NULL AUTO_INCREMENT, `name` varchar(128) NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `byname` (`name`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
    var sources = "CREATE TABLE `sources` ( `id` int(11) NOT NULL AUTO_INCREMENT, `name` varchar(256) NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `byname` (`name`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

    var configuration = this.configuration;

    var connection = mysql.createConnection(configuration.mysqlURL || {
      host : configuration.mysqlHost || "localhost",
      port : configuration.mysqlPort || 3306,
      user : configuration.mysqlUser,
      password : configuration.mysqlPassword,
      database : configuration.mysqlDatabase || "xpl"
    });

    async.eachSeries(
        [ currentBool, currentNumber, currentString, devices, units ],
        connection.query, (error) => {
          connection.end(() => {
            debug("constructor", "DONE !");

            callback(error);
          });
        });
  }

  connect(callback) {
    var configuration = this.configuration;

    var options=configuration.mysqlURL || {
      connectionLimit : 10,
      host : configuration.mysqlHost || "localhost",
      port : configuration.mysqlPort || 3306,
      user : configuration.mysqlUser,
      password : configuration.mysqlPassword,
      database : configuration.mysqlDatabase || "xpl"
    };

    this._pool = mysql.createPool(options);

    debug("connect", "Pool created options=",options);

    callback();
  }

  _getTypeById(name, map, type, connection, callback) {
    if (!SOURCE_SUPPORT && type === "sources") {
      return callback();
    }

    var id = map[name];
    if (typeof (id) !== "undefined") {
      return callback(null, id);
    }

    var sql = 'SELECT id FROM ?? WHERE name=? limit 1';

    debug("_getTypeById", "Execute QUERY from ", type, sql);

    connection.query({
      sql : sql,
      timeout : this.sqlTimeout,
      values : [ type, name ]

    }, (error, results) => {

      debug("_getTypeById", "select: error=", error, "results=", results);

      if (results.length === 1) {
        var id = results[0].id;
        map[name] = id;
        map[id] = name;

        return callback(null, id);
      }

      debug("_getTypeById", "Execute INSERT into ", type);

      connection.query({
        sql : "INSERT INTO ?? SET ?",
        values : [ type, {
          name : name
        } ],
        timeout : this.sqlTimeout

      }, (error, result) => {
        debug("_getTypeById", "Insert: error=", error, " result=", result);
        if (error) {
          if (error.code === 'ER_DUP_ENTRY') {
            debug("_getTypeById", "Retry getDeviceId(", name, ")");
            setImmediate(this._getDeviceId.bind(this, name, connection,
                callback));
            return;
          }

          return callback(error);
        }

        var id = result.insertId;
        map[name] = id;
        map[id] = name;
        callback(null, id);
      });
    });
  }

  _insertCurrent(deviceId, sourceId, unitId, current, date, connection, callback) {
    var table = "currentString";

    if (/(enable|enabled|on|true)/i.exec(current)) {
      table = "currentBool";
      current = true;
      unitId = false;

    } else if (/(disable|disabled|off|false)/i.exec(current)) {
      table = "currentBool";
      current = false;
      unitId = false;

    } else if (/^[+-]?\d+(\.\d+)?$/.exec(current)) {
      table = "currentNumber";
      current = parseFloat(current);
    }
    var values = {
        device : deviceId,
        current : current,
        date : date
    };

    if (unitId) {
      values.unit = unitId;
    }
    if (sourceId !== undefined) {
      values.source = sourceId;
    }

    debug("_insertCurrent", "INSERT into table=",table,"values=", values);
    connection.query({
      sql : "INSERT INTO ?? SET ?",
      values : [ table, values ],
      timeout : this.sqlTimeout

    }, callback);
  }

  save(message, callback) {
    this._pool.getConnection((error, connection) => {
      if (error) {
        return callback(error);
      }
      this._save(message, connection, (error) => {
        connection.release();

        callback(error);
      });
    });
  }

  _save(message, connection, callback) {
    debug("_save", "Save sensor.basic", message);
    var body = message.body;

    var deviceName = body.device;
    var current = body.current;
    var sourceName = message.header.source;

    if (this.deviceAliases && this.deviceAliases[deviceName]) {
      deviceName = this.deviceAliases[deviceName];
    }

    if (!deviceName || current === undefined) {
      return callback();
    }

    if (body.type) {
      deviceName += "@" + body.type;
    }

    var units = body.units;
    var date = body.date;
    if (!date) {
      date = new Date(message.timestamp);

    } else if (/^[0-9]+$/.exec(date)) {
      date = new Date(parseInt(date));

    } else {
      date = new Date(date);
    }

    this._getTypeById(deviceName, this._deviceIds, "devices", connection, (error, deviceId) => {
      if (error) {
        return callback(error);
      }

      debug("_save", "Device=", deviceName, "=>", deviceId);

      this._getTypeById(sourceName, this.sourceIds, "sources", connection, (error, sourceId) => {
        if (error) {
          return callback(error);
        }

        if (SOURCE_SUPPORT) {
          debug("_save", "Source=", sourceName, "=>", sourceId);
        }

        var cache = {
            deviceName : deviceName,
            current : current,
            date : date
        };
        this._setCache(cache);

        if (!units) {
          return this._insertCurrent(deviceId, this._sourceId, undefined,
              current, date, connection, callback);
        }

        cache.units = units;

        this._getTypeById(units, this._unitIds, "units", connection, (error, unitId) => {
          if (error) {
            return callback(error);
          }

          debug("_save", "Unit=", units, '=>', unitId);

          this._insertCurrent(deviceId, sourceId, unitId, current,
              date, connection, callback);
        });
      });
    });
  }

  _setCache(value) {
    var old = this._lastCache[value.device];
    if (old) {
      if (old.date.getTime() > value.date.getTime()) {
        return;
      }
    }

    this._lastCache[value.device] = value;
  }

  getLast(deviceKey, options, callback) {
    debug("getLast", "Get last of deviceKey=", deviceKey, "options=",options);

    options = options || {};

    var result = this._lastCache[deviceKey];
    if (result) {
      debug("getLast", "Get last of deviceKey=", deviceKey, "returns cache result=", result);
      return callback(null, result);
    }

    options.sqlCB = (type) => {
      return 'SELECT current, date' + (type === 'Bool' ? '' : ', unit') +
      ' FROM ::database WHERE device=:deviceId order by date desc limit 1';
    };

    this.getHistory(deviceKey, options, (error, result) => {
      if (error) {
        return callback(error);
      }

      var ret = result[0];
      if (!ret) {
        return callback();
      }

      this._setCache(ret);

      return callback(null, ret);
    });
  }

  getHistory(key, options, callback) {
    options = options || {};

    if (!options.limit) {
      var d = options.dateMax || (new Date());
      options.dateMax = d;
      options.limit = 1 << 24; // On limite à 65536 points !

      if (!options.dateMin) {
        var d2 = new Date(d.getTime());
        d2.setDate(d2.getDate() - 1);

        options.dateMin = d2;
      }
    }

    if (!options.sqlCB) {
      options.sqlCB = (type) => {

        var where = "";

        if (options.dateMin) {
          if (options.dateMax) {
            where = ' AND date>=:dateMin AND date<=:dateMax';
          } else {
            where = ' AND date>=:dateMin';
          }

        } else if (options.dateMax) {
          where = ' AND date<=:dateMax';
        }

        return 'SELECT current, date' + (type === 'Bool' ? '' : ', unit') +
        ' FROM ::database WHERE device=:deviceId' + where +
        ' order by date ' + ((options.descending) ? "desc" : "asc") +
        ' limit :limit';
      };
    }

    this._getHistory(key, options, "Number", (error, current) => {
      if (error) {
        return callback(error);       
      } 

      if (current !== undefined) {
        return callback(null, current);
      }

      this._getHistory(key, options, "Bool", (error, current) => {
        if (error) {
          return callback(error);
        }

        if (current !== undefined) {
          return callback(null, current);
        }

        this._getHistory(key, options, "String", callback);
      });
    });
  }

  _getHistory(deviceKey, options, type, callback) {
    debug("_getHistory", "deviceKey=", deviceKey, "options=", options, "type=", type);

    this._searchDeviceId(deviceKey, (deviceId, connection, callback) => {

      options.deviceKey = deviceKey;
      options.deviceId = deviceId;

      var sql = options.sqlCB(type);

      sql = sql.replace(/\:\:database/g, connection.escapeId("current" + type));

      sql = sql.replace(/\:(\w+)/g, (txt, key) => {
        if (options.hasOwnProperty(key)) {
          return connection.escape(options[key]);
        }
        return txt;
      });

      debug("_getHistory", "Execute QUERY last from current", type, " for ", deviceKey, "(",
          deviceId, ") =>", sql);
      connection.query({
        sql : sql,
        timeout : this.sqlTimeout

      }, (error, results) => {
        if (error) {
          debug("Error", error, error.stack);
          return callback(error);
        }

        debug("_getHistory", "SQL returns=", results);
        if (!results || !results.length) {
          return callback();
        }

        var ar = [];
        var units = null;

        var fillArray = () => {
          for (var i = 0; i < results.length; i++) {
            var result = results[i];
            var current = result.current;

            if (Buffer.isBuffer(current)) {
              current = !!current[0];
            }

            var ret = {
                current : current,
                date : result.date
                // device : deviceKey
            };
            if (units) {
              ret.units = units;
            }
            if (options.hit) {
              options.hit(ar, ret);
              continue;
            }
            ar.push(ret);
          }

          if (options.hitEnd) {
            options.hitEnd(ar);
          }

          callback(null, ar);
        };

        var unitId = results[0].unit;

        if (unitId === undefined) {
          return fillArray();
        }

        var unitName = this._unitIds[unitId];
        if (unitName) {
          units = unitName;
          options.outputUnits = units;
          return fillArray();
        }

        var sql = 'SELECT name FROM units WHERE id=? limit 1';

        debug("_getHistory", "Execute QUERY from unit for ", unitId, "(", sql, ")");
        connection.query({
          sql : sql,
          timeout : this.sqlTimeout,
          values : [ unitId ]

        }, (error, results) => {
          if (error) {
            debug("_getHistory", "Error", error, error.stack);
            return callback(error);
          }

          if (results.length) {
            units = results[0].name;
            options.outputUnits = units;

            this._unitIds[unitId] = units;
            this._unitIds[units] = unitId;
          }

          fillArray();
        });
      });
    }, callback);
  }

  _searchDeviceId(deviceKey, callback, callbackEnd) {

    this._getConnection((connection, callbackEnd) => {

      var deviceId = this._deviceIds[deviceKey];
      if (deviceId !== undefined) {
        debug("_searchDeviceId", "Device id of key=", deviceKey, "=> [CACHED]", deviceId);

        callback(deviceId, connection, callbackEnd);
        return;
      }

      debug("_searchDeviceId", "Execute QUERY from devices for ", deviceKey);
      connection.query({
        sql : 'SELECT id FROM devices WHERE name=? limit 1',
        timeout : this.sqlTimeout,
        values : [ deviceKey ]

      }, (error, results) => {
        if (error) {
          console.error("_searchDeviceId", "Error", error, error.stack);
          return callbackEnd(error);
        }

        debug("_searchDeviceId", "Device id of key=", deviceKey, "=>", results);

        if (results.length < 1) {
          var ex = new Error("Device '" + deviceKey + "' is not found");
          ex.code = "NOT_FOUND";
          ex.key = deviceKey;

          return callbackEnd(ex);
        }

        var deviceId = results[0].id;
        this._deviceIds[deviceKey] = deviceId;
        this._deviceIds[deviceId] = deviceKey;

        callback(deviceId, connection, callbackEnd);
      });

    }, callbackEnd);
  }

  _getConnection(callback, callbackEnd) {
    this._pool.getConnection((error, connection) => {
      if (error) {
        return callbackEnd(error);
      }

      callback(connection, (error, result) => {
        connection.release();

        callbackEnd(error, result);
      });
    });
  }

  getMinMaxAvgSum(deviceKey, options, callback) {
    options = options || {};

    if (!options.limit) {
      var d = options.dateMax || (new Date());
      options.dateMax = d;

      if (!options.dateMin) {
        var d2 = new Date(d.getTime());
        d2.setDate(d2.getDate() - 1);

        options.dateMin = d2;
      }
    }

    options.descending = false; // Force ascending

    if (options.stepMs) {
      return this.getMinMaxAvgSumByStep(deviceKey, options, callback);
    }

    var min;
    var minDT;
    var max;
    var maxDT;
    var pred;
    var average = 0;
    var totalMs = 0;
    var sum = 0;
    options.hit = (list, pt) => {
      if (options.outputUnits && pt.units !== options.outputUnits) {
        console.error("UNIT is changing !!!!");
      }

      if (!min || min.current > pt.current) {
        min = pt;
      }
      if (!max || max.current < pt.current) {
        max = pt;
      }
      if (pred) {
        var dt = pt.date.getTime() - pred.date.getTime();
        totalMs += dt;
        // debug("DT=", dt, "current", pred.current);
        var p=pred.current * dt;
        average += p;
        
        if (!minDT || minDT.current > p) {
          minDT = {
              current: p,
              date: pt.date
          };
        }
        if (!maxDT || maxDT.current < p) {
          maxDT = {
              current: p,
              date: pt.date
          };
        }
      }
      pred = pt;
      sum += pt.current;
    };
    options.hitEnd = () => {
      if (!pred) {
        return;
      }

      var lastT = (options.maxDate && options.maxDate.getTime()) || Date.now();

      var dt = lastT - pred.date.getTime();
      // debug("DT=", dt, "current", pred.current);
      totalMs += dt;
      average += pred.current * dt;
    };

    this.getHistory(deviceKey, options, (error, result) => {
      if (error) {
        return callback(error);
      }

      if (!min) {
        return callback();
      }

      delete min.units;
      delete max.units;

      var ret = {
          min : min,
          minDT: minDT,
          max : max,
          maxDT: maxDT,
          average : average / totalMs,
          sum : sum
      };
      if (options.outputUnits) {
        ret.units = options.outputUnits;
      }

      callback(null, ret);
    });
  }

  getMinMaxAvgSumByStep(deviceKey, options, callback) {
    options = options || {};

    if (!options.limit) {
      var d = options.dateMax || (new Date());
      options.dateMax = d;

      if (!options.dateMin) {
        var d2 = new Date(d.getTime());
        d2.setDate(d2.getDate() - 1);

        options.dateMin = d2;
      }
    }
    if (!options.stepMs) {
      options.stepMs = (options.dateMax.getTime() - options.dateMin.getTime()) / 10;
    }

    options.descending = false; // Force ascending

    var currentIndex = -1;
    var pred;
    var minTime = options.dateMin.getTime();

    var stepMs = options.stepMs;

    var stepTo = (targetDate, pt, list) => {

      var ci = Math.floor((targetDate.getTime() - minTime) / stepMs);

      for (var i = Math.max(0, currentIndex); i <= ci; i++) {
        if (list[i]) {
          continue;
        }
        list[i] = {
            startDate : new Date(minTime + i * stepMs),
            endDate : new Date(minTime + (i + 1) * stepMs - 1),
            min : undefined,
            max : undefined,
            sum : 0,
            average : 0,
            totalMs : 0
        };
      }

      var cell = list[ci];
      if (pt) {
        if (!cell.min || cell.min.current > pt.current) {
          cell.min = pt;
        }
        if (!cell.max || cell.max.current < pt.current) {
          cell.max = pt;
        }
      }

      if (!pred) {
        pred = pt;
        currentIndex = ci;
        return;
      }

      var dt = targetDate - pred.date.getTime();

      if (ci === currentIndex) {
        cell.sum += pred.current;
        cell.totalMs += dt;
        cell.average += pred.current * dt;

        pred = pt;
        return;
      }

      // start
      cell = list[currentIndex];

      var ts = 0;
      var dtBegin = cell.endDate.getTime() - pred.date.getTime();
      var curBegin = pred.current * (dtBegin / dt);
      cell.sum += curBegin;
      cell.totalMs += dtBegin;
      ts += dtBegin;
      cell.average += curBegin * dtBegin;

      // mid
      for (currentIndex++; currentIndex < ci; currentIndex++) {
        var curMed = pred.current * (stepMs / dt);
        cell = list[currentIndex];
        ts += stepMs;
        cell.sum += curMed;
        cell.totalMs += stepMs;
        cell.average += curMed * stepMs;
      }

      // end
      cell = list[currentIndex];
      var dtEnd = targetDate.getTime() - cell.startDate.getTime();
      var curEnd = pred.current * (dtEnd / dt);
      ts += dtEnd;
      cell.sum += curEnd;
      cell.totalMs += dtEnd;
      cell.average += curEnd * dtEnd;

      //console.log(ts + "/" + dt);

      pred = pt;
    };

    options.hit = (list, pt) => {
      if (options.outputUnits && pt.units !== options.outputUnits) {
        console.error("UNIT is changing !!!!");
      }

      stepTo(pt.date, pt, list);
    };
    options.hitEnd = (list) => {
      if (!pred) {
        return;
      }

      var lastT = options.maxDate || (new Date());

      stepTo(lastT, null, list);
    };

    this.getHistory(deviceKey, options, (error, results) => {
      if (error) {
        return callback(error);
      }

      if (!results || !results.length) {
        return callback();
      }

      if (options.outputUnits) {
        results.units = options.outputUnits;
      }

      results.forEach((r) => r.average /= r.totalMs);

      callback(null, results);
    });
  }
  
  getCumulated(deviceKey, options, callback) {
    options = options || {};

    if (!options.limit) {
      var d = options.dateMax || (new Date());
      options.dateMax = d;

      if (!options.dateMin) {
        var d2 = new Date(d.getTime());
        d2.setDate(d2.getDate() - 1);

        options.dateMin = d2;
      }
    }
    var pred=undefined;
    var cumul=0;
    
    options.hit = (list, pt) => {
      if (options.outputUnits && pt.units !== options.outputUnits) {
        console.error("UNIT is changing !!!!");
      }

      if (pred===undefined || pt.current<pred) {
        pred=pt.current;
        return;
      }
      cumul+=pt.current-pred;
      
      pred=pt.current;
    };
  
    this.getHistory(deviceKey, options, (error, results) => {
      if (error) {
        return callback(error);
      }

      if (!results || !results.length) {
        return callback();
      }

      if (options.outputUnits) {
        results.units = options.outputUnits;
      }

      callback(null, cumul);
    });
  }
}

module.exports = Mysql;
