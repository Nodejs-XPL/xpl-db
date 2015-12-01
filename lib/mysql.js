/*jslint node: true, vars: true, nomen: true */
'use strict';

var debug = require('debug')('xpl-db:mysql');
var mysql = require('mysql');
var async = require('async');

var SOURCE_SUPPORT = false;

function Mysql(configuration, deviceAliases) {
  this.configuration = configuration || {};
  this.deviceAliases = deviceAliases;

  this.deviceIds = {};
  this.sourceIds = {};
  this.unitIds = {};
}

module.exports = Mysql;

Mysql.fillCommander = function(commander) {
  commander.option("-h, --mysqlHost <host>", "Mysql host name");
  commander.option("-P --mysqlPort <port>", "Mysql port", parseInt);
  commander.option("-u --mysqlUser <user>", "Mysql user name");
  commander.option("-p --mysqlPassword <password>", "Mysql password");
  commander.option("-d --mysqlDatabase <database>", "Mysql database");
  commander.option("--mysqlURL <url>", "Mysql url");
}

Mysql.prototype.create = function(callback) {
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
      connection.query, function(error) {
        connection.end(function() {
          debug("DONE !");

          callback(error);
        });
      });
}

Mysql.prototype.connect = function(callback) {
  var configuration = this.configuration;

  this._pool = mysql.createPool(configuration.mysqlURL || {
    connectionLimit : 10,
    host : configuration.mysqlHost || "localhost",
    port : configuration.mysqlPort || 3306,
    user : configuration.mysqlUser,
    password : configuration.mysqlPassword,
    database : configuration.mysqlDatabase || "xpl"
  });

  debug("Pool created");

  callback();
};

Mysql.prototype._getTypeById = function(name, map, type, connection, callback) {
  if (!SOURCE_SUPPORT && type === "sources") {
    return callback();
  }

  var id = map[name];
  if (typeof (id) !== "undefined") {
    return callback(null, id);
  }

  debug("Execute QUERY from ", type);
  connection.query({
    sql : 'SELECT id FROM ?? WHERE name=?',
    timeout : 1000 * 20,
    values : [ type, name ]

  }, function(error, results, fields) {

    debug("getDeviceId.select: error=", error, "results=", results);

    if (results.length === 1) {
      var id = results[0].id;
      map[name] = id;

      return callback(null, id);
    }

    debug("Execute INSERT into ", type);

    connection.query({
      sql : "INSERT INTO ?? SET ?",
      values : [ type, {
        name : name
      } ],
      timeout : 1000 * 20

    }, function(error, result) {
      debug("getDeviceId.Insert: error=", error, " result=", result);
      if (error) {
        if (error.code === 'ER_DUP_ENTRY') {
          debug("Retry getDeviceId(", name, ")");
          setImmediate(getDeviceId.bind(this, name, connection, callback));
          return;
        }

        return callback(error);
      }

      var id = result.insertId;
      map[name] = id;
      callback(null, id);
    });
  });
}

Mysql.prototype._insertCurrent = function(deviceId, sourceId, unitId, current,
    date, connection, callback) {
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
  }
  if (unitId) {
    values.unit = unitId;
  }
  if (sourceId !== undefined) {
    values.source = sourceId;
  }

  connection.query({
    sql : "INSERT INTO ?? SET ?",
    values : [ table, values ],
    timeout : 1000 * 20

  }, callback);
}

Mysql.prototype.save = function(message, callback) {
  var self = this;
  this._pool.getConnection(function(error, connection) {
    if (error) {
      return callback(error);
    }
    self._save(message, connection, function(error) {
      connection.release();

      callback(error);
    });
  });
}

Mysql.prototype._save = function(message, callback) {
  debug("Save sensor.basic", message);
  var body = message.body;

  var deviceName = body.device;
  var current = body.current;
  var sourceName = message.head.source;

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

  var self = this;
  this._getTypeById(deviceName, deviceIds, "devices", connection, function(
      error, deviceId) {
    if (error) {
      return callback(error);
    }

    debug("Device=", deviceName, "=>", deviceId);

    self._getTypeById(sourceName, sourceIds, "sources", connection, function(
        error, sourceId) {
      if (error) {
        return callback(error);
      }

      debug("Device=", sourceName, "=>", sourceId);

      if (!units) {
        return self._insertCurrent(deviceId, sourceId, undefined, current,
            date, connection, callback);
      }

      self._getTypeById(units, unitIds, "units", connection, function(error,
          unitId) {
        if (error) {
          return callback(error);
        }

        debug("Unit=", units, '=>', unitId);

        self._insertCurrent(deviceId, sourceId, unitId, current, date,
            connection, callback);
      });
    });
  });
}