var Xpl = require("xpl-api");
var commander = require('commander');
var mysql = require('mysql');
var os = require('os');
var debug = require('debug')('xpl-mysqllogger');
var async = require('async');

commander.version(require("./package.json").version);
commander.option("-h, --mysqlHost <host>", "Mysql host name");
commander.option("-P --mysqlPort <port>", "Mysql port", parseInt);
commander.option("-u --mysqlUser <user>", "Mysql user name");
commander.option("-p --mysqlPassword <password>", "Mysql password");
commander.option("-d --mysqlDatabase <database>", "Mysql database");
commander.option("--mysqlURL <url>", "Mysql url");
commander.option("-a, --deviceAliases <aliases>", "Devices aliases");

Xpl.fillCommander(commander);

commander
    .command("create")
    .action(
        function() {

          var currentBool = "CREATE TABLE `currentBool` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT, `device` int(10) unsigned NOT NULL, `current` bit(1) DEFAULT NULL, `date` timestamp NOT NULL, PRIMARY KEY (`id`), KEY `bydate` (`device`,`date`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
          var currentNumber = "CREATE TABLE `currentBool` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT, `device` int(10) unsigned NOT NULL, `unit` int(4) unsigned DEFAULT NULL, `current` double DEFAULT NULL, `date` timestamp NOT NULL, PRIMARY KEY (`id`), KEY `bydate` (`device`,`date`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
          var currentString = "CREATE TABLE `currentBool` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT, `device` int(10) unsigned NOT NULL, `unit` int(4) unsigned DEFAULT NULL, `current` varchar(256) DEFAULT NULL, `date` timestamp NOT NULL, PRIMARY KEY (`id`), KEY `bydate` (`device`,`date`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

          var devices = "CREATE TABLE `devices` ( `id` int(11) NOT NULL AUTO_INCREMENT, `name` varchar(128) NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `byname` (`name`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
          var units = "CREATE TABLE `units` ( `id` int(11) NOT NULL AUTO_INCREMENT, `name` varchar(128) NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `byname` (`name`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

          var connection = mysql.createConnection(commander.mysqlURL || {
            host : commander.mysqlHost || "localhost",
            port : commander.mysqlPort || 3306,
            user : commander.mysqlUser,
            password : commander.mysqlPassword,
            database : commander.mysqlDatabase || "xpl"
          });

          async.eachSeries([ currentBool, currentNumber, currentString,
              devices, units ], connection.query, function(error) {
            if (error) {
              console.error(error);
              return;
            }

            connection.end(function() {
              console.log("DONE !");
            });
          });
        });

commander.command("run").action(
    function() {

      var pool = mysql.createPool(commander.mysqlURL || {
        connectionLimit : 10,
        host : commander.mysqlHost || "localhost",
        port : commander.mysqlPort || 3306,
        user : commander.mysqlUser,
        password : commander.mysqlPassword,
        database : commander.mysqlDatabase || "xpl"
      });

      try {
        if (!commander.xplSource) {
          var hostName = os.hostname();
          if (hostName.indexOf('.') > 0) {
            hostName = hostName.substring(0, hostName.indexOf('.'));
          }

          commander.xplSource = "mysqllogger." + hostName;
        }

        var xpl = new Xpl(commander);

        var deviceAliases = Xpl.loadDeviceAliases(commander.deviceAliases);

        xpl.on("error", function(error) {
          console.log("XPL error", error);
        });

        xpl.bind(function(error) {
          if (error) {
            console.log("Can not open xpl bridge ", error);
            process.exit(2);
            return;
          }

          console.log("Xpl bind succeed ");

          function processMessage(message) {

            if (message.bodyName === "sensor.basic") {
              pool.getConnection(function(error, connection) {
                if (error) {
                  console.error('error connecting: ', error, error.stack);
                  return;
                }
                saveSensorBasic(message, connection, deviceAliases, function(
                    error) {
                  connection.release();

                  if (error) {
                    console.error(error);
                  }
                });
              });
              return;
            }
          }
          xpl.on("xpl:xpl-trig", processMessage);
          xpl.on("xpl:xpl-stat", processMessage);
        });
      } catch (x) {
        console.log(x);
      }
    });

var deviceIds = {};
var unitIds = {};

function getTypeById(name, map, type, connection, callback) {
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

function insertCurrent(deviceId, unitId, current, date, connection, callback) {
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

  connection.query({
    sql : "INSERT INTO ?? SET ?",
    values : [ table, values ],
    timeout : 1000 * 20

  }, callback);
}

function saveSensorBasic(message, connection, deviceAliases, callback) {
  debug("Save sensor.basic", message);
  var body = message.body;

  var deviceName = body.device;
  var current = body.current;

  if (deviceAliases && deviceAliases[deviceName]) {
    deviceName = deviceAliases[deviceName];
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
    date = new Date();

  } else if (/^[0-9]+$/.exec(date)) {
    date = new Date(parseInt(date));

  } else {
    date = new Date(date);
  }

  getTypeById(deviceName, deviceIds, "devices", connection, function(error,
      deviceId) {
    if (error) {
      return callback(error);
    }

    debug("Device=", deviceName, "=>", deviceId);

    if (!units) {
      return insertCurrent(deviceId, undefined, current, date, connection,
          callback);
    }

    getTypeById(units, unitIds, "units", connection, function(error, unitId) {
      if (error) {
        return callback(error);
      }

      debug("Unit=", units, '=>', unitId);

      insertCurrent(deviceId, unitId, current, date, connection, callback);
    });
  });
}

commander.parse(process.argv);
