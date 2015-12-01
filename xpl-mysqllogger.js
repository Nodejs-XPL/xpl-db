var Xpl = require("xpl-api");
var commander = require('commander');
var mysql = require('mysql');
var os = require('os');
var debug = require('debug')('xpl-mysqllogger');

commander.version(require("./package.json").version);
commander.option("-h, --mysqlHost <host>", "Mysql host name");
commander.option("-P --mysqlPort <port>", "Mysql port", parseInt);
commander.option("-u --mysqlUser <user>", "Mysql user name");
commander.option("-p --mysqlPassword <password>", "Mysql password");
commander.option("-d --mysqlDatabase <database>", "Mysql database");
commander.option("--mysqlURL <url>", "Mysql url");

Xpl.fillCommander(commander);

commander.command("run").action(function() {

  var pool = mysql.createPool(commander.mysqlURL || {
    connectionLimit : 10,
    host : commander.mysqlHost || "localhost",
    port : commander.mysqlPort || 3306,
    user : commander.mysqlUser,
    password : commander.mysqlPassword,
    database : commander.mysqlDatabase
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

      if (false) {
        pool.getConnection(function(error, connection) {
          if (error) {
            console.error('error connecting: ', error, error.stack);
            return;
          }

          saveSensorBasic({
            device : "sensors/maison",
            type : "water",
            current : "663",
            units : "W"

          }, connection, function(error) {
            connection.release();

            console.error(error);
          });

        });
      }

      xpl.on("xpl:xpl-trig", function(message) {

        if (message.bodyName === "sensor.basic") {
          pool.getConnection(function(error, connection) {
            if (error) {
              console.error('error connecting: ', error, error.stack);
              return;
            }
            saveSensorBasic(message, connection, function(error) {
              connection.release();

              if (error) {
                console.error(error);
              }
            });
          });
        }
      });

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

  connection.query({
    sql : 'SELECT id FROM ?? WHERE name=?',
    timeout : 1000 * 20,
    values : [ type, name ]

  }, function(error, results, fields) {

    debug("getDeviceId.select: error=", error, " Results=", results, "fields=",
        fields);

    if (results.length === 1) {
      var id = results[0].id;
      map[name] = id;

      return callback(null, id);
    }

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

function saveSensorBasic(message, connection, callback) {
  debug("Save sensor.basic", message);
  var body = message.body;

  var deviceName = body.device;

  if (!deviceName) {
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
    date = Date.UTC(date);
  }

  var current = message.current;

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
