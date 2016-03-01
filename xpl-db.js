/*jslint node: true, vars: true, nomen: true */
'use strict';

var Xpl = require("xpl-api");
var commander = require('commander');
var os = require('os');
var debug = require('debug')('xpl-db');
var Mysql = require('./lib/mysql');
var Server = require('./lib/server');

commander.version(require("./package.json").version);
commander.option("-a, --deviceAliases <aliases>", "Devices aliases");
commander.option("--httpPort <port>", "REST server port", parseInt);

Mysql.fillCommander(commander);
Xpl.fillCommander(commander);

var Store = Mysql;

commander.command("create").action(() => {

  var store = new Store(commander);

  store.create(function(error) {
    if (error) {
      console.error(error);
      return;
    }
  });
});


commander.command("rest").action(() => {

  var deviceAliases = Xpl.loadDeviceAliases(commander.deviceAliases);

  var store = new Store(commander, deviceAliases);

  var server = new Server(commander, store);

  server.listen((error) => {
    if (error) {
      console.error(error);
    }
  });
});

commander.command("store").action(() => {

  var deviceAliases = Xpl.loadDeviceAliases(commander.deviceAliases);

  var store = new Store(commander, deviceAliases);

  store.connect((error) => {
    if (error) {
      console.error(error);
      return;
    }
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

        console.log("Xpl bind succeed ");

        var processMessage = (message) => {

          if (message.bodyName === "sensor.basic") {
            store.save(message, (error) => {
              if (error) {
                console.error('error connecting: ', error, error.stack);
                return;
              }
            });
            return;
          }
        }
        xpl.on("xpl:xpl-trig", processMessage);
        xpl.on("xpl:xpl-stat", processMessage);

        xpl.on("message", function(message, packet, address) {

        });

      });
    } catch (x) {
      console.error(x);
    }
  });
});

commander.parse(process.argv);
