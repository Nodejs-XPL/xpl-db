/*jslint node: true, vars: true, nomen: true */
'use strict';

var Xpl = require("xpl-api");
var commander = require('commander');
var os = require('os');
var debug = require('debug')('xpl-db');
var Mysql = require('./mysql');

commander.version(require("./package.json").version);
commander.option("-a, --deviceAliases <aliases>", "Devices aliases");

Mysql.fillCommander(commander);
Xpl.fillCommander(commander);

var Store = Mysql;

commander.command("create").action(function() {

  var store = new Store(commander);

  store.create(function(error) {
    if (error) {
      console.error(error);
      return;
    }
  });
});

commander.command("run").action(function() {

  var deviceAliases = Xpl.loadDeviceAliases(commander.deviceAliases);

  var store = new Store(commander, deviceAliases);

  store.connect(function(error) {
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
            store.save(message, function(error) {
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
      });
    } catch (x) {
      console.error(x);
    }
  });
});

commander.parse(process.argv);
