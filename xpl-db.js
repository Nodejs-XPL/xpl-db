/*jslint node: true, vars: true, nomen: true, esversion: 6 */
'use strict';

const Xpl = require("xpl-api");
const commander = require('commander');
const os = require('os');
const debug = require('debug')('xpl-db');
const Mysql = require('./lib/mysql');
const Server = require('./lib/server');
const Memcache = require('./lib/memcache');
const Async = require('async');
const ip = require('ip');
const API = require('./lib/API');

commander.version(require("./package.json").version);
commander.option("-a, --deviceAliases <aliases>", "Devices aliases");
commander.option("--httpPort <port>", "REST server port", parseInt);
commander.option("--configPath <path>", "Static config files of http server");
commander.option("--xplCommand", "Enable xpl commands by Http command");
commander.option("--memcached", "Store xpl values in memcache");
commander.option("--db", "Store xpl values in a DB");

Mysql.fillCommander(commander);
Xpl.fillCommander(commander);
Memcache.fillCommander(commander);

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
  var initCbs=[];
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
      var server = new Server(commander, store, xpl, memcache);
    
      server.listen((error, server) => {
        if (error) {
          console.error(error);
        }

        if (memcache && store) {
          process.on('exit', () => {
            memcache.saveRestServerURL('', (error) => {
              debug("xpl-db", "Reset rest server URL into memcache !");
            });
          });
        }
        
        var url="http://"+ ip.address() + ":"+ server.address().port;
        debug("xpl-db", "Set rest server url to",url);
        if (!memcache) {
          return;
        }
        memcache.saveRestServerURL(url, (error) => {
          if (error) {
            console.error(error);
          }
        });
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

  var deviceAliases = Xpl.loadDeviceAliases(commander.deviceAliases);
  
  debug("store", "Store starting ... deviceAliases=",deviceAliases);

  var store;
  var memcache;
  var initCbs=[];
  
  if (commander.db) {
    initCbs.push((callback) => {
      store = new Store(commander, deviceAliases);

      store.connect((error) => {
        if (error) {
          debug("store", "Store error=",error);
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
          debug("store", "Memcached error=",error);
          return callback(error);
        }
        
        debug("store", "Memcached connected");
        
        callback();
      });
    });
  }
  
  Async.parallel(initCbs, (error) => {
    if (error) {
      debug("store", "Initialization error=",error);
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

        var processMessage = (message) => {

          if (message.bodyName === "sensor.basic") {
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
            
            return;
          }
        };
        
        xpl.on("xpl:xpl-trig", processMessage);
        xpl.on("xpl:xpl-stat", processMessage);

        /*
         * xpl.on("message", (message, packet, address) => {
         * 
         * });
         */

      });
    } catch (x) {
      console.error(x);
    }
  });
});

commander.command("request").action((path) => {
  var query=new API.Query(commander);
  
  query.getValue(path, (error, value) => {
    if (error) {
      console.error(error);
      return;
    }
    
    console.log(value);
    
    query.close();
  });
  
});

commander.parse(process.argv);
