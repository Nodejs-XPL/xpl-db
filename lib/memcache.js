/*jslint node: true, vars: true, nomen: true, esversion: 6 */
'use strict';

const Memcache = require('memcache');
const debug = require('debug')('xpl-db:memcache');


const KEY_PREFIX="xpl:";

const NUMBER_UNITS = [ "c", "%", "mm" ];

const TRUE_CURRENT = /(on|enable|true)/i;

const FALSE_CURRENT = /(off|disable|false)/i;

class XplMemcache {
  constructor(configuration, deviceAliases) {
    configuration=configuration || {};

    this._configuration=configuration;
    this._deviceAliases=deviceAliases;

    this._keyPrefix=configuration.keyPrefix || KEY_PREFIX;
  }

  static fillCommander(commander) {
    commander.option("--memcacheHost <host>", "Memcache host name");
    commander.option("--memcachePort <port>", "Memcache port", parseInt);
  }

  initialize(callback) {
    var configuration =  this._configuration;

    var client = new Memcache.Client(configuration.memcachePort, configuration.memcacheHost);
    
    var connecting=true;
    client.on('connect', (error) => {
      if (!connecting) {
        return;
      }
      connecting=false;

      if (error) {
        console.error("Connect error",error);
        return callback(error);
      }
      debug("initialize", "Memcache connected");

      this._memcacheClient=client;

      callback();
    });

    client.on('error', (error) => {
      console.error("Memcache error", error);
 
      if (!connecting) {
        return;
      }
      connecting=false;
      callback(error);
    });

    client.on('close', () => {
      debug("initialize", "Memcache closed");

      this._memcacheClient=null;
    });
    client.on('timeout', () => {
      console.error("Memcache timeout");      
    });

    client.connect();
  }

  getCurrent(path, callback) {
    if (!this._memcacheClient) {
      console.error("No memcache !");
      return callback(new Error("No memcache client"));
    }

    var key = this._keyPrefix + path.replace(/[\s]/g, '_');
    debug("getCurrent", "Search path=", path,"key=",key);

    this._memcacheClient.get(key, (error, result) => {
      if (error) {
        console.error(error);
        return callback(error);
      }
      if (!result) {
        return callback();
      }

      var obj = JSON.parse(result);
      
      if (obj && (typeof(obj.date)==="string" || typeof(obj.date)==="number")) {
        obj.date=new Date(obj.date);
      }

      debug("getCurrent", "Get", path, "return =>", obj);

      callback(null, obj);
    });
  }

  saveMessage(message) {
    var current = message.body.current || message.body.command;
    if (!current) {
      return;
    }
    
    var deviceName = message.body.device;
    if (this._deviceAliases && this._deviceAliases[deviceName]) {
      deviceName = this._deviceAliases[deviceName];
    }

    var path = deviceName || message.body.address;
    if (!path) {
      return;
    }

    var type = message.body.type || message.body.unit;
    if (type) {
      path += "/" + type;
    }

    var currentLC = current.toLowerCase();

    if (NUMBER_UNITS[message.body.units || ''] ||
        /^(-?[0-9]+)(\.[0-9])?$/.exec(current)) {
      current = parseFloat(current);

    } else if (TRUE_CURRENT.exec(currentLC)) {
      current = true;

    } else if (FALSE_CURRENT.exec(currentLC)) {
      current = false;
    }

    var d=message.body.date && new Date(message.body.date);
    if (!d) {
      d=new Date();
    }
    
    var v = JSON.stringify({
      date : d,
      current : current,
      units : message.body.units
    });

    var key = this._keyPrefix + path.replace(/[\s]/g, '_');

    // console.log("Save memcache", key, " value=", v);

    setTimeout(() => {
      if (!this._memcacheClient) {
        console.error("No memcache !");
        return;
      }

      debug("saveMessage", "Set memcache key=",key,"value=",v);

      this._memcacheClient.set(key, v, (error, result) => {
        if (error) {
          console.error("SET-ERROR '" + key + "' (" + v + ")", error);
        }
      });
    }, 100);
  }
  
  saveRestServerURL(url, callback) {
    if (!this._memcacheClient) {
      return callback(new Error("No memcache client"));
    }
    
    debug("saveRestServerURL", "Set REST server url to ",url);

    this._memcacheClient.set(this._keyPrefix+"@@REST_SERVER_URL", url, callback);
  }
  
  getRestServerURL(callback) {
    if (!this._memcacheClient) {
      return callback(new Error("No memcache client"));
    }
    
    this._memcacheClient.get(this._keyPrefix+"@@REST_SERVER_URL", (error, url) => {
      debug("getRestServerURL", "Get REST server url=>",url,"error=",error);
      
      callback(error, url);
    });
  }
  
  close(callback) {
    if (!callback) {
      callback=()=>{};
    }
    if (!this._memcacheClient) {
      return callback();
    }
    
    this._memcacheClient.close();
    this._memcacheClient=null;
    callback();
  }
}

module.exports = XplMemcache;