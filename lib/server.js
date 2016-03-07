/*jslint node: true, vars: true, nomen: true */
'use strict';

const express = require('express');
const Http = require('http');
const debug = require('debug')('xpl-db:server');
const async = require('async');
const bodyParser = require('body-parser');
const serve_static = require('serve-static');
const compression = require('compression');

const NO_CACHE_CONTROL = "no-cache, private, no-store, must-revalidate, max-stale=0, max-age=1,post-check=0, pre-check=0";

class Server {
  constructor(configuration, store, xpl) {
    this.configuration = configuration || {};
    this.store = store;
    this.xpl = xpl;

    this.app = this.configuration.express || express();
  }

  listen(callback) {
    var app = this.app;

    app.enable('etag');
    app.use(compression());  
    app.use(bodyParser.json());
    app.use(bodyParser.urlencoded({
      extended: true
    }));
// app.use(noCache);

    app.get(/^\/last\/(.*)$/, this._getLast.bind(this));
    app.get(/^\/history\/(.*)$/, this._getHistory.bind(this));
    app.get(/^\/minMaxAvgSum\/(.*)$/, this._getMinMaxAvgSum.bind(this));

    if (this.xpl) {
      app.get(/^\/xplCommand\/(.*)$/, this._xplCommand.bind(this));
      app.get(/^\/proxyCommands(.*)$/, this._proxyCommands.bind(this));
    }

    app.post('/last', this._postLast.bind(this));
    app.post('/history', this._postHistory.bind(this));
    app.post('/minMaxAvgSum', this._postMinMaxAvgSum.bind(this));

    if (this.configuration.configPath) {

      var oneYear = 1000*60*60*24*365;      

      app.use(express.static(__dirname + '/public', {  }));

      app.use("/config", serve_static(this.configuration.configPath, {
        index : false, 
        maxAge: oneYear
      }));
    }

    app.use(function(req, res, next) {
      res.status(404).send('Sorry cant find that!');
    });

    var server = app.listen(this.configuration.httpPort || 8480, (error) => {
      if (error) {
        console.error("Server can not listen", error);
        return;
      }
      debug("listen", "Server is listening ", server.address());

      callback(error);
    });
  }
  
  _proxyCommands(request, response) {
    var hs = {
        'Content-Type': 'application/json',
        'Cache-Control': NO_CACHE_CONTROL
    };
    response.writeHead(200, hs);

    var cb = (message, packet, address) => {
      var infos= {
          date: (new Date()).toUTCString(),
          from: address.address()+":"+address.port,
          message: message
      };
      
      response.write(JSON.stringify(infos));
    };
    
    this.xpl.on("message", cb);
    
    response.on('error', (error) => {
      console.error("Catch error !");
      
      this.xpl.removeListener("message", cb);      
    });
    
    response.on('end', () => {
      console.error("Catch end !");
      
      this.xpl.removeListener("message", cb);      
    });
  }

  _xplCommand(request, response) {
    var key = request.params[0];

    var query = request.query;
    var command = query.c || query.cmd || query.command;
    var device = query.device;
    var target = query.target || "*";
    var bodyName = query.bodyName || "delabarre.command";
    var current = query.v || query.current;

    if (!device && !command) {
      var reg=/(.*)\/([^/]+)$/.exec(key);
      if (reg) {
        device=reg[1];
        command=reg[2];
      }
    }

    debug("xplCommand", 
        "key=", key,
        "command=", command,
        "device=",device,
        "target=",target,
        "bodyName=",bodyName,
        "current=",current);
    
    if (!device || !command) {
      response.status(404).send("Device or command not found");
      return;
    }
    
    var source="http."+request.connection.remoteAddress;
    
    var params={
        command: command,
        device: device
    };
    if (current!==undefined) {
      params.current=current;
    }
    
    this.xpl.sendXplCmnd(params, bodyName, target, source, (error) => {
      
      if (error) {
        console.error("Command is not sent",error);
        response.status(500).send("Internal error");
        return;
      }
      
      console.log("Command sent !");
      response.status(200).send("Command sent");
    });    
  }

  _set(name, request, response, func) {
    var keys = Object.keys(request.body);
    debug("_set", name, "set keys=", keys);

    var options = formatOptions(request);

    var results = {};
    var lastDate=null;

    async.eachLimit(keys, 8, (key, callback) => {
      var reg=/(.*)\/([^/]+)$/.exec(key);
      if (reg) {
        key=reg[1]+"@"+reg[2]; // Transform @ to /
      }

      func.call(this.store, key, options, (error, value) => {
        debug("_set", name, " set key=", key, "value=", value, "error=", error);
        if (error) {
          return callback(error);
        }

        var reg2=/(.*)\@([^@]+)$/.exec(key);
        if (reg2) {
          key=reg2[1]+"/"+reg[2]; // Transform / to @
        }

        results[key] = value;

        if (value.date) {
          if (!lastDate || lastDate<value.date) {
            lastDate=value.date;
          }
        }

        callback();
      });

    }, (error) => {
      if (error) {
        // send 500
        if (error.code === 'NOT_FOUND') {
          response.status(404).send("Key '" + error.key + "' not found");
          return;
        }

        response.status(500).send(String(error));
        return;
      }

      if (lastDate) {
        if (!response.headersSent) {
          var since=request.headers['if-modified-since'];
          console.log("Since=",since,"/",lastDate);

          if (since && (new Date(since)).getTime()===lastDate.getTime()) {
            response.status(304).send("Not modified");
            return;
          }
        }

        response.setHeader("Last-Modified", lastDate.toUTCString());
      }

      response.json(results);
    });
  }

  _get(name, request, response, func) {
    var key = request.params[0];
    debug("_get", "type=", name, "key=", key);

    var reg=/(.*)\/([^/]+)$/.exec(key);
    if (reg) {
      key=reg[1]+"@"+reg[2]; // Transform @ to /
    }

    var options = formatOptions(request);

    func.call(this.store, key, options, (error, values) => {
      debug("_get", "type=", name, "key=", key, "values=", values);
      if (error) {
        // send 500
        if (error.code === 'NOT_FOUND') {
          response.status(404).send('Key not found');
          return;
        }

        response.status(500).send(String(error));
        return;
      }

      response.json(values);
    });
  }

  _getLast(request, response) {
    this._get("getLast", request, response, this.store.getLast);
  }

  _getHistory(request, response) {
    this._get("getHistory", request, response, this.store.getHistory);
  }

  _getMinMaxAvgSum(request, response) {
    this._get("getMinMaxAvgSum", request, response, this.store.getMinMaxAvgSum);
  }

  _postLast(request, response) {
    this._set("getLastSet", request, response, this.store.getLast);
  }

  _postHistory(request, response) {
    this._set("getHistorySet", request, response, this.store.getHistory);
  }

  _postMinMaxAvgSum(request, response) {
    this._set("getMinMaxAvgSumSet", request, response, this.store.getMinMaxAvgSum);
  }
}


function formatOptions(request) {
  var ret = {};

  var query = request.query;
  if (query.limit) {
    ret.limit = parseInt(query.limit, 10);
  }

  if (query.minDate) {
    ret.minDate = new Date(query.minDate);
  }

  if (query.maxDate) {
    ret.maxDate = new Date(query.maxDate);
  }

  if (query.averageMs) {
    ret.averageMs = parseInt(query.averageMs, 10);
  }

  if (query.step) {
    ret.stepMs = parseInt(query.step, 10) * 1000;
  }

  return ret;
}

module.exports = Server;
