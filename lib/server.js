/*jslint node: true, vars: true, nomen: true */
'use strict';

var express = require('express');
var Http = require('http');
var debug = require('debug')('xpl-db:server');
var async = require('async');
var noCache = require('connect-nocache')();
var bodyParser = require('body-parser');
var serve_static = require('serve-static');
var compress = require('compression');

class Server {
  constructor(configuration, store) {
    this.configuration = configuration || {};
    this.store = store;

    this.app = this.configuration.express || express();

    if (this.configuration.compression !== false) {
      try {
        var compression = require('compression');
        this.app.use(compression())
      } catch (x) {
        console.error("No compression module !");
      }
    }
  }

  listen(callback) {
    var app = this.app;

    app.enable('etag');
    app.use(compress());  
    app.use(bodyParser.json());
    app.use(bodyParser.urlencoded({
      extended: true
    }));
    app.use(noCache);

    app.get(/^\/last\/(.*)$/, this.getLast.bind(this));
    app.get(/^\/history\/(.*)$/, this.getHistory.bind(this));
    app.get(/^\/minMaxAvgSum\/(.*)$/, this.getMinMaxAvgSum.bind(this));

    app.post('/last', this.getLastSet.bind(this));
    app.post('/history', this.getHistorySet.bind(this));
    app.post('/minMaxAvgSum', this.getMinMaxAvgSumSet.bind(this));
    
    if (this.configuration.configPath) {
      app.use("/config", serve_static(this.configuration.configPath, {
        'index' : false
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
  };

  _set(name, request, response, func) {
    var keys = Object.keys(request.body);
    debug("_set", name, "set keys=", keys);

    var options = formatOptions(request);

    var results = {};

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

        callback();
      });

    }, (error) => {
      if (error) {
        // send 500
        if (error.code === 'NOT_FOUND') {
          response.status(404).body("Key '" + error.key + "' not found");
          return;
        }

        response.status(500).body(String(error));
        return;
      }

      response.json(results);
    });
  };

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

  getLast(request, response) {
    this._get("getLast", request, response, this.store.getLast);
  }

  getHistory(request, response) {
    this._get("getHistory", request, response, this.store.getHistory);
  }

  getMinMaxAvgSum(request, response) {
    this._get("getMinMaxAvgSum", request, response, this.store.getMinMaxAvgSum);
  }

  getLastSet(request, response) {
    this._set("getLastSet", request, response, this.store.getLast);
  }

  getHistorySet(request, response) {
    this._set("getHistorySet", request, response, this.store.getHistory);
  }

  getMinMaxAvgSumSet(request, response) {
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
