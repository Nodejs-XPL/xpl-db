/*jslint node: true, vars: true, nomen: true, esversion: 6 */
'use strict';

const debug = require('debug')('xpl-db:db');
const async = require('async');

class Db {
	constructor(configuration, deviceAliases) {
		this.configuration = configuration || {};
		this.deviceAliases = deviceAliases;

		this._lastCache = {};
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
		var totalMs = 0;
		var sum = 0;
		var sumDT = 0;
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
				var p = pred.current * dt;
				var p2 = pred.current / dt;
				sumDT += p;

				if (!minDT || minDT.current > p2) {
					minDT = {
						current: p2,
						date: pt.date
					};
				}
				if (!maxDT || maxDT.current < p2) {
					maxDT = {
						current: p2,
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

			var lastT = (options.dateMax && options.dateMax.getTime()) || Date.now();

			var dt = lastT - pred.date.getTime();
			// debug("DT=", dt, "current", pred.current);
			totalMs += dt;
			sumDT += pred.current * dt;
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
				min: min,
				minDT: minDT,
				max: max,
				maxDT: maxDT,
				average: sumDT / totalMs,
				sum: sum,
				sumDT: sumDT,
				startDate: options.dateMin,
				endDate: options.dateMax
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
					startDate: new Date(minTime + i * stepMs),
					endDate: new Date(minTime + (i + 1) * stepMs - 1),
					min: undefined,
					max: undefined,
					sum: 0,
					average: 0,
					totalMs: 0
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

			var lastT = options.dateMax || (new Date());

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

		options.descending = false; // Force ascending

		var pred = undefined;
		var cumul = 0;
		var count = 0;

		options.hit = (list, pt) => {
			if (options.outputUnits && pt.units !== options.outputUnits) {
				console.error("UNIT is changing !!!!");
			}

			if (pred === undefined || pt.current < pred) {
				pred = pt.current;
				count++;
				return;
			}
			cumul += pt.current - pred;
			count++;

			pred = pt.current;
		};

		this.getHistory(deviceKey, options, (error, results) => {
			if (error) {
				return callback(error);
			}

			var r = {current: cumul, count};

			if (options.outputUnits) {
				r.units = options.outputUnits;
			}

			r.startDate = options.dateMin;
			r.endDate = options.dateMax;

			callback(null, r);
		});
	}
}

module.exports = Db;
