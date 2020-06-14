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

    getMinMaxAvgSum(deviceKey, options = {}, callback) {
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

        const projection = options.projection || {
            startDate: true,
            endDate: true,
            min: true,
            max: true,
            sum: true,
            average: true,
            totalMs: true,
            count: true,
            minDT: true,
            maxDT: true,
            units: true,
        };
        if (projection.maxValue) {
//            projection.max = true;
        }
        if (projection.minValue) {
//            projection.min = true;
        }


        let min;
        let minDT;
        let max;
        let maxDT;
        let pred;
        let totalMs = 0;
        let sum = 0;
        let sumDT = 0;
        let count = 0;
        options.hit = (list, pt) => {
            if (options.outputUnits && pt.units !== options.outputUnits) {
                console.error("UNIT is changing !!!!");
            }

            if ((projection.min || projection.minValue) && (!min || min.current > pt.current)) {
                min = pt;
            }
            if ((projection.max || projection.maxValue) && (!max || max.current < pt.current)) {
                max = pt;
            }
            if (pred) {
                const dt = pt.date.getTime() - pred.date.getTime();
                if (projection.average) {
                    totalMs += dt;
                }
                // debug("DT=", dt, "current", pred.current);
                const p = pred.current * dt;
                const p2 = pred.current / dt;
                if (projection.average) {
                    sumDT += p;
                }

                if (projection.minDT && (!minDT || minDT.current > p2)) {
                    minDT = {
                        current: p2,
                        date: pt.date
                    };
                }
                if (projection.maxDT && (!maxDT || maxDT.current < p2)) {
                    maxDT = {
                        current: p2,
                        date: pt.date
                    };
                }
            }
            pred = pt;
            if (projection.sum) {
                sum += pt.current;
            }
            count++;
        };
        options.hitEnd = () => {
            if (!pred) {
                return;
            }

            const lastT = (options.dateMax && options.dateMax.getTime()) || Date.now();

            const dt = lastT - pred.date.getTime();
            // debug("DT=", dt, "current", pred.current);
            if (projection.average) {
                totalMs += dt;
            }
            if (projection.sum) {
                sumDT += pred.current * dt;
            }
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

            const ret = {
                startDate: options.dateMin,
                endDate: options.dateMax,
            };

            if (projection.minValue && min) {
                ret.minValue = min.current;

            } else if (projection.min) {
                ret.min = min;
            }
            if (projection.minDT) {
                ret.minDT = minDT;
            }

            if (projection.maxValue && max) {
                ret.maxValue = max.current;

            } else if (projection.max) {
                ret.max = max;
            }
            if (projection.maxDT) {
                ret.maxDT = maxDT;
            }

            if (projection.average) {
                ret.average = sumDT / totalMs;
                ret.sumDT = sumDT;
            }
            if (projection.sum) {
                ret.sum = sum;
            }
            if (projection.count) {
                ret.count = count;
            }

            if (options.outputUnits && projection.units) {
                ret.units = options.outputUnits;
            }

            callback(null, ret);
        });
    }

    computeItemIndex(cache, targetDate, pt, options) {

        if (options.stepMs === 'day') {
            if (!cache.stepMs) {
                cache.stepMs = 3600 * 24;
                const d = new Date(options.dateMin.getTime());
                d.setHours(0, 0, 0, 0);
                const baseTime = d.getTime();

                cache.curDate = d;
                cache.curTime = baseTime;
                cache.ci = 0;

                let cachedIndex = -1;
                let cachedStartDate;
                let cachedEndDate;
                let cachedDate;
                cache.createItem = (i) => {
                    if (cachedIndex === i) {
                        return {
                            startDate: cachedStartDate,
                            endDate: cachedEndDate,
                            date: cachedDate,
                        }
                    }

                    const startDate = new Date(baseTime);
                    startDate.setDate(startDate.getDate() + i);

                    const endDate = new Date(startDate.getTime());
                    endDate.setDate(endDate.getDate() + 1);
                    endDate.setMilliseconds(-1);

                    cachedStartDate = startDate;
                    cachedEndDate = endDate;
                    cachedIndex = i;
                    cachedDate = startDate.getFullYear() + '-' + (startDate.getMonth() + 1) + '-' + startDate.getDate();

                    const item = {
                        startDate,
                        endDate,
                        date: cachedDate,
                    };

                    return item;
                };
            }

            const t = new Date(targetDate.getTime());
            t.setHours(0, 0, 0, 0);
            const ttime = t.getTime();
            while (ttime > cache.curTime) {
                cache.ci++;
                cache.curDate.setDate(cache.curDate.getDate() + 1);
                cache.curTime = cache.curDate.getTime();
            }

            return cache;
        }

        if (!cache.stepMs) {
            cache.stepMs = options.stepMs;
            cache.baseTime = options.dateMin.getTime();
            let cachedIndex = -1;
            let cachedStartDate;
            let cachedEndDate;
            cache.createItem = (i) => {
                if (cachedIndex === i) {
                    return {
                        startDate: cachedStartDate,
                        endDate: cachedEndDate,
                    }
                }

                const startDate = new Date(cache.baseTime + i * cache.stepMs);
                const endDate = new Date(cache.baseTime + (i + 1) * cache.stepMs - 1);

                cachedStartDate = startDate;
                cachedEndDate = endDate;
                cachedIndex = i;

                const item = {
                    startDate,
                    endDate,
                };

                return item;
            };
        }

        cache.ci = Math.floor((targetDate.getTime() - cache.baseTime) / cache.stepMs);

        return cache;
    }

    getMinMaxAvgSumForADay(deviceKey, min, max, callback) {
        callback(null, []);
    }

    saveMinMaxAvgSumForADay(deviceKey, date, result, callback) {
        callback();
    }

    processMinMaxAvgSumByDay(deviceKey, options = {}, projection, callback) {
        debug('processMinMaxAvgSumByDay', 'deviceKey=', deviceKey, 'options=', options, 'projection=', projection);

        const min = new Date(options.dateMin);
        min.setHours(0, 0, 0, 0);

        let max = options.dateMax;
        max.setHours(0, 0, 0, 0);

        const today = new Date();
        today.setHours(0, 0, 0, 0);

        if (max.getTime() > today.getTime()) {
            max = new Date(today);
        }

        let cacheMax = max;
        if (max.getTime() === today.getTime()) {
            cacheMax = new Date(today);
            cacheMax.setMilliseconds(cacheMax.getMilliseconds() - 1);
        }

        this.getMinMaxAvgSumForADay(deviceKey, min, cacheMax, (error, results) => {
            debug('processMinMaxAvgSumByDay', 'getMinMaxAvgSumForADay returns=', results, 'error=', error);

            if (error) {
                return callback(error);
            }

            let ret = [];
            let toFetch = [];

            let d = new Date(min);
            d.setHours(0, 0, 0, 0);

            for (; d.getTime() <= max.getTime();) {

                let top = results[0];
                if (top && top.startDate.getTime() === d.getTime()) {
                    debug('processMinMaxAvgSumByDay', 'process d=', d.getTime(), 'max=', max.getTime(), 'found=', top);

                    results.shift();
                    ret.push(top);

                    d = new Date(d);
                    d.setDate(d.getDate() + 1);
                    continue;
                }

                debug('processMinMaxAvgSumByDay', 'process d=', d.getTime(), 'max=', max.getTime(), 'NOT FOUND, request one !');

                toFetch.push(ret.length);
                ret.push(d);

                d = new Date(d);
                d.setDate(d.getDate() + 1);
                d.setHours(0, 0, 0, 0);
            }

            async.eachOfLimit(toFetch, 4, (index, arrayIndex, callback) => {
                const date = ret[index];

                const maxDate = new Date(date);
                maxDate.setDate(maxDate.getDate() + 1);
                maxDate.setMilliseconds(-1);

                this.getMinMaxAvgSumByStep(deviceKey, {
                    dateMin: date,
                    dateMax: maxDate,
                    stepMs: maxDate.getTime() - date.getTime() + 1,

                }, (error, dateResult) => {
                    debug('processMinMaxAvgSumByDay', 'getMinMaxAvgSumByStep index=', index, 'minDate=', date, 'maxDate=', maxDate, 'dateResult=', dateResult);

                    if (error) {
                        return callback(error);
                    }

                    if (dateResult && dateResult.length > 1) {
                        console.error('Can not get more one element date=', date, dateResult);
                    }

                    ret[index] = (dateResult && dateResult[0]) || {startDate: date, endDate: maxDate};

                    if (today.getTime() === date.getTime()) {
                        // We do not record values for today
                        callback();
                        return;
                    }

                    this.saveMinMaxAvgSumForADay(deviceKey, date, ret[index], callback);
                });

            }, (error) => {
                debug('processMinMaxAvgSumByDay', 'endOfLimit error=', error, 'ret=', ret);

                if (error) {
                    return callback(error);
                }

                // Process projection
                if (!options.projection) {
                    callback(null, ret);
                    return;
                }

                const newRet = ret.map((prev) => {
                    const item = {};

                    if (projection.startDate) {
                        item.startDate = prev.startDate;
                    }
                    if (projection.endDate) {
                        item.endDate = prev.endDate;
                    }
                    if (projection.minValue && prev.min) {
                        item.minValue = prev.min.current;

                    } else if (projection.min) {
                        item.min = prev.min;
                    }
                    if (projection.maxValue && prev.max) {
                        item.maxValue = prev.max.current;

                    } else if (projection.max) {
                        item.max = prev.max;
                    }
                    if (projection.diffMinMax && prev.min && prev.max) {
                        item.diffMinMax = prev.max.current - prev.min.current;
                    }

                    if (projection.sum) {
                        item.sum = prev.sum;
                    }
                    if (projection.average) {
                        item.average = prev.average;
                    }
                    if (projection.totalMs) {
                        item.totalMs = prev.totalMs;
                    }
                    if (projection.count) {
                        item.count = prev.count;
                    }
                    if (projection.countChanges) {
                        item.countChanges = prev.countChanges;
                    }

                    if (projection.delta && prev.min && prev.max) {
                        item.delta = prev.max.current - prev.min.current;
                    }

                    return item;
                });

                callback(null, newRet);
            });
        });
    }

    getMinMaxAvgSumByStep(deviceKey, options = {}, callback) {
        const projection = options.projection || {
            startDate: true,
            endDate: true,
            min: true,
            max: true,
            sum: true,
            average: true,
            totalMs: true,
            count: true,
            countChanges: true,
            units: true,
        };
        if (projection.maxValue) {
//            projection.max = true;
        }
        if (projection.minValue) {
//            projection.min = true;
        }

        if (!options.limit) {
            const d = options.dateMax || (new Date());
            options.dateMax = d;

            if (!options.dateMin) {
                const d2 = new Date(d.getTime());
                d2.setDate(d2.getDate() - 1);

                options.dateMin = d2;
            }
        }

        options.descending = false; // Force ascending

        if (options.stepMs === 'day' && this.getMinMaxAvgSumForADay) {
            return this.processMinMaxAvgSumByDay(deviceKey, options, projection, callback);
        }

        if (!options.stepMs) {
            options.stepMs = (options.dateMax.getTime() - options.dateMin.getTime()) / 10;
        }

        let currentIndex = -1;
        let predPt;

        const computeIndexCache = {};

        const stepTo = (targetDate, pt, list) => {

            const {stepMs, ci, createItem} = this.computeItemIndex(computeIndexCache, targetDate, pt, options);

//            console.log('Compute index=', targetDate, '=>', ci);

            for (let i = Math.max(0, currentIndex); i <= ci; i++) {
                if (list[i]) {
                    continue;
                }
                const item = createItem(i, ci);
                if (projection.sum) {
                    item.sum = 0;
                }
                if (projection.average) {
                    item.average = 0;
                }
                if (projection.totalMs) {
                    item.totalMs = 0;
                }
                if (projection.count) {
                    item.count = 0;
                }
                if (projection.countChanges) {
                    item.countChanges = 0;
                }

                list[i] = item;
            }

            let cell = list[ci];
            if (pt) {
                if (projection.min && (cell.min === undefined || cell.min.current > pt.current)) {
                    cell.min = pt;
                }
                if (projection.max && (cell.max === undefined || cell.max.current < pt.current)) {
                    cell.max = pt;
                }
            }

            if (!predPt) {
                predPt = pt;
                currentIndex = ci;
                return;
            }

            const dt = targetDate - predPt.date.getTime();

            if (ci === currentIndex) {
                if (projection.sum) {
                    cell.sum += predPt.current;
                }
                if (projection.totalMs) {
                    cell.totalMs += dt;
                }
                if (projection.average) {
                    cell.average += predPt.current * dt;
                }
                if (projection.count) {
                    if (pt) {
                        cell.count++;
                    }
                }
                if (projection.countChanges) {
                    if (pt && pt.current !== predPt.current) {
                        cell.countChanges++;
                    }
                }

                predPt = pt;
                return;
            }

            if (!projection.sum && !projection.totalMs && !projection.average) {
                if (projection.count) {
                    cell.count++;
                }
                if (projection.countChanges) {
                    if (pt && predPt && pt.current !== predPt.current) {
                        cell.countChanges++;
                    }
                }
                predPt = pt;
                return;
            }

            // start
            cell = list[currentIndex];
            if (!cell) {
                console.error('CurrentIndex=', currentIndex, 'ci=', ci, 'length=', list.length);
                return;
            }
            debug('getMinMaxAvgSumByStep', 'Index=', currentIndex, 'cell=', cell, 'predPt=', predPt);

            let ts = 0;
            const dtBegin = cell.endDate.getTime() - predPt.date.getTime();
            const curBegin = predPt.current * (dtBegin / dt);
            if (projection.sum) {
                cell.sum += curBegin;
            }
            if (projection.totalMs) {
                cell.totalMs += dtBegin;
            }
            ts += dtBegin;
            if (projection.average) {
                cell.average += curBegin * dtBegin;
            }

            // mid
            for (currentIndex++; currentIndex < ci; currentIndex++) {
                const curMed = predPt.current * (stepMs / dt);
                cell = list[currentIndex];
                ts += stepMs;
                if (projection.sum) {
                    cell.sum += curMed;
                }
                if (projection.totalMs) {
                    cell.totalMs += stepMs;
                }
                if (projection.average) {
                    cell.average += curMed * stepMs;
                }
            }

            // end
            cell = list[currentIndex];
            const dtEnd = targetDate.getTime() - cell.startDate.getTime();
            const curEnd = predPt.current * (dtEnd / dt);
            ts += dtEnd;
            if (projection.sum) {
                cell.sum += curEnd;
            }
            if (projection.totalMs) {
                cell.totalMs += dtEnd;
            }
            if (projection.average) {
                cell.average += curEnd * dtEnd;
            }
            if (projection.count) {
                cell.count++;
            }
            if (projection.countChanges) {
                if (pt && pt.current !== predPt.current) {
                    cell.countChanges++;
                }
            }

            //console.log(ts + "/" + dt);

            predPt = pt;
        };

        options.hit = (list, pt) => {
            if (options.outputUnits && pt.units !== options.outputUnits) {
                console.error("UNIT is changing !!!!");
            }

            stepTo(pt.date, pt, list);
        };
        options.hitEnd = (list) => {
            if (!predPt) {
                return;
            }

            const lastT = options.dateMax || (new Date());

            stepTo(lastT, null, list);
        };

        this.getHistory(deviceKey, options, (error, results) => {
            debug('getMinMaxAvgSumByStep', 'deviceKey=', deviceKey, 'options=', options, 'results=', (results && results.length), 'error=', error);

            if (error) {
                return callback(error);
            }

            if (!results || !results.length) {
                return callback();
            }

            if (options.outputUnits && projection.units) {
                results.units = options.outputUnits;
            }

            if (projection.average) {
                results.forEach((r) => r.average /= r.totalMs);
            }

            if (!projection.startDate && !projection.endDate) {
                results.forEach((r) => {
                    delete r.startDate;
                    delete r.endDate;
                });
            } else if (!projection.startDate) {
                results.forEach((r) => {
                    delete r.startDate;
                });

            } else if (!projection.endDate) {
                results.forEach((r) => {
                    delete r.endDate;
                });
            }
            if (projection.minValue) {
                results.forEach((r) => {
                    if (!r.min) {
                        return;
                    }
                    r.minValue = r.min.current;
                    delete r.min;
                });
            }
            if (projection.maxValue) {
                results.forEach((r) => {
                    if (!r.max) {
                        return;
                    }
                    r.maxValue = r.max.current;
                    delete r.max;
                });
            }
            if (projection.diffMinMax && prev.min && prev.max) {
                results.forEach((r) => {
                    if (!r.max || !r.min) {
                        return;
                    }
                    r.diffMinMax = r.max.current - r.min.current;
                });
            }


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

        let pred = undefined;
        let cumul = 0;
        let count = 0;
        let countChanges = 0;

        options.hit = (list, pt) => {
            if (options.outputUnits && pt.units !== options.outputUnits) {
                console.error("UNIT is changing !!!!");
            }

            if (pred === undefined) {
                pred = pt.current;
                count++;
                countChanges++;
                return;
            }

            if (pt.current < pred) {
                cumul += pt.current - pred;
                countChanges++;
            }

            count++;

            pred = pt.current;
        };

        this.getHistory(deviceKey, options, (error, results) => {
            if (error) {
                return callback(error);
            }

            const r = {current: cumul, count, countChanges};

            if (options.outputUnits) {
                r.units = options.outputUnits;
            }

            r.startDate = options.dateMin;
            r.endDate = options.dateMax;
            r.deviceKey = deviceKey;

            callback(null, r);
        });
    }
}

module.exports = Db;
