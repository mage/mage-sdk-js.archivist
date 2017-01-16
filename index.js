var EventEmitter = require('events.js');
var mage = require('mage-sdk-js');
var inherits = require('inherits');
var rumplestiltskin = require('rumplestiltskin').trueName;
var Tome = require('tomes').Tome;

function createTrueName(index, topic) {
	return rumplestiltskin(index || {}, topic);
}

function Archivist() {
	EventEmitter.call(this);
}

inherits(Archivist, EventEmitter);

module.exports = exports = new Archivist();

var loaded = {};   // our cache
var changes = {};  // changes queued to be executed on the server

var distributing = false;  // flag to indicate if we're currently distributing

var operations = {
	queue: {},
	exec: {}
};


function getFromCache(trueName, maxAge) {
	var value = loaded[trueName];

	return value && (typeof maxAge !== 'number' || value._writtenAt > Date.now() - maxAge * 1000) ?
		value :
		undefined;
}


function Buffer(base64data) {
	this.data = base64data;
}

Buffer.prototype.toString = function () {
	return this.data;
};


function forEach(target, fn) {
	if (Array.isArray(target)) {
		for (var i = 0; i < target.length; i++) {
			fn(i, target[i]);
		}
	} else {
		for (var key in target) {
			if (target.hasOwnProperty(key)) {
				fn(key, target[key]);
			}
		}
	}
}

var regMediaTypes = {
	'*': {
		writer: function (value, mediaType, data) {
			value.mediaType = mediaType;
			value.data = data;
		}
	},
	'application/json': {
		detector: function () {
			return 0.5;
		},
		encoders: {
			'utf8-live': JSON.parse,
			'live-utf8': function (data) {
				return JSON.stringify(data, null, '\t');
			}
		}
	},
	'text/plain': {
		detector: function (data) {
			return (typeof data === 'string') ? 0.2 : 0;
		},
		encoders: {
			'utf8-live': function (data) {
				return data;
			},
			'live-utf8': function (data) {
				return data;
			}
		}
	},
	'application/octet-stream': {
		encoders: {
			'base64-live': function (data) {
				return new Buffer(data);
			},
			'live-base64': function (buffer) {
				return buffer.toString();
			}
		}
	}
};

var detectorList = [];
for (var mediaType in regMediaTypes) {
	if (regMediaTypes[mediaType].detector) {
		detectorList.push(mediaType);
	}
}


function guessMediaType(data) {
	var lastCertainty = 0;
	var result;

	for (var i = 0, len = detectorList.length; i < len; i++) {
		var mediaType = detectorList[i];
		var detector = regMediaTypes[mediaType].detector;

		var certainty = detector(data);
		if (certainty >= 1) {
			// 100% certain, instantly return
			return mediaType;
		}

		if (certainty > lastCertainty) {
			lastCertainty = certainty;
			result = mediaType;
		}
	}

	return result;
}


function encode(data, mediaType, fromEncoding, toEncodings) {
	if (!Array.isArray(toEncodings)) {
		toEncodings = [toEncodings];
	}

	if (toEncodings.indexOf(fromEncoding) !== -1) {
		return { data: data, encoding: fromEncoding };
	}

	var spec = regMediaTypes[mediaType];
	var glob = regMediaTypes['*'];
	var encoders = (spec && spec.encoders) || glob.encoders || {};

	for (var i = 0; i < toEncodings.length; i++) {
		var toEncoding = toEncodings[i];

		var encoder = encoders[fromEncoding + '-' + toEncoding];
		if (encoder) {
			data = encoder(data);

			return { data: data, encoding: toEncoding };
		}
	}

	throw new Error('No encoder found from ' + fromEncoding + ' to ' + toEncodings);
}


var loading = {};

function createGetCallback(callback, options) {
	return function (error, value) {
		if (!callback) {
			return;
		}

		if (error) {
			return callback(error);
		}

		// apply options

		if (!options.optional && (!value || value.data === undefined)) {
			return callback(new Error('Required value does not exist'));
		}

		// return the value

		return callback(null, value);
	};
}

function runGetCallbacks(trueNames, error) {
	for (var i = 0; i < trueNames.length; i++) {
		var trueName = trueNames[i];

		var callbacks = loading[trueName];
		delete loading[trueName];

		if (!callbacks) {
			continue;
		}

		for (var j = 0; j < callbacks.length; j++) {
			var callback = callbacks[j];

			if (error) {
				callback(error);
			} else {
				callback(null, loaded[trueName]);
			}
		}
	}
}


function VaultValue(topic, index) {
	this.mediaType = undefined;
	this.data = undefined;
	this.expirationTime = undefined;
	this.topic = topic;
	this.index = index || {};
	this._timer = undefined;
	this._writtenAt = undefined;
}


VaultValue.prototype.touch = function (expirationTimeOnServer) {
	clearTimeout(this._timer);
	this._timer = undefined;

	if (!expirationTimeOnServer) {
		this.expirationTime = undefined;
		return;
	}

	this.expirationTime = mage.time ?
		mage.time.serverTimeToClientTime(expirationTimeOnServer) :
		expirationTimeOnServer;

	var ttl = this.expirationTime * 1000 - Date.now();

	var that = this;

	this._timer = setTimeout(function expire() {
		that.del();
	}, ttl);
};


VaultValue.prototype.del = function () {
	this._writtenAt = Date.now();

	if (this.data === undefined) {
		return;
	}

	this.mediaType = undefined;
	this.data = undefined;
	this.encoding = undefined;

	// clear out the expiration time
	this.touch();
};


VaultValue.prototype.setData = function (mediaType, data, encoding) {
	if (!encoding) {
		encoding = 'live';
	}

	if (encoding === 'live' && !mediaType) {
		mediaType = guessMediaType(data);
	}

	var spec = regMediaTypes[mediaType];
	var glob = regMediaTypes['*'];
	var encoders = (spec && spec.encoders) || glob.encoders || {};
	var writer = (spec && spec.writer) || glob.writer;

	if (!writer) {
		throw new Error('No writer for mediaType ' + mediaType);
	}

	if (encoding !== 'live' && encoders) {
		var encoder = encoders[encoding + '-live'];
		if (!encoder) {
			throw new Error('Cannot convert encoding "' + encoding + '" to "live"');
		}

		data = encoder(data);
	}

	writer(this, mediaType, data);

	this._writtenAt = Date.now();
};


VaultValue.prototype.applyDiff = function (diff) {
	if (!diff || this.data === undefined) {
		return;
	}

	var api = regMediaTypes[this.mediaType];

	if (api && api.diff && api.diff.set) {
		api.diff.set(this.data, diff);
	} else {
		console.warn(
			'Received a diff for topic "' + this.topic + '" ' +
			'which does not support diffs (media type "' + this.mediaType + '")'
		);
	}

	this._writtenAt = Date.now();
};


/* Cache and mutation logic
 * --------------------------------
 * We keep a cache of what we know to be true through:
 * - received events
 * - previously distributed mutations
 * - not yet distributed mutations
 *
 * Because our not yet distributed mutations also affect the cache:
 * - We ignore incoming events when caused by our own actions (we're already uptodate).
 * - We should distribute fast, in order to avoid race conditions with other users.
 */

function getChange(topic, index, replace) {
	var trueName = createTrueName(index, topic);

	if (replace || !changes[trueName]) {
		changes[trueName] = {
			topic: topic,
			index: index || {}
		};
	}

	return changes[trueName];
}


operations.exec.set = function (topic, index, data, mediaType, encoding, expirationTime) {
	var trueName = createTrueName(index, topic);
	var value = loaded[trueName];

	if (!value) {
		value = new VaultValue(topic, index);

		loaded[trueName] = value;
	}

	value.setData(mediaType, data, encoding);
	value.touch(expirationTime);

	exports.emit(topic, 'set', value);
};


operations.queue.set = function (topic, index, data, mediaType, encoding, expirationTime) {
	if (!encoding || encoding === 'live') {
		mediaType = mediaType || guessMediaType(data);

		// turn live into a string, for transportation

		var result = encode(data, mediaType, 'live', ['utf8', 'base64']);

		encoding = result.encoding;
		data = result.data;
	}

	if (!mediaType) {
		throw new Error('Could not detect mediaType.');
	}

	var change = getChange(topic, index, true);

	change.operation = 'set';
	change.data = data;
	change.mediaType = mediaType;
	change.encoding = encoding;
	change.expirationTime = expirationTime;
};


operations.exec.add = function (topic, index, data, mediaType, encoding, expirationTime) {
	var trueName = createTrueName(index, topic);
	var value = loaded[trueName];

	if (value && value.data !== undefined) {
		console.warn('Could not add value (already exists):', topic, index);
		return;
	}

	value = new VaultValue(topic, index);

	loaded[trueName] = value;

	value.setData(mediaType, data, encoding);
	value.touch(expirationTime);

	exports.emit(topic, 'add', value);
};


operations.queue.add = function (topic, index, data, mediaType, encoding, expirationTime) {
	if (!encoding || encoding === 'live') {
		mediaType = mediaType || guessMediaType(data);

		// turn live into a string, for transportation

		var result = encode(data, mediaType, 'live', ['utf8', 'base64']);

		encoding = result.encoding;
		data = result.data;
	}

	if (!mediaType) {
		throw new Error('Could not detect mediaType.');
	}

	var change = getChange(topic, index);

	if (change.operation) {
		throw new Error('Value already has changes queued, so cannot add.');
	}

	change.operation = 'add';
	change.data = data;
	change.mediaType = mediaType;
	change.encoding = encoding;
	change.expirationTime = expirationTime;
};


operations.exec.applyDiff = function (topic, index, diff, expirationTime) {
	var trueName = createTrueName(index, topic);
	var value = loaded[trueName];

	if (!value) {
		return console.log('Got a diff for a non-existent value:', topic, index, diff);
	}

	value.applyDiff(diff);
	value.touch(expirationTime);

	exports.emit(topic, 'applyDiff', value);
};


operations.queue.applyDiff = function (topic, index, data) {
	var change = getChange(topic, index);

	// all other mutating changes take precendence

	if (change.hasOwnProperty('data') || change.operation === 'del') {
		return;
	}

	// if the change is already diffing, there's no need to do it again

	if (change.diff) {
		return;
	}

	// get an API for the data (which is like a tome)

	var trueName = createTrueName(index, topic);
	var value = loaded[trueName];

	if (!value) {
		// nothing can be diffed if we don't know the value
		return;
	}

	var api = regMediaTypes[value.mediaType];

	if (!api || !api.diff || !api.diff.get) {
		throw new Error('Cannot read diffs from mediaType: ' + value.mediaType);
	}

	// register a toJSON function so we serialize the diff on demand

	change.operation = 'set';

	change.diff = {
		toJSON: function () {
			return api.diff.get(data);
		}
	};
};


operations.exec.touch = function (topic, index, expirationTime) {
	var trueName = createTrueName(index, topic);
	var value = loaded[trueName];

	if (value) {
		value.touch(expirationTime);

		exports.emit(topic, 'touch', value);
	}
};


operations.queue.touch = function (topic, index, expirationTime) {
	var change = getChange(topic, index);

	if (change.operation === 'del') {
		return;
	}

	if (!change.operation) {
		change.operation = 'touch';
	}

	change.expirationTime = expirationTime;
};


operations.exec.del = function (topic, index) {
	var trueName = createTrueName(index, topic);
	var value = loaded[trueName];

	if (value) {
		// not removing the value from loaded[] allows us to *know* it's no longer there

		value.del();

		exports.emit(topic, 'del', value);
	}
};


operations.queue.del = function (topic, index) {
	var change = getChange(topic, index, true);

	change.operation = 'del';
};


mage.eventManager.on('archivist:set', function (path, info) {
	if (!distributing) {
		var key = info.key;
		var rawValue = info.value;

		operations.exec.set(
			key.topic,
			key.index,
			rawValue.data,
			rawValue.mediaType,
			rawValue.encoding,
			info.expirationTime
		);
	}
});


mage.eventManager.on('archivist:applyDiff', function (path, info) {
	if (!distributing) {
		var key = info.key;

		operations.exec.applyDiff(key.topic, key.index, info.diff, info.expirationTime);
	}
});


mage.eventManager.on('archivist:touch', function (path, info) {
	if (!distributing) {
		var key = info.key;

		operations.exec.touch(key.topic, key.index, info.expirationTime);
	}
});


mage.eventManager.on('archivist:del', function (path, info) {
	if (!distributing) {
		var key = info.key;

		operations.exec.del(key.topic, key.index);
	}
});


exports.getCache = function () {
	return loaded;
};


exports.clearCache = function () {
	loaded = {};
};


/**
 * Distributes all changes made/queued to the server
 *
 * @param {Function} cb  Receives an error, or an array of issues that were encountered
 */

exports.distribute = function (cb) {
	cb = cb || function () {};

	var trueNames = Object.keys(changes);

	if (trueNames.length === 0) {
		return cb(null, []);
	}

	distributing = true;

	var distribution = [];

	for (var i = 0; i < trueNames.length; i++) {
		distribution.push(changes[trueNames[i]]);
	}

	changes = {};

	exports.rawDistribute(distribution, function (error, issues) {
		distributing = false;

		cb(error, issues);
	});
};


exports.add = function (topic, index, data, mediaType, encoding, expirationTime) {
	operations.exec.add(topic, index, data, mediaType, encoding, expirationTime);
	operations.queue.add(topic, index, data, mediaType, encoding, expirationTime);
};


exports.set = function (topic, index, data, mediaType, encoding, expirationTime) {
	operations.exec.set(topic, index, data, mediaType, encoding, expirationTime);
	operations.queue.set(topic, index, data, mediaType, encoding, expirationTime);
};


exports.touch = function (topic, index, expirationTime) {
	operations.exec.touch(topic, index, expirationTime);
	operations.queue.touch(topic, index, expirationTime);
};


exports.del = function (topic, index) {
	operations.exec.del(topic, index);
	operations.queue.del(topic, index);
};

function setOrDelete(info) {
	var key = info.key;
	var rawValue = info.value;

	if (!rawValue) {
		return operations.exec.del(key.topic, key.index);
	}

	operations.exec.set(
		key.topic,
		key.index,
		rawValue.data,
		rawValue.mediaType,
		rawValue.encoding,
		info.expirationTime
	);
}


exports.exists = function (topic, index, options, cb) {
	if (typeof options === 'function') {
		cb = options;
		options = {};
	} else if (!options) {
		options = {};
	}

	var trueName = createTrueName(index, topic);

	var value = getFromCache(trueName, options.maxAge);
	if (value) {
		// we already know if the value exists or not

		return setTimeout(function () {
			cb(null, value.data !== undefined);
		}, 0);
	}

	exports.rawExists(topic, index, cb);
};


exports.getValue = function (topic, index, options, cb) {
	if (typeof options === 'function') {
		cb = options;
		options = {};
	} else if (!options) {
		options = {};
	}

	var trueName = createTrueName(index, topic);

	var value = getFromCache(trueName, options.maxAge);

	if (value) {
		// the value has already been cached
		return setTimeout(function () {
			cb(null, value);
		}, 0);
	}

	exports.rawGet(topic, index, options, function (error, info) {
		if (error) {
			return cb(error);
		}

		setOrDelete(info);

		return cb(null, loaded[trueName]);
	});
};


exports.get = function (topic, index, options, cb) {
	if (typeof options === 'function') {
		cb = options;
		options = {};
	} else if (!options) {
		options = {};
	}

	exports.getValue(topic, index, options, function (error, value) {
		if (error) {
			return cb(error);
		}

		if (value) {
			cb(null, value.data);
		} else {
			cb();
		}
	});
};


exports.mgetValues = function (queries, options, cb) {
	if (typeof options === 'function') {
		cb = options;
		options = {};
	} else if (!options) {
		options = {};
	}

	// flatten the object-notation for queries into an array

	var trueNames = [];
	var realQueries = [];
	var realResult = Array.isArray(queries) ? new Array(queries.length) : {};
	var masterError;

	forEach(queries, function (queryId, query) {
		var trueName = createTrueName(query.index, query.topic);

		var callback = createGetCallback(function (error, value) {
			if (error) {
				masterError = error;
			} else {
				realResult[queryId] = value;
			}
		}, options);

		var value = getFromCache(trueName, options.maxAge);

		if (value) {
			// the value has already been cached

			return callback(null, value);
		}

		// make sure we run our query

		trueNames.push(trueName);

		if (loading.hasOwnProperty(trueName)) {
			// a normal get-operation is already loading this value

			loading[trueName].push(callback);
		} else {
			// first time someone asks for this value, so load it

			loading[trueName] = [callback];
			realQueries.push(query);
		}
	});

	// if nothing is loading or needs to be loaded, do nothing

	if (trueNames.length === 0) {
		return setTimeout(function () {
			return cb(null, realResult);
		}, 0);
	}

	return exports.rawMGet(realQueries, options, function (error, results) {
		if (!error && results) {
			results.forEach(setOrDelete);
		}

		runGetCallbacks(trueNames, error); // the get-function should also use this for callbacks

		error = error || masterError;

		if (error) {
			cb(error);
		} else {
			cb(null, realResult);
		}
	});
};


exports.mget = function (queries, options, cb) {
	if (typeof options === 'function') {
		cb = options;
		options = {};
	} else if (!options) {
		options = {};
	}

	exports.mgetValues(queries, options, function (error, values) {
		if (error) {
			return cb(error);
		}

		var realResult = Array.isArray(values) ? new Array(values.length) : {};

		forEach(values, function (queryId, value) {
			realResult[queryId] = value ? value.data : undefined;
		});

		cb(null, realResult);
	});
};


exports.list = function (topic, partialIndex, options, cb) {
	if (typeof options === 'function') {
		cb = options;
		options = {};
	} else if (!options) {
		options = {};
	}

	exports.rawList(topic, partialIndex, options, cb);
};

