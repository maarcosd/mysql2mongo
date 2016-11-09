var util = require('util'),
    assert = require('assert'),
    mongodb = require('mongodb');

module.exports = Mongo;

function Mongo(app) {
    this.db = null;
    this.config = app.config;
    this.mongo = mongodb;
}

Mongo.prototype.connect = function() {
    var callback, database;

    switch (arguments.length) {
        case 1:
            callback = arguments[0];
            break;
        case 2:
            database = arguments[0];
            callback = arguments[1];
            break;
    }

    var self = this,
        mongoConfig = this.config.mongodb,
        host = mongoConfig.host + ':' + (mongoConfig.port || 27017);

    database = database || mongoConfig.db;

    // util.log('Connecting to MongoDB...');

    this.mongo.MongoClient.connect('mongodb://' + host + '/' + database, function(err, db) {
        assert.equal(null, err);

        self.db = db;

        // util.log('Connected to Mongo at ' + host + ' using ' + database);

        callback();
    });

    return this;
};

Mongo.prototype.close = function() {
    this.db.close();

    return this;
};

Mongo.prototype.insert = function(collectionName, objects, callback) {
    var collection = this.db.collection(collectionName);

    objects = objects.constructor !== Array ? [objects] : objects;

    collection.insertMany(objects, function(err, result) {
        assert.equal(err, null);

        callback(result);
    });

    return this;
};

Mongo.prototype.remove = function(collectionName, params, callback) {
    var collection = this.db.collection(collectionName);

    callback = callback === undefined && typeof params == 'function' ? params : callback;

    collection.removeMany(params, function(err, removedCount) {
        assert.equal(err, null);

        callback(removedCount);
    });

    return this;
};

Mongo.prototype.find = function(collectionName, filters, callback) {
    var collection = this.db.collection(collectionName);

    collection.find(filters).toArray(function(err, docs) {
        assert.equal(err, null);

        callback(docs);
    });
};

Mongo.prototype.update = function(collectionName, filters, set, callback) {
    var collection = this.db.collection(collectionName);

    collection.updateMany(filters, { $set : set }, function(err, result) {
        assert.equal(err, null);

        callback(result);
    });
};
