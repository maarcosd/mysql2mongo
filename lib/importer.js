var util = require('util'),
    _ = require('underscore'),
    assert = require('assert'),
    Mongo = require('./mongo.js');

module.exports = Importer;

function Importer(app) {
    this.config = app.config;
    this.mongo = app.mongo;
    this.mysql = app.mysql;
    this.app = app;
    this.globals = {};
}

Importer.prototype.start = function () {
    util.log('Starting tailer...');

    this.tail();
};

Importer.prototype.tail = function () {
    var self = this;

    this.preprocess();
};

Importer.prototype.preprocess = function() {
    var self = this;

    if (this.config.preprocess !== undefined) {
        this.config.preprocess(this, function(globals) {
            self.globals = globals;

            self.processCollections();
        });
    }
    else {
        self.processCollections();
    }
};

Importer.prototype.processCollections = function() {
    var self = this,
        config = self.config;

    var order = config.collectionOrder || _.keys(config.collections);

    util.log('Started prcessing collection queue (' + order.length + ' total)');

    var i = 0;

    function loopCollections() {
        var table = order[i];

        if (i < order.length) {
            util.log('Processing collection ' + table + ' (' + (i+1) + ' of ' + order.length + ')');

            processCollection(table, self.config.collections[table], function () {
                i++;
                loopCollections();
            });
        }
        else {
            self.setRelations();
        }
    }

    if (order.length > 0) {
        loopCollections();
    }

    function processCollection(table, params, callback) {
        params.target = params.target || table;
        params.truncate = params.truncate === true;
        params.append = params.append === true;
        params.associations = params.associations || {};

        self.mysql.selectEverything(table, function(mysqlResult) {
            util.log('Found ' + mysqlResult.length + ' records in ' + table);

            var mongoDocuments = [];

            for (var i in mysqlResult) {
                var row = mysqlResult[i],
                    mongoDocument = {};

                for (var mysqlColumn in row) {
                    if (params.associations.hasOwnProperty(mysqlColumn)) {
                        var association = params.associations[mysqlColumn],
                            mysqlValue = row[mysqlColumn],
                            key, value;

                        switch (typeof association) {
                            case 'string':
                                key = association;
                                value = mysqlValue;
                                break;
                            case 'function':
                                var transformedValue = association(mysqlValue, self);

                                if (typeof transformedValue !== 'object') {
                                    transformedValue = {
                                        key : mysqlColumn,
                                        value : transformedValue
                                    };
                                }

                                key = transformedValue.key;
                                value = transformedValue.value;
                                break;
                            case 'object':
                                key = association.key || mysqlColumn;

                                if (association.allowEmptyString === false && mysqlValue === '') {
                                    mysqlValue = undefined;
                                }

                                value = (mysqlValue === null || mysqlValue === undefined) && association.default !== undefined ?
                                    association.default : mysqlValue;

                                break;
                        }

                        if (key !== undefined && value !== undefined) {
                            mongoDocument[key] = value;
                        }
                    }
                }

                // Process any fields that need to be resolved
                if (params.resolve !== undefined) {
                    for (var attribute in params.resolve) {
                        var attrResolve = params.resolve[attribute],
                            value = (typeof attrResolve == 'function') ? attrResolve(row, self) : attrResolve;

                        mongoDocument[attribute] = value;
                    }
                }

                if (_.size(mongoDocument) > 0) {
                    // For future reference it may be nice to know wether comes from MySQL
                    mongoDocument.__imported = true;

                    mongoDocuments.push(mongoDocument);
                }
            }

            self.mongo.connect(params.db || self.config.mongodb.db, function() {
                var insert = function() {
                    self.mongo.insert(params.target, mongoDocuments, function (result) {
                        util.log(result.length + ' records imported.');

                        callback();
                    });
                };

                // User may choose to only append new data to collection instead of truncating it first
                if (params.append === true) {
                    insert();
                } else {
                    self.mongo.remove(params.target, null, function() {
                        insert();
                    });
                }
            });
        });
    }
};

Importer.prototype.setRelations = function() {
    var self = this,
        config = self.config,
        collections = config.collections,
        i = 0;

    for (var tableName in collections) {
        var collection = collections[tableName],
            collectionName = collection.target || tableName;

        util.log('Setting relations on collection ' + collectionName + ' (' + (i+1) + ' of ' + _.keys(collections).length + ')');

        if (collection.relations) {
            processCollection(collection);
        }
        else {
            util.log('No relations found on ' + collectionName + ', moving on...');
        }

        i++;
    }


    /**
     * Updates all relations on a collection
     *
     * @param params object
     */
    function processCollection(params) {
        var relations = params.relations;

        if (relations) {
            for (var attr in relations) {
                updateRelation(attr, params);
            }
        }
    }


    /**
     * Updates all references on a relation
     *
     * @param relationKey string
     * @param collection object
     */
    function updateRelation(relationKey, collection) {
        var relations = collection.relations,
            relation = relations[relationKey],
            using = relation.using || '__' + relationKey, // The attribute to use on origin collection
            via = relation.via || '__id', // The attribute to use on target collection
            targetTable = self.config.collections[relation.table];

        // Create a new connection for the target collection as it may be on another DB
        var targetMongo = new Mongo(self.app);

        targetMongo.connect(targetTable.db || self.config.mongodb.db, function() {

            // Find all records on target collection
            targetMongo.find(targetTable.target, {}, function (result) {

                for (var i in result) {
                    var referencedObj = result[i],
                        set = {},
                        filter = {};

                    filter[using] = referencedObj[via];

                    set[relationKey] = referencedObj._id.toString();
                    set[using] = undefined;

                    // Adds key for relation on origin collection
                    self.mongo.update(collection.target, filter, set, function (result) {
                        // util.log('ok');
                    });
                }
            });
        });
    }
};