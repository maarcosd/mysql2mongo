var util = require('util'),
    _ = require('underscore'),
    assert = require('assert'),
    mongodb = require('mongodb'),
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

    // self.setRelations();
    // return;

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
            util.log('    Found ' + mysqlResult.length + ' records in ' + table);

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
                        util.log('    ' + result.ops.length + ' records imported.');

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
        collectionKeys = config.collectionOrder || _.keys(config.collections),
        i = 0;

    function loopCollections() {
        var tableName = collectionKeys[i],
            collection = collections[tableName],
            collectionName = collection.target || tableName;

        util.log('Setting relations on collection ' + collectionName + ' (' + (i+1) + ' of ' + collectionKeys.length + ')');

        if (i < collectionKeys.length) {
            if (collection.relations) {
                processCollection(collection, function() {
                    i++;
                    loopCollections();
                });
            }
            else {
                util.log('    No relations found on ' + collectionName + ', moving on...');
                i++;
                loopCollections();
            }
        }
    }

    loopCollections();


    /**
     * Updates all relations on a collection
     *
     * @param params object
     * @param callback function
     */
    function processCollection(params, callback) {
        var relations = params.relations,
            relationKeys = _.keys(relations);

        if (relations) {
            var i = 0;

            function processUpdateRelation() {
                if (i < relationKeys.length) {
                    updateRelation(relationKeys[i], params, function () {
                        i++;

                        processUpdateRelation();
                    });
                }
                else {
                    callback();
                }
            }

            processUpdateRelation();
        }
        else {
            callback();
        }
    }


    /**
     * Updates all references on a relation
     *
     * @param relationKey string
     * @param collection object
     * @param callback function
     */
    function updateRelation(relationKey, collection, callback) {
        var relations = collection.relations,
            relation = relations[relationKey],
            using = relation.using || '__' + relationKey, // The attribute to use on origin collection
            via = relation.via || '__id', // The attribute to use on target collection
            targetTable = self.config.collections[relation.table];

        util.log('    Setting ' + relationKey + ' (via ' + using + ') on ' + collection.target);

        // Create a new connection for the target collection as it may be on another DB
        var targetMongo = new Mongo(self.app);

        targetMongo.connect(targetTable.db || self.config.mongodb.db, function() {

            // Find all records on target collection
            targetMongo.find(targetTable.target, {}, function (result) {

                var i = 0;

                function loopResults() {
                    if (i < result.length) {
                        var referencedObj = result[i],
                            set = {},
                            filter = {};

                        filter[using] = referencedObj[via];

                        set[relationKey] = new mongodb.ObjectID(referencedObj._id.toString());
                        set[using] = undefined;

                        // Adds key for relation on origin collection
                        self.mongo.update(collection.target, filter, set, function (result) {
                            util.log('    ' + relationKey + ' in ' + collection.target + ' set for ' + result.result.n + ' records');

                            // if (result.result.n > 0) {
                            //     util.log('      - ' + referencedObj._id.toString() + '  |  ' + JSON.stringify(referencedObj))
                            // }

                            i++;
                            loopResults();
                        });
                    }
                    else {
                        callback();
                    }
                }

                loopResults();
            });
        });
    }
};
