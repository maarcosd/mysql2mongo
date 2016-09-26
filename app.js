var Mongo = require('./lib/mongo.js'),
    MySQL = require('./lib/mysql.js'),
    Importer = require('./lib/importer.js'),
    config = require('./config.js');

var app = {};
app.config = config;
app.mongo = new Mongo(app);
app.mysql = new MySQL(app);
app.tailer = new Importer(app);

app.tailer.start();