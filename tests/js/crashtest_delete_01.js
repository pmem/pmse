load('jstests/libs/parallelTester.js');
mydbpath = db.serverCmdLineOpts().parsed.storage.dbPath;

db.dropDatabase();
db.runCommand(
    {
        createIndexes: "testt",
        indexes: [
        {
            key: {
                a: 1
            },
            name: "a",
        },
        ]
    }
)

var data_thread = function() {
    var coll = db.getCollection("testt");
    coll.deleteOne({a: "a"});
};

var coll = db.getCollection("testt");
var data = {a: "a"};
coll.insert(data);

dt = new ScopedThread(data_thread);
dt.start();
dt.join();

x = startMongoProgram('mongod', '--storageEngine=pmse', '--dbpath='+mydbpath, '--port', 27017);

var db = x.getDB("test");
var coll = db.getCollection("testt");
var coll_validate = coll.validate();

printjson(coll_validate);

var data_count = coll.find().count();

assert.eq(coll_validate.valid, true, "Collection is not valid");
assert.eq(data_count, coll.find({a: "a"}).count(), "Count mismatch! find().count and find().hint({a: 'a'}).count()");
