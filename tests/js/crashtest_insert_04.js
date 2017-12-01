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
        {
            key: {
                b: 1
            },
            name: "b",
        },
    ]
  }
)

var data_thread = function() {
    var coll = db.getCollection("testt");
    var data = {a: "a", b: [1, 2, 3]};
    coll.insert(data);
};

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
assert.eq(data_count, coll.find().hint({_id: 1}).count(), "Count mismatch! find().count and find().hint({_id: 1}).count()");
assert.eq(data_count, coll.find().hint({a: 1}).count(), "Count mismatch! find().count and find().hint({a: 1}).count()");
assert.eq(data_count, coll.find({b: 1}).count(), "Count mismatch! find().count and find({b: 1}).count()");
assert.eq(data_count, coll.find({b: 2}).count(), "Count mismatch! find().count and find({b: 2}).count()");
assert.eq(data_count, coll.find({b: 3}).count(), "Count mismatch! find().count and find({b: 3}).count()");
