load('jstests/libs/parallelTester.js');
mydbpath = db.serverCmdLineOpts().parsed.storage.dbPath;

db.dropDatabase();
db.runCommand(
  {
    createIndexes: "testt",
    indexes: [
        {
            key: {
                h: "hashed",
            },
            name: "h",
        },
    ]
  }
)

var data_thread = function() {
    var coll = db.getCollection("testt");
    var data = {h: 1, h2: 2};
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
assert.eq(data_count, coll.find({h: 1}).count(), "Count mismatch! find().count and find(hashed).count()");
