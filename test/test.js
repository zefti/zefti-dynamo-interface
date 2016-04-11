var assert = require('assert');
var async = require('async');
var ZeftiDynamoInterface = require('zefti-dynamo-interface');
var zeftiTest = require('zefti-test');
var dependencies = zeftiTest.dependencies;

/* databases */
var dynamoTestDb = dependencies.dataSources.dynamoTest;

var dynamoTest = new ZeftiDynamoInterface(dynamoTestDb, {tableName:'dev_test'});


/*
 * requires a dynamo table to be created called 'dev_test'
 * table must have _id as its primary partition key
 */

var _id1 = 'abc';
var _id2 = 'def';
var _id3 = 'ghi';
var _id4 = 'jkl';
var _id5 = 'mno';
var message1 = 'foo';
var message2 = 'barfuu';
var message3 = 'faliejaf';
var message4 = 'iefowef';
var x1 = 'afaefg';
var y1 = 'kuhwef';
var z1 = 'afaefaf';
var inc1 = 3;
var inc2 = 5;
var stringSetItem1 = 'a';
var stringSetItem2 = 'b';
var stringSetItem3 = 'c';
var q = 5;
var r = 6;
var s = 7;
var x1partial = 'afae';
var y1partial = 'kuh';

describe('setup', function(){

  before('clear all test data', function(done){
//TODO: make this parallel


    async.waterfall([
      function(cb){
        async.parallel([
            function(cb){
              dynamoTest.remove({_id:_id1}, function(err, result) {
                assert(!err);
                return cb(null);
              });
            },
            function(cb){
              dynamoTest.remove({_id:_id2}, function(err, result) {
                assert(!err);
                return cb(null);
              });
            },
            function(cb){
              dynamoTest.remove({_id:_id3}, function(err, result) {
                assert(!err);
                return cb(null);
              });
            },
            function(cb){
              dynamoTest.remove({_id:_id4}, function(err, result) {
                assert(!err);
                return cb(null);
              });
            },
            function(cb){
              dynamoTest.remove({_id:_id5}, function(err, result) {
                assert(!err);
                return cb(null);
              });
            }
          ],
          function(err, results){
            cb(err);
          }
        );
      },
      function(cb){
        async.parallel([
            function(cb){
              dynamoTest.create({_id:_id3, a:x1, b:q, c:x1}, function(err, result){
                assert(!err);
                return cb(null);
              });
            },
            function(cb){
              dynamoTest.create({_id:_id4, a:x1, b:r, c:y1}, function(err, result){
                assert(!err);
                return cb(null);
              });
            },
            function(cb){
              dynamoTest.create({_id:_id5, a:x1, b:7, c:z1}, function(err, result){
                assert(!err);
                return cb(null);
              });
            }
          ],
          function(err, results){
            cb(err);
          }
        );
      }],
      function(err, results){
        assert(!err);
        done();
      }
    );


  });

  it('should have setup', function(done){
    done();
  })

});

describe('info', function(){

  it('should get info', function(done){
    dynamoTest.info(function(err, result){
      assert(!err);
      //console.log(typeof result);
      done();
    });
  });

});

describe('create', function(){

  it('should error when create has no arguments', function(done){
    dynamoTest.create(function(err, result){
      assert(err);
      assert.equal(err.errCode, '56f6e1abec4ad22804a064f2');
      done();
    });
  });

  it('should error when create has empty object for hash', function(done){
    dynamoTest.create({}, function(err, result){
      assert(err);
      assert.equal(err.errCode, '56f6e1abec4ad22804a064f2');
      done();
    });
  });

  it('should error when primary key not included', function(done){
    dynamoTest.create({foo:'bar'}, function(err, result){
      assert(err);
      done();
    });
  });

  it('should successfully create item', function(done){
    dynamoTest.create({_id:_id1, message:message1}, function(err, result){
      assert(!err);
      done();
    });
  });

  it('should fail create item because id already exists', function(done){
    dynamoTest.create({_id:_id1, message:message1}, function(err, result){
      assert(err);
      assert.equal(err.errCode, '56f8dbcf3e56b04f0875ceae');
      assert.equal(err.err.code, 'ConditionalCheckFailedException');
      done();
    });
  });

});


describe('findById', function() {

  it('should find by id', function(done){
    dynamoTest.findById({_id:_id1}, function(err, result){
      assert(!err);
      assert.equal(result.message, message1);
      done();
    });
  });

});

describe('update', function() {
  var returnedUpdate1 = null;
  var returnedUpdate2 = null;
  var foundUpdate = null;

  it('should update item with id', function(done){
    dynamoTest.update({_id:_id1}, {message:message2}, function(err, result){
      assert(!err);
      returnedUpdate1 = result;
      done();
    });
  });

  it('should return the newest values by default', function(done){
    assert.equal(returnedUpdate1.message, message2);
    done();
  });

  it('after update, finding message should return message2', function(done){
    dynamoTest.findById({_id:_id1}, function(err, result){
      assert(!err);
      foundUpdate = result;
      assert.equal(result.message, message2);
      done();
    });
  });

  it('should update item with id', function(done){
    dynamoTest.update({_id:_id1}, {message:message3}, {returnedState:'old'}, function(err, result){
      assert(!err);
      returnedUpdate2 = result;
      done();
    });
  });

  it('should return the oldest values when specified', function(done){
    assert.equal(returnedUpdate2.message, message2);
    done();
  });

  it('after update, finding message should return message3', function(done){
    dynamoTest.findById({_id:_id1}, function(err, result){
      assert(!err);
      foundUpdate = result;
      assert.equal(result.message, message3);
      done();
    });
  });

  it('should remove the message field', function(done){
    dynamoTest.update({_id:_id1}, {$del:['message']}, function(err, result){
      assert(!err);
      assert(!result.message);
      done();
    });
  });

  it('after update, should have no message present', function(done){
    dynamoTest.findById({_id:_id1}, function(err, result){
      assert(!err);
      assert(!result.message);
      //assert.equal(result.x, x1);
      done();
    });
  });

  it('should update/add 2 fields', function(done){
    dynamoTest.update({_id:_id1}, {message:message3, x:x1, y:y1, z:z1}, {returnedState:'old'}, function(err, result){
      assert(!err);
      returnedUpdate2 = result;
      done();
    });
  });

  it('after update, should have the 2 new fields present', function(done){
    dynamoTest.findById({_id:_id1}, function(err, result){
      assert(!err);
      assert.equal(result.message, message3);
      assert.equal(result.x, x1);
      assert.equal(result.y, y1);
      assert.equal(result.z, z1);
      done();
    });
  });

  it('should update field & remove field', function(done){
    dynamoTest.update({_id:_id1}, {message:message4, $del:['x']}, {returnedState:'old'}, function(err, result){
      assert(!err);
      done();
    });
  });

  it('after update, should have new message set, and no x field', function(done){
    dynamoTest.findById({_id:_id1}, function(err, result){
      assert(!err);
      assert.equal(result.message, message4);
      assert(!result.x);
      assert(result.y);
      assert(result.z);
      done();
    });
  });

  it('should update field & remove field', function(done){
    dynamoTest.update({_id:_id1}, {message:message1, $del:['y', 'z'], x:x1}, {returnedState:'old'}, function(err, result){
      assert(!err);
      done();
    });
  });

  it('after update, should have new message set, and no x field', function(done){
    dynamoTest.findById({_id:_id1}, function(err, result){
      assert(!err);
      assert.equal(result.message, message1);
      assert.equal(result.x, x1);
      assert(!result.y);
      assert(!result.z);
      done();
    });
  });

  it('should increment a new field, and default to the increment value', function(done){
    dynamoTest.update({_id:_id1}, {$inc:{myInc:inc1}}, function(err, result){
      assert(!err);
      done();
    });
  });

  it('after update, should have new message set, and no x field', function(done){
    dynamoTest.findById({_id:_id1}, function(err, result){
      assert(!err);
      assert.equal(result.myInc, inc1);
      done();
    });
  });

  it('should increment a new field, and default to the increment value', function(done){
    dynamoTest.update({_id:_id1}, {$inc:{myInc:inc2, myInc2:inc1}}, function(err, result){
      assert(!err);
      done();
    });
  });

  it('after update, should have new message set, and no x field', function(done){
    dynamoTest.findById({_id:_id1}, function(err, result){
      assert(!err);
      var total = inc1 + inc2;
      assert.equal(result.myInc, total);
      assert.equal(result.myInc2, inc1);
      done();
    });
  });

  it('should increment a new field, and default to the increment value', function(done){
    dynamoTest.update({_id:_id1}, {$inc:{myInc2:inc1}, $dec:{myInc:inc1}}, function(err, result){
      assert(!err);
      done();
    });
  });

  it('after update, should have new message set, and no x field', function(done){
    dynamoTest.findById({_id:_id1}, function(err, result){
      assert(!err);
      var total = inc1 + inc1;
      assert.equal(result.myInc, inc2);
      assert.equal(result.myInc2, total);
      done();
    });
  });

  it('should add a set', function(done){
    dynamoTest.update({_id:_id1}, {$push:{mySet: [stringSetItem1]}}, function(err, result){
      assert(!err);
      done();
    });
  });

  it('should add multiple items to a set', function(done){
    dynamoTest.update({_id:_id1}, {$push:{mySet: [stringSetItem2, stringSetItem3]}}, function(err, result){
      assert(!err);
      done();
    });
  });

  it('should retrieve item with a set containing 3 items', function(done){
    dynamoTest.findById({_id:_id1}, function(err, result){
      assert(!err);
      assert(result.mySet);
      assert.equal(result.mySet.length, 3);
      assert.equal(result.mySet.indexOf('a'), 0);
      assert.equal(result.mySet.indexOf('b'), 1);
      assert.equal(result.mySet.indexOf('c'), 2);
      done();
    });
  });

  it('should remove an item from a set', function(done){
    dynamoTest.update({_id:_id1}, {$pull:{mySet: [stringSetItem2]}}, function(err, result){
      assert(!err);
      done();
    });
  });

  it('should retrieve item with a set containing 2 items', function(done){
    dynamoTest.findById({_id:_id1}, function(err, result){
      assert(!err);
      assert(result.mySet);
      assert.equal(result.mySet.length, 2);
      assert.equal(result.mySet.indexOf('a'), 0);
      assert.equal(result.mySet.indexOf('c'), 1);
      assert.equal(result.mySet.indexOf('b'), -1);
      done();
    });
  });


});

describe('find', function() {

   it('should find 3 items when testing equality', function(done){
     dynamoTest.find({a:x1}, {}, {indexName:'a-b-index'}, function(err, results){
       assert(!err);
       assert.equal(results.length, 3);
       done();
     });
   });

  it('should find 1 item when testing two field equality', function(done){
    dynamoTest.find({a:x1, b:s}, {}, {indexName:'a-b-index'}, function(err, results){
      assert(!err);
      assert.equal(results.length, 1);
      done();
    });
  });

  it('should find 2 items when testing $gt for range key', function(done){
    dynamoTest.find({a:x1, $gt:{b:5}}, {}, {indexName:'a-b-index'}, function(err, results){
      assert(!err);
      assert.equal(results.length, 2);
      done();
    });
  });

  it('should find 3 items when testing $gte for range key', function(done){
    dynamoTest.find({a:x1, $gte:{b:5}}, {}, {indexName:'a-b-index'}, function(err, results){
      assert(!err);
      assert.equal(results.length, 3);
      done();
    });
  });

  it('should find 1 item when testing $lte for range key', function(done){
    dynamoTest.find({a:x1, $lte:{b:5}}, {}, {indexName:'a-b-index'}, function(err, results){
      assert(!err);
      assert.equal(results.length, 1);
      done();
    });
  });

  it('should find 0 items when testing $lt for range key', function(done){
    dynamoTest.find({$lt:{b:5}, a:x1}, {}, {indexName:'a-b-index'}, function(err, results){
      assert(!err);
      assert.equal(results.length, 0);
      done();
    });
  });

  it('should find 2 items when testing $begins', function(done){
    dynamoTest.find({a:x1, $begins:{c:x1partial}}, {}, {indexName:'a-c-index'}, function(err, results){
      assert(!err);
      assert.equal(results.length, 2);
      done();
    });
  });

  it('should find 1 item when testing $begins', function(done){
    dynamoTest.find({a:x1, $begins:{c:y1partial}}, {}, {indexName:'a-c-index'}, function(err, results){
      assert(!err);
      assert.equal(results.length, 1);
      done();
    });
  });

  it('should find 2 item when $lt and $gt', function(done){
    dynamoTest.find({a:x1, $gt:{b:5}, $lt:{b:7}}, {}, {indexName:'a-b-index'}, function(err, results){
      assert(!err);
      assert.equal(results.length, 3);
      done();
    });
  });

  it('should find 2 item when $lt and $gt', function(done){
    dynamoTest.find({a:x1, $gt:{b:6}, $lt:{b:7}}, {}, {indexName:'a-b-index'}, function(err, results){
      assert(!err);
      assert.equal(results.length, 2);
      done();
    });
  });



});

