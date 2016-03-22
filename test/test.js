var assert = require('assert');
var async = require('async');
var ZeftiDynamoInterface = require('zefti-dynamo-interface');
var zeftiTest = require('zefti-test');
var dependencies = zeftiTest.dependencies;

/* databases */
var dynamoTestDb = dependencies.dataSources.dynamoTest;

var dynamoTest = new ZeftiDynamoInterface(dynamoTestDb, {tableName:'dev_test'});

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
      done();
    });
  });
});
