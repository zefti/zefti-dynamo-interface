var assert = require('assert');
var async = require('async');
var ZeftiDynamoInterface = require('zefti-dynamo-interface');
var zeftiTest = require('zefti-test');
var dbTests = zeftiTest.dbTests;
var dependencies = zeftiTest.dependencies;

/* databases */
var dynamoTestDb = dependencies.dataSources.dynamoTest;
var dynamoTest = new ZeftiDynamoInterface(dynamoTestDb, {tableName:'dev_test'});


/*
 * requires a dynamo table to be created called 'dev_test'
 * table must have _id as its primary partition key
 */

dbTests.setup(dynamoTest);
dbTests.info(dynamoTest);
dbTests.createWithoutContent(dynamoTest);
dbTests.createWithoutPrimaryKey(dynamoTest);
dbTests.createItem(dynamoTest);
dbTests.findById(dynamoTest);
dbTests.updateNew(dynamoTest);
dbTests.updateOld(dynamoTest);
dbTests.removeField(dynamoTest);
dbTests.multiUpdate(dynamoTest);
dbTests.updateAndRemoveField(dynamoTest);
dbTests.updateAndMultiFieldDelete(dynamoTest);
dbTests.increment(dynamoTest);
dbTests.incrementNewAndExisting(dynamoTest);
dbTests.incrementAndDecrement(dynamoTest);
dbTests.setAddition(dynamoTest);
dbTests.setRemoval(dynamoTest);
dbTests.findByFieldEquality(dynamoTest);
dbTests.findByDualFieldEquality(dynamoTest);
dbTests.findWithGtAndLt(dynamoTest);
dbTests.removeAll(dynamoTest);


