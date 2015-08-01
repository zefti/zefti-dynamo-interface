var utils = require('zefti-utils');
var common = require('zefti-common');

/*  EXAMPLE QUERY
var params = {
  IndexName : 'recipient-ts-index'
  , TableName : this.tableName
  , KeyConditionExpression : 'recipient = :xyz'
  , ExpressionAttributeValues : {
    ":xyz" : {
      S : "4dff4ea340f0a823f15d3f4f01ab62eae0e5da579ccb851f8db9dfe84c58b2b37b89903a740e1ee172da793a6e79d560e5f7f9bd058a12a280433ed6fa46510a"
    }
  }
};
*/


var typeMap = {
    string : 'S'
  , number : 'N'
  , boolean : 'BOOL'
  , null : 'NULL'
}

var Dynamo = function(db, options){
  this.db = db;
  if (options.tableName) this.tableName = options.tableName;
  return this;
};

function createItem(hash){
  var item = {};
  for (var key in hash) {
    var someKey = {};
    var type = typeMap[utils.type(hash[key])];
    someKey[type] = hash[key].toString();
    item[key] = someKey;
  }
  return item;
}

Dynamo.prototype.create = function(hash, options, cb){
  var intArgs = common.process3DbArguments(arguments)
  var item = createItem(intArgs[0]);
  var params = {
      Item : item
    , TableName : this.tableName
  };

  this.db.putItem(params, function(err, data) {
    return cb(err, data);
  });


};


var expressionMap = {
    "$gt" : " > :"
  , "$lt" : " < :"
  , "$gte" : " >= :"
  , "$lte" : " <= :"
};


function formatQuery(hash){
  var expression = "";
  var attributes = {};
  var i = 0;
  for (var key in hash) {
    i++;
    if (i>1) expression += ' AND ';
    expression += key;

    if (utils.type(hash[key]) === 'string') {
      expression +=  ' = :';
      attributes[':'+key] = {};
      attributes[':'+key][typeMap[utils.type(hash[key])]] = hash[key].toString();
    } else {
      for (var key2 in hash[key]) {
        expression += expressionMap[key2];
        attributes[':'+key] = {};
        attributes[':'+key][typeMap[utils.type(hash[key][key2])]] = hash[key][key2].toString();
      }
    }
    expression += key;
  }
  return {expression:expression, attributes:attributes};
}




Dynamo.prototype.find = function(hash, fieldMask, options, cb){
//TODO: fix the arguments
  var intArgs = common.process4DbArguments(arguments);
  var item = createItem(intArgs[0]);
  var query = formatQuery(hash);
  var params = {
      IndexName: options.index
    , TableName: this.tableName
    , KeyConditionExpression: query.expression
    , ExpressionAttributeValues: query.attributes
  };


  console.log('params to dynamo:');
  console.log(params);

  this.db.query(params, function(err, data){
    console.log(err);
    console.log(data);
    if (err) return cb(err, null);
    var cleanData = [];

    data.Items.forEach(function(item){
      var dataObj = {};
      for (var key in item) {
        for (var key2 in item[key]) {
          dataObj[key] = item[key][key2];
        }
      }
      cleanData.push(dataObj);
    });
    cb(err, cleanData);
  });
};

/*
 , Limit : 9
 , ExclusiveStartKey : { ts: { N: '1438153408999' },
 connection: { S: 'd5436964ccd629b72910317c0bbea189' },
 recipient: { S: '40b244112641dd78dd4f93b6c9190dd46e0099194d5a44257b7efad6ef9ff4683da1eda0244448cb343aa688f5d3efd7314dafe580ac0bcbf115aeca9e8dc114' } }
 */

Dynamo.prototype.findAndModify = function(hash, sort, update, options, cb){

};


Dynamo.prototype.findById = function(id, fieldMask, options, cb){

};

Dynamo.prototype.findByIdMulti = function(ids, fieldMask, options, cb){

};

Dynamo.prototype.upsert = function(hash, update, options, cb){

};

Dynamo.prototype.update = function(hash, update, options, cb){

};

Dynamo.prototype.updateById = function(id, update, options, cb){

};

Dynamo.prototype.remove = function(hash, options, cb){

};

Dynamo.prototype.removeById = function(id, options, cb){

};

Dynamo.prototype.removeFields = function(hash, update, options, cb){

};

Dynamo.prototype.removeFieldsById = function(id, fields, options, cb){

};

Dynamo.prototype.addToSet = function(hash, update, options, cb){

};

Dynamo.prototype.addToSetById = function(id, update, options, cb){

};

Dynamo.prototype.removeFromSet = function(hash, update, options, cb){

};

Dynamo.prototype.removeFromSetById = function(id, update, options, cb){

};

Dynamo.prototype.expire = function(hash, options, cb){

};

Dynamo.prototype.expireById = function(id, options, cb){

};

Dynamo.prototype.getNewId = function(options){

};





module.exports = Dynamo;