var utils = require('zefti-utils');


var typeMap = {
    string : 'S'
  , number : 'N'
  , boolean : 'BOOL'
  , null : 'NULL'
}

var Dynamo = function(db, options){
  console.log('options are::');
  console.log(options);
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
  console.log('inside 2nd create')
  var intArgs = mongo3Arguments(arguments);
  var item = createItem(intArgs[0]);
  var params = {
      Item : item
    , TableName : this.tableName
  };

  this.db.putItem(params, function(err, data) {
    console.log('err');
    console.log(err);
    console.log('data');
    console.log(data);
    return cb(err, data);
  });


};

Dynamo.prototype.find = function(hash, fieldMask, options, cb){

};

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
function mongo5Arguments(arguments){
  return mongoArguments(arguments, 5);
}

function mongo4Arguments(arguments){
  return mongoArguments(arguments, 4);
}

function mongo3Arguments(arguments){
  return mongoArguments(arguments, 3);
}

function mongoArguments(arguments, num){
  var intArgs = [];
  var newArgs = Array.prototype.slice.call(arguments);
  if (newArgs.length === num) return newArgs;
  if (newArgs.length > 1) {
    intArgs[num-1] = newArgs.splice([newArgs.length - 1])[0] || function(){};
    num -= 1;
  }
  for(var i=0;i<num; i++){
    intArgs[i] = newArgs[i] || {};
  }
  return intArgs;
}




module.exports = Dynamo;