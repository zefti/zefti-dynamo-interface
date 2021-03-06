var utils = require('zefti-utils');
var async = require('async');
var uuid = require('node-uuid');
var randtoken = require('rand-token');
var errors = require('./lib/errors.json');
var errorHandler = require('zefti-error-handler');
errorHandler.addErrors(errors);

/*
 * We will make the requirement that all tables must have a _id field
 */



/*
 * Create if doesn't exit
 * model
 *
 * //update if name is null
 * db.update({_id:123}, {name:'bob'}, {condition:{$eq:{name:null}}})
 *
 * //update specific item if price is > 5
 * db.update({_id:123}, {price:10}, {condition:{$gt:{price:5}})
 *
 * //update specific item only if exists
 *
 *
 * //update item and return new state
 * db.update({_id:123}, {foo:'bar'}, {returnedState: 'new' || 'old' })  (default new)
 *
 * //update item and return only specific fields
 * db.update({_id:123}, {foo:'bar'}, {returnedFields : ['price', 'created', '_id]})
 *
 * //update item and return specific fields with old values
 * db.update({_id:123}, {foo:'bar'}, {returnedFields : ['price'], {returnedState:'old'})
 *
 * //remove field from item
 * db.update({_id:123}, {$del:['last_name']})
 *
 * //change field and remove another field
 * db.update({_id:123}, {foo:'bar', $del:['last_name']});
 *
 * //remove item from set
 * db.update({id:123}, {$pull:{mySet:['a', 'c']}});
 *
 * //add item to set
 * db.update({_id:123}, {$push:{mySet:['z']}});
 *
 * //////////////////////////////////////////////////////
 *
 * //find an item by equivalence
 * db.find({field1:'value1'});
 *
 * //find an item by double equivalence
 * db.find({field1:'value1', field2:5}
 *
 * //find an item with gt
 * db.find({field1:'value1', $gt:{field2:3});
 *
 * //find an item with gte
 * db.find({field1:'value1', $gt:{field2:5});
 *
 * //find an item with lt
 * db.find({field1:'value1', $lt:{field2:7});
 *
 * //find an item with gte
 * db.find({field1:'value1', $lte:{field2:5});
 *
 * //find an item with gt & lt
 * db.find({field1:'value1', $gt:{field2:3}, $lt:{field2:7});
 *
 * //find an item with begins
 * db.find({field1:'value1', $begins:{someField:'abc'});
 *
 */


var typeMap = {
    string : 'S'
  , number : 'N'
  , boolean : 'BOOL'
  , null : 'NULL'
};

var arrTypeMap = {
    string : 'SS'
  , number : 'NS'
};

var expressionMap = {
    '$gt' : ' > :'
  , '$lt' : ' < :'
  , '$gte' : ' >= :'
  , '$lte' : ' <= :'
};

var returnedStateMap = {
    new : 'ALL_NEW'
  , old : 'ALL_OLD'
};

var updateMap = {
    '$del' : 'REMOVE'
  , '$push' : 'ADD'
  , '$pull' : 'DELETE'
  , '$inc' : 'ADD'
  , '$dec' : 'ADD'
};

var findMap = {
    '$gt'  : '>'
  , '$gte' : '>='
  , '$lt'  : '<'
  , '$lte' : '<='
  , '$begins' : 'begins_with'
};



var Dynamo = function(db, options){
  this.db = db;
  if (options.tableName) this.tableName = options.tableName;
  return this;
};


Dynamo.prototype.info = function(cb){
  this.db.describeTable({TableName:this.tableName}, function(err, result){
    cb(err, result);
  });
};

Dynamo.prototype.create = function(){
  var args = utils.resolve3Arguments(arguments);
  var hash = args[0];
  var options = args[1];
  var cb = args[2];
  var item = formatKey(hash);
  var params = {
      Item : item
    , TableName : this.tableName
    , ConditionExpression : 'attribute_not_exists(#I)'
    , ExpressionAttributeNames : {"#I":"_id"}
  };
  if (Object.keys(hash).length === 0) return cb({errCode:'56f6e1abec4ad22804a064f2'});
  if (!hash._id ) return cb({errCode:'56f6e1abec4ad22804a064f3'});
  //console.log(params);
  this.db.putItem(params, function(err, data) {
    return cb(err, data);
  });
};



Dynamo.prototype.find = function(){
//TODO: fix the arguments, use intArgs
  var self = this;
  var intArgs = utils.resolve4Arguments(arguments);
  var hash = intArgs[0];
  var fieldMask = intArgs[1];
  var options = intArgs[2];
  var cb = intArgs[3];
  var query = formatQuery(hash);
  //console.log('QUERY::::::');
  //console.log(query);

  var params = {
      TableName: this.tableName
    , KeyConditionExpression: query.expression
    , ExpressionAttributeValues: query.values
    , ExpressionAttributeNames : query.names
    , IndexVariables : query.indexVariables
  };

  if (options.$limit) params.Limit = options.$limit;
  params.ScanIndexForward = false;
  if (options.$sort && options.$sort[query.indexVariables.range] === -1) params.ScanIndexForward = true;

  //console.log('FIND PARAMS TO DYNAMO::::::');
  //console.log(params);


  async.waterfall([
    function(cb) {
      if (!options.last_item) return cb(null, null);
      var params2 = { Key : formatKey({_id:options.last_item}), TableName :self.tableName};
      self.db.getItem(params2, function(err, result){
        if (err) return cb(err);
        var exclusiveStartKey = {};
        if (!result || !result.Item._id) return cb({errCode:'56f6e1abec4ad22804a064f5'}, null);
        exclusiveStartKey._id = result.Item._id;
        params.IndexVariables.equality.forEach(function(field){
          exclusiveStartKey[field] = result.Item[field];
        });
        if (params.IndexVariables.range) exclusiveStartKey[params.IndexVariables.range] = result.Item[params.IndexVariables.range];
        cb(null, exclusiveStartKey);
      })
    },
    function(exclusiveStartKey, cb){
      if (exclusiveStartKey) params.ExclusiveStartKey = exclusiveStartKey;
      self.db.query(params, function(err, data){
        return cb(err, data)
      });
    }],
    function(err, data){
      if (err) return cb(err, null);
      var last_item = null;
      if (data.LastEvaluatedKey) last_item = data.LastEvaluatedKey._id[Object.keys(data.LastEvaluatedKey._id)[0]];
      cb (err, cleanData(data), last_item);
    }
  );
};

Dynamo.prototype.findAll = function(cb){
  var params = {
    TableName: this.tableName
  };
  this.db.scan(params, function(err, results) {
    return cb(err, cleanData(results));
  })
};


Dynamo.prototype.findAndModify = function(hash, sort, update, options, cb){

};


Dynamo.prototype.findById = function(){
  var args = utils.resolve4Arguments(arguments);
  var id = args[0];
  var hash = {_id:id};
  var fieldMask = args[1];
  var options = args[2];
  var cb = args[3];

  var params = {
      Key : formatKey(hash)
    , TableName : this.tableName
  };

  this.db.getItem(params, function(err, result){
    if (err) return cb(err, null);
    return cb(null, cleanData(result));
  });
};

Dynamo.prototype.findByIdMulti = function(ids, fieldMask, options, cb){

};

//TODO: write unit tests for this one
Dynamo.prototype.upsert = function(){
  var intArgs = utils.resolve3Arguments(arguments);
  var hash = intArgs[0];
  var options = intArgs[1];
  var cb = intArgs[2];
  var item = formatKey(hash);
  var params = {
      Item : item
    , TableName : this.tableName
  };
  if (Object.keys(hash).length === 0) return cb({errCode:'56f6e1abec4ad22804a064f2'});
  this.db.putItem(params, function(err, data) {
    return cb(err, data);
  });
};

Dynamo.prototype.update = function(){
  var args = utils.resolve4Arguments(arguments);
  var item = args[0];
  var update = args[1];
  var options = args[2];
  var cb = args[3];
  var itemObj = formatKey(item);
  var updateObj = formatUpdateExpression(update);

  var params = {
      Key : itemObj
    , TableName : this.tableName
    , UpdateExpression : updateObj.expression
    , ExpressionAttributeNames : updateObj.names
    , ReturnValues : defineReturnState(options)
  };
  if (Object.keys(updateObj.values).length !== 0) params.ExpressionAttributeValues = updateObj.values;

  this.db.updateItem(params, function(err, result){
    if (err) console.log(err);
    return cb(err, cleanData(result));
  });
};

Dynamo.prototype.updateById = function(id, update, options, cb){

};

Dynamo.prototype.remove = function(hash, options, cb){
  var intArgs = utils.resolve3Arguments(arguments);
  var hash = intArgs[0];
  var options = intArgs[1];
  var cb = intArgs[2];
  var key = formatKey(hash);
  var params = {
      Key : key
    , TableName : this.tableName
  };
  this.db.deleteItem(params, function(err, data) {
    return cb(err, data);
  });
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

Dynamo.prototype.removeAll= function(cb){
  //console.log('calling removeAll')
  var self = this;
  var deletedItems = [];
  var params = {
    TableName: this.tableName
  };
  this.db.scan(params, function(err, results){
    if (!results || results.length === 0 || !results.Items || results.Items.length === 0) return cb(null, []);
    var params = {RequestItems:{}};
    params.RequestItems[self.tableName] = [];
    results.Items.forEach(function(item){
      var x = {DeleteRequest:{Key:{_id:item._id}}};
      params.RequestItems[self.tableName].push(x);
      deletedItems.push(item);
    });
    self.db.batchWriteItem(params, function(err, result){
      if (err) return cb(err);
      return cb(null, deletedItems);
    });
  });
};

Dynamo.prototype.expire = function(hash, options, cb){

};

Dynamo.prototype.expireById = function(id, options, cb){

};

Dynamo.prototype.getNewId = function(options){

};




module.exports = Dynamo;




/*
 * helper functions
*/

function formatKey(hash){
  var item = {};
  for (var key in hash) {
    //TODO: this doesn't work for 0, empty string, etc.  Create and use utils.exists
    if (hash[key]) {
      var someKey = {};
      var type = typeMap[utils.type(hash[key])];
       if (!type && utils.type(hash[key]) === 'array') {
         if (utils.type(hash[key][0]) === 'string') type = 'SS';
         if (utils.type(hash[key][0]) === 'number') type = 'NS';
         someKey[type] = hash[key];
       } else {
         someKey[type] = hash[key].toString();
       }
      item[key] = someKey;
    }
  }
  return item;
}



function defineReturnState(hash) {
  return returnedStateMap[hash.returnedState] || 'ALL_NEW';
}


function formatUpdateExpression(hash){
  var updateExpression = '';
  var updateValues = {};
  var updateNames = {};
  var updateCommand = '';
  var updateFragment = '';
  var setUpdateFragment = '';
  var removeUpdateFragment = '';
  var addUpdateFragment = '';
  var deleteUpdateFragment = '';
  //console.log(hash)
  //console.log('----')
  for (var key in hash) {
    updateFragment = '';
    updateCommand = updateMap[key] || 'SET';

    if (updateCommand === 'SET') {
      if (setUpdateFragment) setUpdateFragment = setUpdateFragment + ', ';
      if (!setUpdateFragment) setUpdateFragment = updateCommand;
      var token1 = randtoken.generate(8);
      var token2 = randtoken.generate(8);
      updateNames['#' + token1] = key;
      setUpdateFragment = setUpdateFragment + ' #' + token1 + '=:' + token2;
      updateValues[':' + token2] = {};
      updateValues[':' + token2][typeMap[utils.type(hash[key])]] = hash[key];
    }

    if (updateCommand === 'REMOVE') {
      hash[key].forEach(function(field){
        if (removeUpdateFragment) removeUpdateFragment = removeUpdateFragment + ', ';
        if (!removeUpdateFragment) removeUpdateFragment = updateCommand;
        var token = randtoken.generate(8);
        removeUpdateFragment = removeUpdateFragment + ' #' + token;
        updateNames['#' + token] = field;
      });
    }

    if (key === '$inc') {
      for (var key2 in hash[key]) {
        if (addUpdateFragment) addUpdateFragment = addUpdateFragment + ', ';
        if (!addUpdateFragment) addUpdateFragment = updateCommand;
        var token1 = randtoken.generate(8);
        var token2 = randtoken.generate(8);
        updateNames['#' + token1] = key2;
        addUpdateFragment = addUpdateFragment + ' #' + token1 + ' :' + token2;
        updateValues[':' + token2] = {};
        updateValues[':' + token2][typeMap[utils.type(hash[key][key2])]] = hash[key][key2].toString();
      }
    }

    if (key === '$dec') {
      for (var key2 in hash[key]) {
        if (addUpdateFragment) addUpdateFragment = addUpdateFragment + ', ';
        if (!addUpdateFragment) addUpdateFragment = updateCommand;
        var token1 = randtoken.generate(8);
        var token2 = randtoken.generate(8);
        updateNames['#' + token1] = key2;
        addUpdateFragment = addUpdateFragment + ' #' + token1 + ' :' + token2;
        updateValues[':' + token2] = {};
        var negValue = -hash[key][key2];
        updateValues[':' + token2][typeMap[utils.type(hash[key][key2])]] = negValue.toString();
      }
    }

    if (key === '$push') {
      for (var key2 in hash[key]) {
        if (addUpdateFragment) addUpdateFragment = addUpdateFragment + ', ';
        if (!addUpdateFragment) addUpdateFragment = updateCommand;
        var token1 = randtoken.generate(8);
        var token2 = randtoken.generate(8);
        updateNames['#' + token1] = key2;
        addUpdateFragment = addUpdateFragment + ' #' + token1 + ' :' + token2;
        updateValues[':' + token2] = {};
        updateValues[':' + token2][arrTypeMap[utils.type(hash[key][key2][0])]] = hash[key][key2];
      }
    }

    if (key === '$pull') {
      for (var key2 in hash[key]) {
        if (deleteUpdateFragment) deleteUpdateFragment = deleteUpdateFragment + ', ';
        if (!deleteUpdateFragment) deleteUpdateFragment = updateCommand;
        var token1 = randtoken.generate(8);
        var token2 = randtoken.generate(8);
        updateNames['#' + token1] = key2;
        deleteUpdateFragment = deleteUpdateFragment + ' #' + token1 + ' :' + token2;
        updateValues[':' + token2] = {};
        updateValues[':' + token2][arrTypeMap[utils.type(hash[key][key2][0])]] = hash[key][key2];
      }
    }

  }

  if (setUpdateFragment) updateExpression = updateExpression + setUpdateFragment;
  if (removeUpdateFragment) {
    if (updateExpression) updateExpression = updateExpression + ' ';
    updateExpression = updateExpression + removeUpdateFragment;
  }
  if (addUpdateFragment){
    if (updateExpression) updateExpression = updateExpression + ' ';
    updateExpression = updateExpression + addUpdateFragment;
  }

  if (deleteUpdateFragment){
    if (updateExpression) updateExpression = updateExpression + ' ';
    updateExpression = updateExpression + deleteUpdateFragment;
  }

  return ({expression:updateExpression, values:updateValues, names:updateNames});
}

function cleanData(data){
  var cleanData = [];
  var type = null;
  var dataObjItem = {};
  if (data && data.Items) {
    type = 'array';
    data.Items.forEach(function (item) {
      var dataObj = {};
      for (var key in item) {
        for (var key2 in item[key]) {
          if (key2 === 'N') {
            dataObj[key] = Number(item[key][key2]);
          } else {
            dataObj[key] = item[key][key2];
          }
        }
      }
      cleanData.push(dataObj);
    });
  }

  if (data && data.Item) {
    type = 'object';
    for (var key in data.Item) {
      for (var key2 in data.Item[key]) {
        if (key2 === 'N') {
          dataObjItem[key] = Number(data.Item[key][key2]);
        } else {
          dataObjItem[key] = data.Item[key][key2];
        }
      }
    }
  }

  if (data && data.Attributes) {
    type = 'object';
    for (var key in data.Attributes) {
      for (var key2 in data.Attributes[key]) {
        if (key2 === 'N') {
          dataObjItem[key] = Number(data.Attributes[key][key2]);
        } else {
          dataObjItem[key] = data.Attributes[key][key2];
        }
      }
    }
  }

  if (type === 'array') return cleanData;
  if (type === 'object') return dataObjItem;
  return {};

}

function formatQuery(hash){
  var baseExpression = '';
  var rangeExpression = '';
  var beginExpression = '';
  var startBetweenExpression = '';
  var betweenExpression = '';
  var betweenName = '';
  var betweenOperator = null;
  var betweenValue1 = null;
  var betweenValue2 = null;
  var betweenKey = null;
  var indexVariables = {equality:[]};
  var values = {};
  var names = {};

  for (var key in hash) {
    var token1 = randtoken.generate(8);
    var token2 = randtoken.generate(8);
    var operator = findMap[key] || '=';
    values[':' + token2] = {};

    if (operator === '=') {
      indexVariables.equality.push(key);
      names['#' + token1] = key;
      values[':' + token2][typeMap[utils.type(hash[key])]] = hash[key].toString();
    } else {
      indexVariables.range = Object.keys(hash[key])[0];
      names['#' + token1] = Object.keys(hash[key])[0];
      values[':' + token2][typeMap[utils.type(hash[key][Object.keys(hash[key])[0]])]] = hash[key][Object.keys(hash[key])[0]].toString();
    }
    if (operator === '=') {
      if (baseExpression) baseExpression = baseExpression + ' AND ';
      baseExpression = baseExpression +  '#' + token1 + ' ' + operator + ' :' + token2;
    } else if (operator === 'begins_with') {
      beginExpression =  'begins_with (#' + token1 + ', :' + token2 + ')';
    } else {
      if (!rangeExpression) {
        rangeExpression = '#' + token1 + ' ' + operator + ' :' + token2;
        startBetweenExpression = '#' + token1 + ' BETWEEN ' + ':' + token2;
        betweenName = ':' + token2;
        betweenOperator = operator;
        betweenKey = key;
        betweenValue1 = hash[key][Object.keys(hash[key])[0]];
      } else {
        betweenExpression = startBetweenExpression + ' AND ' + ':' + token2;
        if (betweenOperator === '>') {
          values[betweenName][typeMap[utils.type(hash[betweenKey][Object.keys(hash[betweenKey])[0]])]] = String(betweenValue1 +.00000001);
        }
        if (betweenOperator === '<') {
          values[betweenName][typeMap[utils.type(hash[betweenKey][Object.keys(hash[betweenKey])[0]])]] = String(betweenValue1 -.00000001);
        }

        if (operator === '>') {
          values[':' + token2][typeMap[utils.type(hash[key][Object.keys(hash[key])[0]])]] = String(hash[key][Object.keys(hash[key])[0]] +.00000001);
        }
        if (operator === '<') {
          values[':' + token2][typeMap[utils.type(hash[key][Object.keys(hash[key])[0]])]] = String(hash[key][Object.keys(hash[key])[0]] -.00000001);
        }

        delete names['#' + token1];
      }
    }
  }
  if (rangeExpression && !betweenExpression) baseExpression = baseExpression + ' AND ' + rangeExpression;
  if (betweenExpression) baseExpression = baseExpression + ' AND ' + betweenExpression;
  if (beginExpression) baseExpression = baseExpression + ' AND ' + beginExpression;
  return {expression:baseExpression, values:values, names:names, indexVariables:indexVariables};
}

function getIndexName(hash){
  var indexName = '';
  hash.equality.sort();
  hash.equality.forEach(function(name){
    indexName = indexName + name + '-';
  });
  if (hash.range) indexName = indexName + hash.range + '-';
  indexName = indexName + 'index';
  return indexName;
}





