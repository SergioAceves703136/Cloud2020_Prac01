var AWS = require('aws-sdk');
AWS.config.loadFromPath('./config.json');
var db = new AWS.DynamoDB();
var docClient = new AWS.DynamoDB.DocumentClient();
function keyvaluestore(table) {
    this.inx = -1;
    this.LRU = require("lru-cache");
    this.cache = new this.LRU({max: 500});
    this.tableName = table;
};

/**
 * Initialize the tables
 * 
 */
keyvaluestore.prototype.init = function (callback) {
    var tableName = this.tableName;
    var initCount = this.initCount;
    var self = this;

    db.listTables(function (err, data) {
        if (err)
            console.log(err, err.stack);
        else {
            console.log("Connected to AWS DynamoDB");
            var tables = data.TableNames.toString().split(",");
            console.log("Tables in DynamoDB: " + tables);
            if (tables.indexOf(tableName) === -1) {
                if(tableName === 'Cloud2020_labels'){
                  console.log("Need to create table " + tableName);
                  var params = {
                      AttributeDefinitions:
                              [
                                  {
                                      AttributeName: 'key', 
                                      AttributeType: 'S' 
                                  },
                                  {
                                      AttributeName: 'sortIndex', 
                                      AttributeType: 'N' 
                                  }
                              ],
                      KeySchema:
                              [
                                  {
                                      AttributeName: 'key', 
                                      KeyType: 'HASH' 
                                  },
                                  {
                                      AttributeName: 'sortIndex', 
                                      KeyType: 'RANGE' 
                                  }
                              ],
                      ProvisionedThroughput: {
                          ReadCapacityUnits: 5, 
                          WriteCapacityUnits: 5 
                      },
                      TableName: tableName 
                  };
                  db.createTable(params, function (err, data) {
                      if (err) {
                          console.log(err);
                      } else {
                          console.log("Waiting 10s for consistent state...");
                          setTimeout(function () {
                              self.initCount(callback);
                          }, 10000);
                      }
                  });
                } else{
                  var params = {
                    AttributeDefinitions:
                            [
                                {
                                    AttributeName: 'key', 
                                    AttributeType: 'S' 
                                },
                                {
                                    AttributeName: 'value', 
                                    AttributeType: 'S' 
                                }
                            ],
                    KeySchema:
                            [
                                {
                                    AttributeName: 'key', 
                                    KeyType: 'HASH' 
                                },
                                {
                                    AttributeName: 'value', 
                                    KeyType: 'RANGE' 
                                }
                            ],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 5, 
                        WriteCapacityUnits: 5 
                    },
                    TableName: tableName 
                };
                db.createTable(params, function (err, data) {
                    if (err) {
                        console.log(err);
                    } else {
                        console.log("Waiting 10s for consistent state...");
                        setTimeout(function () {
                            self.initCount(callback);
                        }, 10000);
                    }
                });
              }
            } else {
                self.initCount(callback);
            }
        }
    }
    );
};

/**
 * Gets the count of how many rows are in the table
 * 
 */
keyvaluestore.prototype.initCount = function (whendone) {
    var self = this;
    var params = {
        TableName: self.tableName,
        Select: 'COUNT'
    };

}

/**
 * Get result(s) by key
 * 
 * @param search
 * @param callback
 * 
 * Callback returns a list of objects with keys "inx" and "value"
 */
keyvaluestore.prototype.get = function (search, callback) {
    var params = {
        TableName: this.tableName,
        KeyConditionExpression: "#term = :kw",
        ExpressionAttributeNames: {
            "#term": "key"
        },
        ExpressionAttributeValues: {
            ":kw": search
        }
    };

    docClient.query(params, function (err, data) {
        console.log(params);
        
        if (err) {
            console.log(":( Unable to query. Error: ", JSON.stringify(err, null, 2));
        } else {
            console.log("Query '" + search + "' succeeded: " + data.Count + " record(s) found");
            console.log("\n");
            console.log(data.Items);
            console.log("\n");
        }
        callback(undefined, data.Items);
    });
};

/**
 * Test if search key has a match
 * 
 * @param search
 * @return
 */
keyvaluestore.prototype.exists = function (search, callback) {
    var self = this;

    if (self.cache.get(search))
        callback(null, self.cache.get(search));
    else
        module.exports.get(search, function (err, data) {
            if (err)
                callback(err, null);
            else
                callback(err, (data == null) ? false : true);
        });
};

/**
 * Get result set by key prefix
 * @param search
 *
 * Callback returns a list of objects with keys "inx" and "value"
 */
module.exports = keyvaluestore;