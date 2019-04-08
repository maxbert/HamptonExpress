const cosmos = require("@azure/cosmos");
const CosmosClient = cosmos.CosmosClient;
const config = require("../config");
const databaseId = 'Hampton';
const util = require('util');
const endpoint = config.connection.endpoint;
const masterKey = config.connection.authKey;
const client = new CosmosClient({ endpoint, auth: { masterKey } });


async function init(){
  const database = await client.databases.createIfNotExists({ id: databaseId });
  const users = await database.database.container('users');
  return {database, users}
}

exports.findByToken = async function(token, cb) {
  const {database, users} = await init();
  const querySpec = {
    query: "SELECT * FROM users where users.token = @token",
    parameters: [
      {
        name: "@token",
        value: token
      }

    ]
  };
  const { result: userz } = await users.items.query(querySpec, { enableCrossPartitionQuery: true }).toArray();
  process.nextTick(function() {
    if(userz.length > 0){
      return cb(null, userz[0])
    }
    return cb(null, null);
  });
}
