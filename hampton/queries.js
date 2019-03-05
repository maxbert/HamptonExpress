const cosmos = require("@azure/cosmos");
const CosmosClient = cosmos.CosmosClient;
const config = require("./config");
const databaseId = 'Hampton';
const util = require('util');
const endpoint = config.connection.endpoint;
const masterKey = config.connection.authKey;
const {parse, stringify} = require('flatted/cjs');

const client = new CosmosClient({ endpoint, auth: { masterKey } });

async function getReadings(req, res, next){
  const {database, readings, patients} = await init();
  const querySpec = {
    query: "SELECT * FROM readings where readings.days_before <= @start and readings.days_before >= @end",
    parameters: [
      {
        name: "@start",
        value: req.query.start?parseInt(req.query.start):1000
      },
      {
        name: "@end",
        value: req.query.end?parseInt(req.query.end):0
      }

    ]
  };
  const { result: itemDefList } = await readings.items.query(querySpec, { enableCrossPartitionQuery: true }).toArray();
  var ids = []
  for (var i in itemDefList ) {
    const reading = itemDefList[i]
    console.log(reading["patient_id"])
    if (!ids.includes(reading["patient_id"])) {
      ids.push(reading["patient_id"])
    }
  }
  var ret = [];
  for (var id in ids){
    ret.push({
      id:ids[id],
      readings:itemDefList.filter(reading => reading['patient_id'] == ids[id])
    })
  }
  res.status(200)
      .json({
        patients:ret
      })
}

async function getColumns(req, res, next){
  const {database, readings, patients} = await init();
  const querySpec = {
    query: "SELECT top 1 * FROM patients"
  };
  const { result: itemDefList } = await patients.items.query(querySpec, { enableCrossPartitionQuery: true }).toArray();
  res.status(200)
      .json({
        columns:Object.keys(itemDefList[0])
      })
}


async function getColumnsName(req, res, next){
  console.log("FGETITNG ICOLU")
  const {database, readings, patients} = await init();
  var column = req.query.column_name?req.query.column_name:"ObstetricHistory_AddedDate"
  const querySpec = {
    query: "SELECT patients.Unique_ID, "+ "patients." + column + " FROM patients"
  };

    const { result: itemDefList } = await patients.items.query(querySpec, { enableCrossPartitionQuery: true }).toArray();

  const key = req.query.column_name?req.query.column_name:"Unique_ID"
  res.status(200)
      .json({
        key:itemDefList
        })
}



async function importReadings(req, res, next){
  var newReadings = req.body["patients"]
  const {database, readings, patients} = await init();
  console.log(newReadings)

  var dataToAdd = []

  for (var i in newReadings) {
    var id = newReadings[i]["id"]
    for (var j in newReadings[i]["readings"]){
      newReadings[i]["readings"][j]["patient_id"] = id
      newReadings[i]["readings"][j]["partitionKey"] = id + "-" + newReadings[i]["readings"][j]["days_before"]
      dataToAdd.push(newReadings[i]["readings"][j])
    }
  }

  for(var i in dataToAdd){
    const { body: upserted } = await readings.items.upsert(dataToAdd[i])
  }

  res.status(200)
      .json({
        status:"success"
      })

}


async function importPatients(req, res, next){
  var newPatients = req.body["patients"]
  const {database, readings, patients} = await init();

  for(var i in newPatients){
    const { body: upserted } = await patients.items.upsert(newPatients[i])
  }

  res.status(200)
      .json({
        status:"success"
      })

}

async function init(){
  const database = await client.databases.createIfNotExists({ id: databaseId });
  const readings = await database.database.container('readings');
  const patients = await database.database.container('patients');
  return {database, readings, patients}
}


module.exports = {
  getReadings: getReadings,
  importReadings: importReadings,
  importPatients: importPatients,
  getColumns: getColumns,
  getColumnsName: getColumnsName
}
