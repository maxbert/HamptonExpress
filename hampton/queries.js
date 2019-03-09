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

  if(req.query.start && req.query.end && parseInt(req.query.start) < parseInt(req.query.end)){
    res.status(400).json({message:"end must be less than start"})
  }

  if(!req.query.column_name){
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
    }else{

      var columns = await getColumnsHelper()
      var breakpoint = req.query.breakpoint?req.query.breakpoint:1

      if(!columns.includes(req.query.column_name)){
        res.status(400).json({message:"column_name " + req.query.column_name + " does not exist"})
      }
      const patientYesQuery = {
        query: "SELECT p.Unique_ID from p where p." + req.query.column_name + " >= " + breakpoint
      }


      const { result: patientYesList } = await patients.items.query(patientYesQuery, { enableCrossPartitionQuery: true }).toArray();
      const patientNoQuery = {
        query: "SELECT p.Unique_ID from p where p." + req.query.column_name + "< " + breakpoint
      }

      const { result: patientNoList } = await patients.items.query(patientNoQuery, { enableCrossPartitionQuery: true }).toArray();
      var yesList=patientYesList.map(patient => '"' + patient["Unique_ID"] +'"').join(",")

      var noList=patientNoList.map(patient => '"' + patient["Unique_ID"] + '"').join(",")

      const queryYesSpec = {
        query: "SELECT * FROM readings where readings.days_before <= @start and readings.days_before >= @end and readings.patient_id in (" + yesList + ")",
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

      const queryNoSpec = {
        query: "SELECT * FROM readings where readings.days_before <= @start and readings.days_before >= @end and readings.patient_id in (" + noList + ")",
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
      const { result: itemYesList } = await readings.items.query(queryYesSpec, { enableCrossPartitionQuery: true }).toArray();

      const { result: itemNoList } = await readings.items.query(queryNoSpec, { enableCrossPartitionQuery: true }).toArray();
      var yesIds = []
      for (var i in itemYesList ) {
        const reading = itemYesList[i]
        if (!yesIds.includes(reading["patient_id"])) {
          yesIds.push(reading["patient_id"])
        }
      }
      var yesRet = [];
      for (var id in yesIds){
        var retObj = {
          id:yesIds[id],
          readings:itemYesList.filter(reading => reading['patient_id'] == yesIds[id])
        }

        yesRet.push(retObj)
      }


      var noIds = []
      for (var i in itemNoList ) {
        const reading = itemNoList[i]
        if (!noIds.includes(reading["patient_id"])) {
          noIds.push(reading["patient_id"])
        }
      }
      var noRet = [];
      for (var id in noIds){
        var retObj = {
          id:noIds[id],
          readings:itemNoList.filter(reading => reading['patient_id'] == noIds[id])
        }


        noRet.push(retObj)
      }
      res.status(200)
          .json({
            yes_patients:yesRet,
            no_patients:noRet
          })
    }
}

async function getColumns(req, res, next){
  const {database, readings, patients} = await init();
  const querySpec = {
    query: "SELECT top 1 * FROM patients"
  };
  const { result: itemDefList } = await patients.items.query(querySpec, { enableCrossPartitionQuery: true }).toArray();
  var keys = Object.keys(itemDefList[0])
  ret = {}
  for(var i in keys){
    ret[keys[i]] = typeof itemDefList[0][keys[i]]
  }

  res.status(200)
      .json({
        columns:ret
      })
}

async function getColumnsHelper(){
  const {database, readings, patients} = await init();
  const querySpec = {
    query: "SELECT top 1 * FROM patients"
  };
  const { result: itemDefList } = await patients.items.query(querySpec, { enableCrossPartitionQuery: true }).toArray();
  return Object.keys(itemDefList[0])
}

async function importReadingsHelper(){
  const {database, readings, patients} = await init();
  const querySpec = {
    query: "SELECT readings.partitionKey FROM readings",
  };
  const { result: itemDefList } = await readings.items.query(querySpec, { enableCrossPartitionQuery: true }).toArray();
  return itemDefList.map(pkey => pkey["partitionKey"])
}

async function getColumnsName(req, res, next){
  const {database, readings, patients} = await init();
  var column = req.query.column_name

  if(!req.query.column_name){

    res.status(400)

        .json({message:"column_name missing"})
  }
  const querySpec = {
    query: "SELECT patients.Unique_ID, "+ "patients." + column + " FROM patients"
  };

    const { result: itemDefList } = await patients.items.query(querySpec, { enableCrossPartitionQuery: true }).toArray();

  const key = req.query.column_name?req.query.column_name:"Unique_ID"
  var retObj = {}
  retObj[key]= itemDefList
  res.status(200)

      .json(retObj)
}

async function importReadings(req, res, next){
  if(!req.body["patients"]){
    res.status(400).json({message:"missing column patients"})
    return
  }
  var newReadings = req.body["patients"]

//  var currentReadings = await  importReadingsHelper();

  const {database, readings, patients} = await init();
  var dataToAdd = []
  var dataToUpdate = []

  for (var i in newReadings) {
    var id = newReadings[i]["id"]
    for (var j in newReadings[i]["readings"]){
      if(!Number.isInteger(newReadings[i]["readings"][j]["sbp"])
       || !Number.isInteger(newReadings[i]["readings"][j]["dbp"])
       || !Number.isInteger(newReadings[i]["readings"][j]["days_before"])
       || !newReadings[i]["readings"][j]["date"]
       || !newReadings[i]["id"]){
        res.status(400).json({message:" data improperlly formatted on for item number " + i + " reading number " + j })
        return
      }
      //if(!currentReadings.includes(id + "-" + newReadings[i]["readings"][j]["days_before"])){
      newReadings[i]["readings"][j]["patient_id"] = id
      newReadings[i]["readings"][j]["partitionKey"] = id + "-" + newReadings[i]["readings"][j]["days_before"]
      dataToAdd.push(newReadings[i]["readings"][j])
    // }else{
    //   dataToUpdate.push(newReadings[i]["readings"][j])
    // }
    }
  }

  for(var i in dataToAdd){
    try{
      const { body: upserted } = await readings.items.upsert(dataToAdd[i])
    }catch(e){
      console.log(e)
    }
  }
  // for(var i in dataToAdd){
  //   const { body: upserted } = await readings.items.up(dataToAdd[i])
  // }

  res.status(200)
      .json({
        status:"success"
      })

}

async function getPatient(req, res, next){

  const {database, readings, patients} = await init();
  var id = req.query.id
  const queryMetrics = {
    query: 'SELECT * FROM patients where patients.Unique_ID=' + id
  };
  const queryReadings = {
    query: 'select * from readings where readings.patient_id="' + id + '"'
  }

  const { result: metricList } = await patients.items.query(queryMetrics, { enableCrossPartitionQuery: true }).toArray();
  const { result: readingsList } = await readings.items.query(queryReadings, { enableCrossPartitionQuery: true }).toArray();

  if(metricList.length > 0){
  metricList[0]["readings"]= readingsList
  res.status(200)

      .json(metricList[0])
    }else{
      res.status(400)
          .json({message:"id does not exist"})
    }
}

async function importPatients(req, res, next){
  var newPatients = req.body["patients"]
  const {database, readings, patients} = await init();

  for(var i in newPatients){
    try{
      const { body: upserted } = await patients.items.upsert(newPatients[i])
    }catch(e){
      console.log(e)
    }
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
  getColumnsName: getColumnsName,
  getPatient: getPatient,
}
