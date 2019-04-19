const cosmos = require("@azure/cosmos");
const CosmosClient = cosmos.CosmosClient;
const config = require("./config");
const databaseId = 'Hampton';
const util = require('util');
var moment = require('moment');
const endpoint = config.connection.endpoint;
const masterKey = config.connection.authKey;
const {
  parse,
  stringify
} = require('flatted/cjs');

const client = new CosmosClient({
  endpoint,
  auth: {
    masterKey
  }
});

async function getReadings(req, res, next) {
  const {
    database,
    readings,
    patients,
    users
  } = await init();

  if (req.query.start && req.query.end && parseInt(req.query.start) < parseInt(req.query.end)) {
    res.status(400).json({
      message: "end must be less than start"
    })
  }

  var org = await getOrg(req.user.id)

  if (!req.query.column_name) {

    var maxDates = await calculateDaysBefore(org)
    var querys = ""
    Object.keys(maxDates).forEach(patient_id => {
      var start = moment(maxDates[patient_id]).subtract(req.query.start ? parseInt(req.query.start) : 1000, 'days').format('YYYY-MM-DD')
      var end = moment(maxDates[patient_id]).subtract(req.query.end ? parseInt(req.query.end) : 0, 'days').format('YYYY-MM-DD')
      querys += '(readings.date >= "' + start + '" and readings.date <= "' + end + '" and readings.organisation="' + org + '" and readings.patient_id="' + patient_id + '") OR '
    })
    querys = querys.substring(0, querys.length - 3)

    const querySpec = {
      query: 'SELECT * FROM readings WHERE ' + querys
    };
    const {
      result: itemDefList
    } = await readings.items.query(querySpec, {
      enableCrossPartitionQuery: true
    }).toArray();
    var ids = []
    for (var i in itemDefList) {
      const reading = itemDefList[i]
      if (!ids.includes(reading["patient_id"])) {
        ids.push(reading["patient_id"])
      }
    }
    var ret = [];
    for (var id in ids) {
      ret.push({
        id: ids[id],
        readings: itemDefList.filter(reading => reading['patient_id'] == ids[id]).map(item => {
          item.days_before = (moment(maxDates[item.patient_id]).diff(moment(item.date), 'days'))
          return item
        })
      })
    }
    res.status(200)
      .json({
        patients: ret
      })
  } else {

    var columns = await getColumnsHelper(org)
    var breakpoint = req.query.breakpoint ? req.query.breakpoint : 1

    if (!columns.includes(req.query.column_name)) {
      res.status(400).json({
        message: "column_name " + req.query.column_name + " does not exist"
      })
    }
    const patientYesQuery = {
      query: "SELECT p.Unique_ID from p where p." + req.query.column_name + " >= " + breakpoint
    }



    const {
      result: patientYesList
    } = await patients.items.query(patientYesQuery, {
      enableCrossPartitionQuery: true
    }).toArray();
    const patientNoQuery = {
      query: "SELECT p.Unique_ID from p where p." + req.query.column_name + "< " + breakpoint
    }

    const {
      result: patientNoList
    } = await patients.items.query(patientNoQuery, {
      enableCrossPartitionQuery: true
    }).toArray();
    var yesList = patientYesList.map(patient => patient["Unique_ID"])

    var noList = patientNoList.map(patient => patient["Unique_ID"])

    var yesQueries = ""
    var noQueries = ""
    var maxDates = await calculateDaysBefore(org)
    Object.keys(maxDates).forEach(patient_id => {
      var start = moment(maxDates[patient_id]).subtract(req.query.start?parseInt(req.query.start):1000, 'days').format('YYYY-MM-DD')
      var end =  moment(maxDates[patient_id]).subtract(req.query.end?parseInt(req.query.end):0, 'days').format('YYYY-MM-DD')
      if(yesList.includes(parseInt(patient_id))){
          yesQueries += '(readings.date >= "' + start + '" and readings.date <= "' + end + '" and readings.organisation="' + org + '" and readings.patient_id="' + patient_id + '") OR '
      }else{
          noQueries += '(readings.date >= "' + start + '" and readings.date <= "' + end + '" and readings.organisation="' + org + '" and readings.patient_id="' + patient_id + '") OR '
      }
    })

      if(yesQueries.length > 3){
        yesQueries= " OR " + yesQueries.substring(0, yesQueries.length - 3)
      }
      if(noQueries.length > 3){
        noQueries= " OR " + noQueries.substring(0, noQueries.length - 3)
      }


    const queryYesSpec = {
      query: 'SELECT * FROM readings where false ' + yesQueries
    };

    console.log(queryYesSpec.query)

    const {
      result: itemYesList
    } = await readings.items.query(queryYesSpec, {
      enableCrossPartitionQuery: true
    }).toArray();


  const queryNoSpec = {
    query: 'SELECT * FROM readings where false ' + noQueries
  };

    const {
      result: itemNoList
    } = await readings.items.query(queryNoSpec, {
      enableCrossPartitionQuery: true
    }).toArray();


    var yesIds = []
    for (var i in itemYesList) {
      const reading = itemYesList[i]
      if (!yesIds.includes(reading["patient_id"])) {
        yesIds.push(reading["patient_id"])
      }
    }
    var yesRet = [];
    for (var id in yesIds) {
      var retObj = {
        id: yesIds[id],
        readings: itemYesList.filter(reading => reading['patient_id'] == yesIds[id]).map(item => {
          item.days_before = (moment(maxDates[item.patient_id]).diff(moment(item.date), 'days'))
          return item
        })
      }

      yesRet.push(retObj)
    }


    var noIds = []
    for (var i in itemNoList) {
      const reading = itemNoList[i]
      if (!noIds.includes(reading["patient_id"])) {
        noIds.push(reading["patient_id"])
      }
    }
    var noRet = [];
    for (var id in noIds) {
      var retObj = {
        id: noIds[id],
        readings: itemNoList.filter(reading => reading['patient_id'] == noIds[id]).map(item => {
          item.days_before = (moment(maxDates[item.patient_id]).diff(moment(item.date), 'days'))
          return item
        })
      }


      noRet.push(retObj)
    }
    res.status(200)
      .json({
        yes_patients: yesRet,
        no_patients: noRet
      })
  }
}

async function getColumns(req, res, next) {
  var org = await getOrg(req.user.id)
  const {
    database,
    readings,
    patients,
    users
  } = await init();
  const querySpec = {
    query: 'SELECT top 1 * FROM patients where patients.organisation="' + org + '"'
  };
  const {
    result: itemDefList
  } = await patients.items.query(querySpec, {
    enableCrossPartitionQuery: true
  }).toArray();
  var keys = Object.keys(itemDefList[0])
  ret = {}
  for (var i in keys) {
    if (itemDefList[0][keys[i]] == 1 || itemDefList[0][keys[i]] == 0) {
      ret[keys[i]] = "boolean"
    } else {
      ret[keys[i]] = typeof itemDefList[0][keys[i]]
    }
  }

  res.status(200)
    .json({
      columns: ret
    })
}

async function getColumnsHelper(org) {
  const {
    database,
    readings,
    patients,
    users
  } = await init();
  const querySpec = {
    query: 'SELECT top 1 * FROM patients where patients.organisation="' + org + '"'
  };
  const {
    result: itemDefList
  } = await patients.items.query(querySpec, {
    enableCrossPartitionQuery: true
  }).toArray();
  return Object.keys(itemDefList[0])
}

// async function importReadingsHelper(){
//   var org = await getOrg(req.user.id)
//   const {database, readings, patients, users} = await init();
//   const querySpec = {
//     query: 'SELECT readings.partitionKey FROM readings where readings.organisation="' + org + '"',
//   };
//   const { result: itemDefList } = await readings.items.query(querySpec, { enableCrossPartitionQuery: true }).toArray();
//   return itemDefList.map(pkey => pkey["partitionKey"])
// }

async function getColumnsName(req, res, next) {
  var org = await getOrg(req.user.id)
  const {
    database,
    readings,
    patients,
    users
  } = await init();
  var column = req.query.column_name

  if (!req.query.column_name) {

    res.status(400)

      .json({
        message: "column_name missing"
      })
  }
  const querySpec = {
    query: "SELECT patients.Unique_ID, " + "patients." + column + ' FROM patients where patients.organisation="' + org + '"'
  };

  const {
    result: itemDefList
  } = await patients.items.query(querySpec, {
    enableCrossPartitionQuery: true
  }).toArray();

  const key = req.query.column_name ? req.query.column_name : "Unique_ID"
  var retObj = {}
  retObj[key] = itemDefList
  res.status(200)

    .json(retObj)
}

async function importReadings(req, res, next) {



  var org = await getOrg(req.user.id)
  if (!req.body["patients"]) {
    res.status(400).json({
      message: "missing column patients"
    })
    return
  }
  var newReadings = req.body["patients"]

  //  var currentReadings = await  importReadingsHelper();

  const {
    database,
    readings,
    patients,
    users
  } = await init();
  var dataToAdd = []
  var dataToUpdate = []

  for (var i in newReadings) {
    var id = newReadings[i]["id"]
    for (var j in newReadings[i]["readings"]) {
      if (!newReadings[i]["readings"][j]["date"] ||
        !newReadings[i]["id"]) {
        res.status(400).json({
          message: " data improperlly formatted on for item number " + i + " reading number " + j
        })
        return
      }
      //if(!currentReadings.includes(id + "-" + newReadings[i]["readings"][j]["days_before"])){
      newReadings[i]["readings"][j]["patient_id"] = id
      newReadings[i]["readings"][j]["organisation"] = org
      newReadings[i]["readings"][j]["days_before"]
      newReadings[i]["readings"][j]["unique_key"] = id + "-" + newReadings[i]["readings"][j]["date"] + "-" + org
      dataToAdd.push(newReadings[i]["readings"][j])
      // }else{
      //   dataToUpdate.push(newReadings[i]["readings"][j])
      // }
    }
  }

  for (var i in dataToAdd) {
    try {
      const {
        body: upserted
      } = await readings.items.upsert(dataToAdd[i])
    } catch (e) {
      console.log(e)
    }
  }
  // for(var i in dataToAdd){
  //   const { body: upserted } = await readings.items.up(dataToAdd[i])
  // }

  res.status(200)
    .json({
      status: "success"
    })

}

async function getPatient(req, res, next) {
  var org = await getOrg(req.user.id)
  var maxDates = await calculateDaysBefore(org)
  const {
    database,
    readings,
    patients,
    users
  } = await init();
  var id = req.query.id
  const queryMetrics = {
    query: 'SELECT * FROM patients where patients.Unique_ID=' + id + ' and patients.organisation="' + org + '"'
  };
  const queryReadings = {
    query: 'select * from readings where readings.patient_id="' + id + '"' + ' and readings.organisation="' + org + '"'
  }

  const {
    result: metricList
  } = await patients.items.query(queryMetrics, {
    enableCrossPartitionQuery: true
  }).toArray();
  const {
    result: readingsList
  } = await readings.items.query(queryReadings, {
    enableCrossPartitionQuery: true
  }).toArray()

  if (metricList.length > 0) {
    metricList[0]["readings"] = readingsList.map(item => {
      item.days_before = (moment(maxDates[item.patient_id]).diff(moment(item.date), 'days'))
      return item
    });
    res.status(200)

      .json(metricList[0])
  } else {
    res.status(400)
      .json({
        message: "id does not exist"
      })
  }
}


async function getPatients(req, res, next) {

  var org = await getOrg(req.user.id)

  const {
    database,
    readings,
    patients,
    users
  } = await init();
  var id = req.query.id
  const queryMetrics = {
    query: 'SELECT * FROM patients where patients.organisation="' + org + '"'
  };


  const {
    result: metricList
  } = await patients.items.query(queryMetrics, {
    enableCrossPartitionQuery: true
  }).toArray();

  if (metricList.length > 0) {

    res.status(200)

      .json({
        patients: metricList
      })
  } else {
    res.status(400)
      .json({
        message: "no patients exist"
      })
  }
}

async function importPatients(req, res, next) {
  var org = await getOrg(req.user.id)
  var newPatients = req.body["patients"]
  var skipped = []


  const {
    database,
    readings,
    patients,
    users
  } = await init();

  for (var i in newPatients) {
    const querySpec = {
      query: 'SELECT * FROM patients where patients.Unique_ID=' + newPatients[i]["Unique_ID"] + ' and patients.organisation="' + org + '"',
    };
    const {
      result: patient
    } = await patients.items.query(querySpec, {
      enableCrossPartitionQuery: true
    }).toArray();
    if (patient.length > 0) {
      skipped.push(newPatients[i].Unique_ID)
    } else {
      newPatients[i]["organisation"] = org
      newPatients[i]["unique_key"] = newPatients[i].Unique_ID + '-' + org
      try {
        const {
          body: upserted
        } = await patients.items.upsert(newPatients[i])
      } catch (e) {
        console.log(e)
      }
    }
  }

  res.status(200)
    .json({
      status: "success",
      skipped: skipped
    })

}

async function getOrg(user) {
  const {
    database,
    readings,
    patients,
    users
  } = await init();
  const querySpec = {
    query: "SELECT users.organisation FROM users where users.id=@id",
    parameters: [{
        name: "@id",
        value: user
      }

    ]
  };
  const {
    result: org
  } = await users.items.query(querySpec, {
    enableCrossPartitionQuery: true
  }).toArray();
  return org[0]["organisation"]
}


async function updatePatient(req, res, next) {

}

async function deletePatient(req, res, next) {
  var org = await getOrg(req.user.id)
  var patient_id = req.body["id"]
  const {
    database,
    readings,
    patients,
    users
  } = await init();
  const querySpec = {
    query: 'SELECT patients.id FROM patients where patients.Unique_ID=' + patient_id + ' and patients.organisation="' + org + '"',
  };

  const queryReadings = {
    query: 'SELECT readings.id FROM readings where readings.patient_id="' + patient_id + '" and readings.organisation="' + org + '"',
  };

  const {
    result: patient
  } = await patients.items.query(querySpec, {
    enableCrossPartitionQuery: true
  }).toArray();
  const {
    result: patientReadings
  } = await readings.items.query(queryReadings, {
    enableCrossPartitionQuery: true
  }).toArray();
  console.log(patientReadings)
  if (patient.length > 0) {
    const {
      result: deleted
    } = await patients.item(patient[0].id, org).delete()
  }
  for (var i in patientReadings) {
    console.log(patientReadings[i])
    await readings.item(patientReadings[i].id, org).delete()
  }
  res.status(200)
    .json({
      status: 'success'
    })
}

async function updateReadings(req, res, next) {

}

async function calculateDaysBefore(org) {

  const container = client.database("Hampton").container("readings");
  const sprocId = "cube";

  var filterQuery = 'select * from readings';
  var cubeConfig = {
    groupBy: 'id',
    field: 'date',
    f: 'max'
  };
  var memo = {
    cubeConfig: {
      groupBy: 'patient_id',
      field: 'date',
      f: 'max'
    },
    filterQuery: 'select * from readings where readings.organisation="' + org + '"'
  };

  const {
    body: result
  } = await container.storedProcedure(sprocId).execute(memo, {
    partitionKey: org
  })
  //console.table(result.savedCube.cellsAsCSVStyleArray)
  var minDates = {}
  result.savedCube.cellsAsCSVStyleArray.forEach(cell => {
    if (parseInt(cell[0])) {
      minDates[cell[0]] = cell[2]
    }
  })
  return minDates
}


async function login(req, res, next) {
  const {
    database,
    readings,
    patients,
    users
  } = await init();
  const querySpec = {
    query: "SELECT users.token FROM users where users.username=@username and users.password=@password",
    parameters: [{
        name: "@username",
        value: req.body["username"]
      },
      {
        name: "@password",
        value: req.body["password"]
      }

    ]
  };
  const {
    result: token
  } = await users.items.query(querySpec, {
    enableCrossPartitionQuery: true
  }).toArray();

  if (token.length > 0) {
    res.status(200)
      .json({
        user: token[0]
      })
  } else {
    res.status(401)
      .json({
        message: "unauthorized"
      })
  }

}

async function init() {
  const database = await client.databases.createIfNotExists({
    id: databaseId
  });
  const readings = await database.database.container('readings');
  const patients = await database.database.container('patients');
  const users = await database.database.container('users');
  return {
    database,
    readings,
    patients,
    users
  }
}


module.exports = {
  getReadings: getReadings,
  importReadings: importReadings,
  importPatients: importPatients,
  getColumns: getColumns,
  getColumnsName: getColumnsName,
  getPatient: getPatient,
  getPatients: getPatients,
  login: login,
  deletePatient: deletePatient,
  updateReadings: updateReadings,
  updatePatient: updatePatient,
  calculateDaysBefore: calculateDaysBefore,

}
