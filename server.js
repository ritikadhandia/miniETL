const express = require('express')
const path = require('path')
var formidable = require('formidable');
var http = require('http');
var https = require('https');
const bodyParser = require('body-parser');
var fs = require('fs');
const util = require('util');
const csv = require('fast-csv');
const dotenv = require('dotenv');
dotenv.config();
const jsforce = require('jsforce');
const sfbulk = require('node-sf-bulk2');
const characters  = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
const pollFreqency = process.env.pollfrequency || 10000;
const PORT = process.env.PORT || 5000

const outputFile = __dirname + '/output.csv';
const failedResultsFile = __dirname + '/failedResults.csv';

// Global variables to be used between calls
var csvFilePath;
var csvString;
var bulkconnect;
var objectName;

var app = express();
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static(path.join(__dirname, 'public')));
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.listen(PORT, () => console.log(`Listening on ${ PORT }`));

// Base Core Function
// Fetch Inventory and Menu for Machine Id
app.get('/', function(req, resp){
    
    //testFailedResults();
    var headersData = {
      "headers":[]
    }
    resp.render('pages/main', {
      headerData: JSON.stringify(headersData)
    });
});

app.get('/downloadFile', (req, res) => {

  res.download(outputFile, 'output.csv', function(err){
      /*
      fs.unlink(outputFile, function(err){
        console.log('File Removed');
      });
      */
  });
})

app.get('/downloadFailedFile', (req, res) => {

  res.download(failedResultsFile, 'failedResults.csv', function(err){
      /*
      fs.unlink(outputFile, function(err){
        console.log('File Removed');
      });
      */
  });

});

app.post('/fetchJobStatus', function(req, res){

  var jobId = req.body.jobId;
  loginProcess()
  .then(function(){
    console.log('login finished');
    return getJobStatus(jobId);
  })
  .then(function(response){
    console.log(response);
    res.status(200).json({ status: 'success', response : response });
  })
  .catch(function (error) {
    console.log(error);
    res.status(200).json({ status: 'error', error : error });
  });


});

app.post('/fetchFailedResults', function(req, res){

  var jobId = req.body.jobId;

  const writeStream = fs.createWriteStream(failedResultsFile);
  writeStream.on("finish", function(){
    res.status(200).json({ a: 1 });
  });

  const transform = csv.format({ headers: true })
    .transform((row) => {
      return row;
  });

  loginProcess()
  .then(function(){
    console.log('login finished');
    return getFailedResults(jobId);
  })
  .then(function(responseString){
    csv.parseString(responseString, { headers: true })
    .pipe(transform)
    .pipe(writeStream);
      
  })
  .catch(function (error) {
    console.log(error);
    res.status(200).json({ status: 'error', error : error });
  });

});

app.post('/uploadSF', function(req, res){

  var opType = req.body.operationType;

  loginProcess()
  .then(function(){
    console.log('login finished');
    return submitBulkUploadJob(opType)
  })
  .then(function(jobId){
    fs.unlink(outputFile, function(err){
      console.log('File Removed');
    });
    console.log('upload job started with job id '+jobId);
    res.status(200).json({ jobId: jobId});
  })
  .catch(function (error) {
    res.status(200).json({ status: 'error', error : error });
  });
})

app.post('/transformFile', function(req, res){

    var columnParams = req.body.data;
    const writeStream = fs.createWriteStream(outputFile);
    writeStream.on("finish", function(){
      res.status(200).json({ a: 1 });
    });

    const transform = csv.format({ headers: true })
    .transform((row) => {
      
      let returnVal = {};
      Object.keys(columnParams).forEach(function(key){
     
        if(columnParams[key] != 'Exclude'){
          var a = row[key]?row[key]:'';
          returnVal[key] = transformData(columnParams[key], a);        }
     
      });
      return returnVal;
    });

    if(csvFilePath){
      const parse = csv.parse({ headers: true });
      const stream = fs.createReadStream(csvFilePath)
      .pipe(parse)
      .pipe(transform)
      .pipe(writeStream);
    }
    else if(csvString){
      csv.parseString(csvString, { headers: true })
      .pipe(transform)
      .pipe(writeStream);
  }

});

function transformData(type, a){

    switch (type){
      case 'Jumble Up Text':
          a = scramble(a);
          break;
      case 'Jumble Up Email':
          a = scrambleEmail(a);
          break;
      case 'Random Phone':
          a = randomPhone();
          break;
      case 'Random Past Date':
          a = randomPastDate(a);
          break;
      case 'Random Future Date':
          a = randomFutureDate(a);
          break;
      case 'Random Past Date Time':
          a = randomPastDateTime(a);
          break;
      case 'Clear':
          a = ''
          break;
      case 'No Change':
          break;
      default:
    }
    return a;
}

function getRandomDate(start, end) {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

function randomPastDateTime(a){

  if(a && a != ''){
    const d = new Date(a);
    if(d instanceof Date && !isNaN(d)){
      d.setDate(d.getDate()-10);
      var randomDate = getRandomDate(new Date(1950, 0, 1), d);
      a = randomDate.toISOString();
    }
  }
  return a;
  
}

function randomPastDate(a){
  if(a && a != ''){
    const d = new Date(a);
    if(d instanceof Date && !isNaN(d)){
      d.setDate(d.getDate()-10);
      var randomDate = getRandomDate(new Date(1950, 0, 1), d);
      a = randomDate.toISOString().split('T')[0];
    }
  }
  return a;

}

function randomFutureDate(a){

    if(a && a != ''){
      const d = new Date(a);
      if(d instanceof Date && !isNaN(d)){
        d.setDate(d.getDate()+10);
        var randomDate = getRandomDate(d, new Date(2050, 0, 1));
        a = randomDate.toISOString().split('T')[0];
      }
    }
    return a;

}

function randomPhone(a){
  if(a && a != ''){
    return Math.floor(Math.random()*(899)+100)+"-"+Math.floor(Math.random()*(899)+100)+"-"+Math.floor(Math.random()*(8999)+1000);
  }
  return a;
}

function randomText(cnt){

    var charactersLength = characters.length;
    var result = '';
    for ( var i = 0; i < cnt; i++ ) {
      result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    return result;
}

function scramble(a){
  a=a.split("");
  for(var b=a.length-1;0<b;b--){
      var c=Math.floor(Math.random()*(b+1));
      d=a[b];a[b]=a[c];a[c]=d
  }
  return a.join("")
}

function scrambleEmail(a){

  if(isEmail(a)){
    a=a.split("@");
    a[0] = scramble(a[0]);
    a = [...a[0], "@", a[1]];
    return a.join("");
  }
  return a;
}

function isEmail(a){
    var pattern = /^\w+@[a-zA-Z_]+?\.[a-zA-Z]{2,3}$/; 
    return a.match(pattern);    
}


app.post('/uploadFile', function(req, res){

  console.log('Data Ex started');
  var form = new formidable.IncomingForm();
  
  form.parse(req, function (err, fields, files) {

    objectName = fields.objectName;

    if(files.csvFile.name != ''){
      csvFilePath = files.csvFile.path;
      processCSVFileHeader(csvFilePath)
      .then(function(data){
        
        res.render('pages/main', {
          headerData: JSON.stringify(data)
        });

      })
    }

    else if(fields.queryText != ''){
      
      loginProcess()
      .then(function(){
        console.log('login finished');
        return submitBulkQueryJob(fields.queryText)
      })
      .then(function(jobId){
        console.log('bulk query job submitted');
        return bulkStatusRecursive(jobId);
      })
      .then(function(data){
        console.log('bulk query job finished');
        return processCSVStringHeader(data);
      })
      .then(function(data){
        res.render('pages/main', {
          headerData: JSON.stringify(data)
        });
      })
      .catch(function(err){
        res.render('pages/result', {
          resultText: JSON.stringify(err)
        });
      })
    }

  });

});

function processCSVFileHeader(filePath){

  return new Promise(function(resolve, reject){

      const parse = csv.parse({ headers: true });
      var headersDataResult = {};
      parse.on('headers', function(headers){
          headersDataResult.headers = headers;
      });

      parse
      .on('error', error => {
        reject(error);
      })
      .on('data', row => {})
      .on('end', rowCount => {

        console.log(`Parsed ${rowCount} rows`);
        headersDataResult.rowCount = rowCount;
        resolve(headersDataResult);

      });

      const stream = fs.createReadStream(filePath)
      .pipe(parse);

  });

}

function processCSVStringHeader(data){

  return new Promise(function(resolve, reject){

    var headersDataResult = {};
    csv.parseString(data, { headers: true })
      .on('headers', function(headers){
          headersDataResult.headers = headers;
      })
      .on('error', error => {
        reject(error)
      })
      .on('data', row => {})
      .on('end', rowCount => {
        
        console.log(`Parsed ${rowCount} rows`);
        headersDataResult.rowCount = rowCount;
        resolve(headersDataResult);
      });
  });

}

async function submitBulkQueryJob(queryText){

  try {
        const bulkapi2 = new sfbulk.BulkAPI2(bulkconnect);
        const queryInput = {
            'query': queryText,
            'operation': 'query'
        };
        const response = await bulkapi2.submitBulkQueryJob(queryInput);
        return Promise.resolve(response.id);

    } catch (ex) {
        return Promise.reject(ex);       
    }
}

async function submitBulkUploadJob(operationType){
  try {
      console.log('bulk upload started');
      // create a new BulkAPI2 class
      const bulkrequest = new sfbulk.BulkAPI2(bulkconnect);
      // create a bulk update job
      const jobRequest = {
          'object': objectName,
          'operation': operationType
      };
      const response = await bulkrequest.createDataUploadJob(jobRequest);
      if (response.id) {
          console.log('upload job created' + response.id);
          // read csv data from the local file system
          const data = await util.promisify(fs.readFile)(outputFile, "UTF-8");
          const status = await bulkrequest.uploadJobData(response.contentUrl, data);
          if (status === 201) {
              // close the job for processing
              await bulkrequest.closeOrAbortJob(response.id, 'UploadComplete');
              console.log('upload job subimtted' + response.id);
              return Promise.resolve(response.id);
          }
      }
      else{
        return Promise.reject('Object name not Present');  
      }
  } catch (ex) {
      return Promise.reject(ex);
  }
}

async function getJobStatus(jobId){

  const bulkrequest = new sfbulk.BulkAPI2(bulkconnect);
  const response = await bulkrequest.getIngestJobInfo(jobId);
  console.log(JSON.stringify(response));
  console.log(JSON.parse(JSON.stringify(response)))
  return Promise.resolve(JSON.parse(JSON.stringify(response)));

}


async function getFailedResults(jobId){

  const bulkrequest = new sfbulk.BulkAPI2(bulkconnect);
  const response = await bulkrequest.getResults(jobId, 'failedResults');
  return Promise.resolve(response);

}

async function loginProcess(){

  console.log('login process started');
  if (process.env.username && process.env.password) {
      const conn = new jsforce.Connection({
        loginUrl : process.env.loginurl
      });
      await conn.login(process.env.username, process.env.password);
      bulkconnect = {
            'accessToken': conn.accessToken,
            'apiVersion': '51.0',
            'instanceUrl': conn.instanceUrl
      };
      return Promise.resolve();
  }
  else {
      return Promise.reject('Incorrect Credentials');  
  }
}


function asyncThing( asyncParam) { // example operation
  const promiseDelay = (data,msec) => new Promise(res => setTimeout(res,msec,data));
  return promiseDelay( asyncParam, pollFreqency); //resolve with argument in 10 second.
}

function bulkStatusRecursive( jobId ) { // example "recursive" asynchronous function

    console.log('polling SF bulk status');
    
    async function decide( jobId) {

        const bulkapi2 = new sfbulk.BulkAPI2(bulkconnect);
        const response = await bulkapi2.getBulkQueryJobInfo(jobId);

        if(response.state == 'InProgress' || response.state == 'UploadComplete' ){
            return bulkStatusRecursive(jobId);
        }
        else if(response.state == 'JobComplete'){
            const result = await bulkapi2.getBulkQueryResults(jobId);
            csvString = result.data;
            return result.data;
        }
    }

    // Return a promise resolved by doing something async and deciding what to do.
    // to be clear the returned promise is the one returned from the .then call

    return asyncThing(jobId).then(decide);
}


