const express = require('express')
const path = require('path')
var formidable = require('formidable');
var http = require('http');
var https = require('https');
const bodyParser = require('body-parser');
const multer = require('multer');
const upload = multer({ dest: './public/uploads/' });
var fs = require('fs');
const util = require('util');
const csv = require('fast-csv');
const dotenv = require('dotenv');

var outputFilesWritten = 0;

//const columnsToClear = ['BillingStreet','BillingCity','BillingState','BillingPostalCode','ShippingStreet','ShippingCity','ShippingState','ShippingPostalCode','Description','Email_List__c','Pincode__c','GSTIN__c','Salespoint_Address__c','Latitude_of_Salespoint_Address__c','Longitude_of_Salespoint_Address__c'];
const columnsToClear = [];

dotenv.config();
//const jsforce = require('jsforce');
const sfbulk = require('node-sf-bulk2');
const { resolve } = require('path');
const characters  = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
const pollFreqency = process.env.pollfrequency || 10000;
const PORT = process.env.PORT || 8080

var consumer_key = '3MVG9d8..z.hDcPI89tOplGirN9Ae17MVEa5ZpkY_yALFchiG9UPuYujA3A.GTA7h4sZFKtfLfIbJA8bXhmuD'
var consumer_secret = '8A324DFF62E0583F21097B1BFABB7D160C573D059AFF04D40053F43A82E938A9';
var callback_url = 'http://localhost:8080/oauthcallback';

const allmasters = ['Bank_Branch_Master__c','Branch_Master__c','Branch_Product_Master__c','Branch_Product_Region_Master__c','Branch_User_Master__c','Channel_Profile_Master__c','City_Master__c','Communication_Master__c','Condition_Master__c','Contest_Master__c','Country_Master__c','Deferral_Request_Approver_Master__c','Disbursal_Memo_Favoring_Details_Master__c','Document_Master__c','EMI_Date_Master__c','Employer_Master__c','Financier_Master__c','Foreclosure_Deviation_Master__c','Insurance_Deviation_Master__c','Insurance_Master__c','Negative_Profile_Master__c','NFA_Master__c','OD_Tenure_master__c','Perfios_Bank_Master__c','Pincode_Master__c','Pricing_Deviation_Master__c','Principal_Distribution_Master__c','Product_Master__c','Product_User_Master__c','Promo_Code_Master__c','Rejection_Cancellation_Reason_Master__c','SBI_Designation_Master__c','SBI_Employer_Master__c','SBI_Pincode_Master__c','Scheme_Master__c','Scheme_Selection_Master__c','Special_Pricing_Master__c','Stamp_Duty_Master__c','State_Master__c','VKYC_Master__c','Integration_Master__c','Insurance_Plans_Validations__c','Insurance_Rates__c','Questionnaire_Question__c','Questionnaire_Configuration__c','Questionnaire_Answer__c','Questionnaire_Section__c','Disbursal_Memo_Favoring_Details_Master__c'];


const outputFileDir = __dirname+'/output/';
const outputFile = __dirname + '/output.csv';
const failedResultsFile = __dirname + '/failedResults.csv';
const successfulResultsFile = __dirname + '/successfulResults.csv';
const queryResultsFile = __dirname + '/queryResults.csv';
const queryFetchFile = __dirname+'/bulkQueryResult.csv';


// Global variables to be used between calls
var csvFilePath;
var objectName;
var sessionResponse;

var app = express();
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static(path.join(__dirname, 'public')));
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.listen(PORT, () => console.log(`Listening on ${ PORT }`));

/**
 * Step 1 Web Server Flow - Get Code
 */
app.get('/webServer', function (req,res){  
  
  var sfdcURL = process.env.loginurl+'/services/oauth2/authorize' ;
  var url = sfdcURL+'?client_id='+ consumer_key+
        '&redirect_uri='+ callback_url+
        '&response_type=code';
  res.redirect(url);
});

/**
 * Step 2 Web Server Flow - Get Access Token
*/
app.get('/oauthcallback' ,  function(req,fnres) {
    
  var authCode = req.query.code;

  requestAccessToken(authCode)
  .then(function(response){
    // Save in persistent variable
    console.log('OAuth Login Successful');
    sessionResponse = response;
    console.log(sessionResponse);
    var headersData = {
      "headers":[],
      "instanceInfo":response.instance_url
    }
    fnres.render('pages/main', {
      headerData: JSON.stringify(headersData)
    });
  })
  .catch(function (error){
    fnres.status(200).json({ status: 'error', error : error });
    console.log(error);
  })
}); 


app.get('/', function(req, resp){

    var headersData = {
      "headers":[]
    }

    if(sessionResponse){
      headersData.instanceInfo = sessionResponse.instance_url;
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
      fs.unlink(failedResultsFile, function(err){
        console.log('File Removed from server');
      });
  });
});

app.get('/downloadSuccessfulFile', (req, res) => {

  res.download(successfulResultsFile, 'successfulResults.csv', function(err){
      fs.unlink(successfulResultsFile, function(err){
        console.log('File Removed from server');
      });
  });
});

app.get('/downloadQueriedFile', (req, res) => {

  res.download(queryResultsFile, 'queryResults.csv', function(err){
      fs.unlink(queryResultsFile, function(err){
        console.log('File Removed from server');
      });
  });

});

app.get('/copySharingRuleFile', (req, res) => {

  allmasters.forEach((masterName) =>{
      copyFile('Applicant_Relationship_Master__c.sharingRules-meta.xml', masterName+'.sharingRules-meta.xml');
  })


});

function copyFile(sourceFileName, targetFileName) {
  const sourcePath = path.join(__dirname, sourceFileName);
  const targetPath = path.join(__dirname, targetFileName);

  fs.copyFile(sourcePath, targetPath, (err) => {
    if (err) {
      console.error(`Error copying the file: ${err.message}`);
    } else {
      console.log(`File copied from ${sourceFileName} to ${targetFileName}`);
    }
  });
}

app.post('/fetchJobStatus', function(req, res){

  var jobId = req.body.jobId;
  checkSessionValidaity()
  .then(function(){
    console.log('login finished');
    return getJobStatus(jobId);
  })
  .then(function(response){
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

  checkSessionValidaity()
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

app.post('/fetchSuccessfulResults', function(req, res){

  var jobId = req.body.jobId;

  const writeStream = fs.createWriteStream(successfulResultsFile);
  writeStream.on("finish", function(){
    res.status(200).json({ a: 1 });
  });

  const transform = csv.format({ headers: true })
    .transform((row) => {
      return row;
  });

  checkSessionValidaity()
  .then(function(){
    console.log('login finished');
    return getSuccessfulResults(jobId);
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

app.post('/fetchQueryResults', function(req, res){

  var jobId = req.body.jobId;
  var locator = req.body.locator;

  checkSessionValidaity()
  .then(function(){
    console.log('login finished');
    return getAllBulkQueryResult(jobId, locator, queryResultsFile);
  })
  .then(function(responseString){
    res.status(200).json({ a: 1 });
  })
  .catch(function (error) {
    console.log(error);
    res.status(200).json({ status: 'error', error : error });
  });

});

app.post('/uploadSF', function(req, res){

  var opType = req.body.operationType;
  var extField = req.body.externalIdField;

  checkSessionValidaity()
  .then(function(){
    return splitIntoMultipleCSV();
  })
  .then(function(){
    console.log('All CSV Split');
    return submitBulkUploadJobMultiple(opType, extField);
    //return submitBulkUploadJob(opType, extField, outputFile);
  })
  .then(function(){
    console.log('After all Submitted')
    /*
    fs.unlink(outputFile, function(err){
      console.log('File Removed');
    });
    */
    console.log('upload jobs started - see console for job ids');
    res.status(200).json({ status: 'success', jobId: 'Multiple Job Ids - See console'});
  })
  .catch(function (error) {
    res.status(200).json({ status: 'error', error : error });
  });
  
})

app.post('/transformFile', function(req, res){

    console.log('Transforming');
    var columnParams = req.body.data;
    var headerNames = req.body.headers;
    const writeStream = fs.createWriteStream(outputFile);
    writeStream.on("finish", function(){
      res.status(200).json({ a: 1 });
    });

    const transform = csv.format({ headers: headerNames })
    .transform((row) => {
      
      let returnVal = {};
      Object.keys(columnParams).forEach(function(key){
        if(columnParams[key] != 'Exclude'){
          var a = row[key]?row[key]:'';
          returnVal[key] = transformData(columnParams[key], a);        
        }
      });

      columnsToClear.forEach(function(col){
        returnVal[col] = '#N/A';
      })

      return returnVal;
    });

    if(csvFilePath){
      const parse = csv.parse({ headers: headerNames })
      const stream = fs.createReadStream(csvFilePath)
      .pipe(parse)
      .pipe(transform)
      .pipe(writeStream);
    }
    else {
      const parse = csv.parse({ headers: true });
      const stream = fs.createReadStream(queryFetchFile)
      .pipe(parse)
      .pipe(transform)
      .pipe(writeStream);
  }

});


app.post('/', upload.single('csvFile'), function(req, res){

  console.log('Extracting');

  objectName = req.body.objectName;
  if(req.file){
      
      csvFilePath = req.file.path;
      console.log(csvFilePath);
      processCSVFileHeader(csvFilePath)
      .then(function(data){
        console.log('header processed');
        var respData = data;
        if(sessionResponse){
          respData.instanceInfo = sessionResponse.instance_url;
        }
        res.render('pages/main', {
          headerData: JSON.stringify(respData)
        });
      })
  }
  else{
    
    let queryText = req.body.queryText;
    checkSessionValidaity()
    .then(function(){
      return submitBulkQueryJob(queryText)
    })
    .then(function(jobId){
      console.log('bulk query job submitted ' + jobId);
      return bulkStatusRecursive(jobId);
    })
    .then(function(data){
      var respData = data;
      console.log(respData);
      if(sessionResponse){
        respData.instanceInfo = sessionResponse.instance_url;
      }
      res.render('pages/main', {
        headerData: JSON.stringify(respData)
      });
    })
    .catch(function(err){
      res.status(200).json({ status: 'error', error : err });
    })
  }
  
});

function checkSessionValidaity(){
  return new Promise(function(resolve, reject) {

    if(sessionResponse){
      // TBD Check if Access Token still Valid
      // Else Exchange Refresh Token for new Access Token
      resolve();
    }
    else{
      reject('Invalid Session. Please Login');
    }

  });
}


function httpRequest(post_options,post_data ) {
    
  return new Promise(function(resolve, reject) {
    var post_req = https.request(post_options, function(res) {
      var respBody = '';
      res.setEncoding('utf8');
      res.on('data', function (respData) {
          respBody += respData;
      });
      res.on('end', function(){
          var response = JSON.parse(respBody);
          if(!response.error){
            resolve(response);    
          }
          else{
            var errorMessage = response.error_description;
            reject(errorMessage);
          }
      });
      res.on('error', function(error){
          console.log('Error: ' + error);
          reject(error);
      });
    });

    post_req.on('error', function(error){
      console.log('Error: ' + error);
      reject(error);
    });
    // post the data
    post_req.write(post_data);
    post_req.end();
    });
}

function requestAccessToken(authCode){

  var post_options = {
          method:'POST',
          headers: {
            'Content-Type': 'application/json',
             'Content-Type':'application/x-www-form-urlencoded'
          },
          host: process.env.loginhost,
          path:'/services/oauth2/token'
        }

  var post_data = 'client_id='+ consumer_key
            + '&redirect_uri='+ callback_url
            + '&grant_type=authorization_code'
            + '&code='+ authCode;

  return httpRequest(post_options, post_data);
}

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
      .pipe(parse)

  });

}

async function submitBulkQueryJob(queryText){

  console.log('inside submitBulkQueryJob');
  try {

        let bulkconnect = {
              'accessToken': sessionResponse.access_token,
              'apiVersion': '51.0',
              'instanceUrl': sessionResponse.instance_url
        };

        const bulkapi2 = new sfbulk.BulkAPI2(bulkconnect);
        const queryInput = {
            'query': queryText,
            'operation': 'query'
        };
        const response = await bulkapi2.submitBulkQueryJob(queryInput);
        return Promise.resolve(response.id);

    } catch (ex) {
        console.log(ex);
        return Promise.reject(ex);       
    }
}

async function submitBulkUploadJob(operationType, extField, outputFilePath){
  try {
      
      console.log('bulk upload started');
      
      let bulkconnect = {
          'accessToken': sessionResponse.access_token,
          'apiVersion': '51.0',
          'instanceUrl': sessionResponse.instance_url
      };

      const bulkrequest = new sfbulk.BulkAPI2(bulkconnect);
      // create a bulk update job
      const jobRequest = {
          'object': objectName,
          'operation': operationType,
          'contentType':'CSV'
      };

      if(operationType == 'upsert'){
        jobRequest["externalIdFieldName"] = extField;
      }

      console.log(jobRequest);

      const response = await bulkrequest.createDataUploadJob(jobRequest);
      if (response.id) {
          console.log('upload job created' + response.id);
          // read csv data from the local file system
          const data = await util.promisify(fs.readFile)(outputFilePath, "UTF-8");
          const status = await bulkrequest.uploadJobData(response.contentUrl, data);
          if (status === 201) {
              // close the job for processing
              await bulkrequest.closeOrAbortJob(response.id, 'UploadComplete');
              console.log('upload job subimtted' + response.id);
              return Promise.resolve();
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

  let bulkconnect = {
          'accessToken': sessionResponse.access_token,
          'apiVersion': '51.0',
          'instanceUrl': sessionResponse.instance_url
  };

  const bulkrequest = new sfbulk.BulkAPI2(bulkconnect);
  const response = await bulkrequest.getIngestJobInfo(jobId);
  return Promise.resolve(JSON.parse(JSON.stringify(response)));

}

async function getSuccessfulResults(jobId){

  let bulkconnect = {
          'accessToken': sessionResponse.access_token,
          'apiVersion': '51.0',
          'instanceUrl': sessionResponse.instance_url
  };

  const bulkrequest = new sfbulk.BulkAPI2(bulkconnect);
  const response = await bulkrequest.getResults(jobId, 'successfulResults');
  return Promise.resolve(response);

}

async function getAllBulkQueryResult(jobId, locator, filePath){

  console.log('fetching results of all batches');
  let sforceLocator = locator;
  let bulkconnect = {
      'accessToken': sessionResponse.access_token,
      'apiVersion': '51.0',
      'instanceUrl': sessionResponse.instance_url
  };

  const bulkrequest = new sfbulk.BulkAPI2(bulkconnect);
  const csvStringArray = [];
  var checkMax = false;
  var maxCnt = 0;
  if(process.env.maxQueryLocators){
    checkMax = true;
    maxCnt = process.env.maxQueryLocators;
  }
  var cnt = 0;  
  do{
      console.log(new Date().toLocaleString() + ' fetching for sforce locator : ' + sforceLocator);
      const response = await bulkrequest.getBulkQueryResults(jobId, sforceLocator);
      cnt++;
      csvStringArray.push(response.data);
      sforceLocator = response.headers["sforce-locator"];
      if(checkMax && cnt >= maxCnt){
        console.log('Max Locators reached. Please note the sforce locator to start with again - '+ sforceLocator);
        break;
      }
  }
  while(sforceLocator && sforceLocator != null && sforceLocator != 'null' && sforceLocator != '');
  return concatCSVAndWrite(csvStringArray, filePath);

}

async function getFailedResults(jobId){

  let bulkconnect = {
          'accessToken': sessionResponse.access_token,
          'apiVersion': '51.0',
          'instanceUrl': sessionResponse.instance_url
  };

  const bulkrequest = new sfbulk.BulkAPI2(bulkconnect);
  const response = await bulkrequest.getResults(jobId, 'failedResults');
  return Promise.resolve(response);

}

function asyncThing( asyncParam) { // example operation
    const promiseDelay = (data,msec) => new Promise(res => setTimeout(res,msec,data));
    return promiseDelay( asyncParam, pollFreqency); //resolve with argument in 10 second.
}

function bulkStatusRecursive( jobId ) {

    console.log('polling SF bulk status');
    async function decide( jobId) {        
        let bulkconnect = {
              'accessToken': sessionResponse.access_token,
              'apiVersion': '51.0',
              'instanceUrl': sessionResponse.instance_url
        };

        const bulkapi2 = new sfbulk.BulkAPI2(bulkconnect);
        const response = await bulkapi2.getBulkQueryJobInfo(jobId);

        if(response.state == 'InProgress' || response.state == 'UploadComplete' ){
            return bulkStatusRecursive(jobId);
        }
        else if(response.state == 'JobComplete'){
            // const result = await bulkapi2.getBulkQueryResults(jobId);
            return getAllBulkQueryResult(jobId, null,  queryFetchFile);
        }
        else if(response.state == 'Aborted' || response.state == 'Failed'){
            return response;
        }
    }

    // Return a promise resolved by doing something async and deciding what to do.
    // to be clear the returned promise is the one returned from the .then call

    return asyncThing(jobId).then(decide);
}


// Utility Functions


function transformData(type, a){

    switch (type){
      case 'Jumble Up Text':
          a = scramble(a);
          break;
      case 'Jumble Up Numbers':
        a = scrambleNumbers(a);
        break;
      case 'Jumble Up Email':
          console.log(a);
          a = scrambleEmail(a);
          console.log(a);
          break;
      case 'Random Phone':
          a = randomPhone(a);
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
      case 'Mask with X':
          a = a.replace(/./g, 'X');
          break;
      case '#NA':
          if(a && a != ''){
            a = '#N/A';
          }
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

function scrambleNumbers(a){
  if(a && a != ''){
    a= numberExponentToLarge(a);
    var sign="";
    a.charAt(0)=="-" && (a = a.substring(1),sign ="-"); // remove - sign & remember it
    var deciSp = 1.1.toLocaleString().substring(1,2);  // Get Deciaml Separator 
    strA = a.split(deciSp);
    var rhs = (strA[1])? "."+strA[1] : "";
    a = strA[0];
    a = scramble(a);
    return sign+a+rhs;
  }
  return a;
}

function numberExponentToLarge(numIn) {
  numIn +="";                                                   // To cater to numric entries
  var sign="";                                                  // To remember the number sign
  numIn.charAt(0)=="-" && (numIn =numIn.substring(1),sign ="-"); // remove - sign & remember it
  var str = numIn.split(/[eE]/g);                                 // Split numberic string at e or E
  if (str.length<2) return sign+numIn;                   // Not an Exponent Number? Exit with orginal Num back
  var power = str[1];                                             // Get Exponent (Power) (could be + or -)
 
  var deciSp = 1.1.toLocaleString().substring(1,2);  // Get Deciaml Separator
  str = str[0].split(deciSp);                        // Split the Base Number into LH and RH at the decimal point
  var baseRH = str[1] || "",                         // RH Base part. Make sure we have a RH fraction else ""
      baseLH = str[0];                               // LH base part.

   if (power>=0) {   // ------- Positive Exponents (Process the RH Base Part)
      if (power> baseRH.length) baseRH +="0".repeat(power-baseRH.length); // Pad with "0" at RH
      baseRH = baseRH.slice(0,power) + deciSp + baseRH.slice(power);      // Insert decSep at the correct place into RH base
       if (baseRH.charAt(baseRH.length-1) ==deciSp) baseRH =baseRH.slice(0,-1); // If decSep at RH end? => remove it
 
   } else {         // ------- Negative exponents (Process the LH Base Part)
      num= Math.abs(power) - baseLH.length;                               // Delta necessary 0's
      if (num>0) baseLH = "0".repeat(num) + baseLH;                       // Pad with "0" at LH
      baseLH = baseLH.slice(0, power) + deciSp + baseLH.slice(power);     // Insert "." at the correct place into LH base
      if (baseLH.charAt(0) == deciSp) baseLH="0" + baseLH;                // If decSep at LH most? => add "0"
 
   }
   // Rremove leading and trailing 0's and Return the long number (with sign)
   return sign + (baseLH + baseRH).replace(/^0*(\d+|\d+\.\d+?)\.?0*$/,"$1");
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
    var pattern = /^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$/;
    return a.match(pattern);    
}

function splitIntoMultipleCSV(){

  return new Promise((resolve, reject) => {
    const datas = {}; 
    const options = {headers: true}; // relative to your CSV usage
    let rowCnt = 0;
    let splitNo = 0;
    let splitRowCount = process.env.splitRowsCount;
    datas[splitNo] = [];
    csv
    .parseFile(outputFile, options)
    .on('data', d => {
        datas[splitNo].push(d);
        rowCnt ++;
        if(rowCnt == splitRowCount){
          rowCnt = 0;
          splitNo ++;
          datas[splitNo] = [];
        }
    })
    .on('end', () => {
      outputFilesWritten = splitNo+1;
      Object.keys(datas).forEach(splitNo => {
            // For each ID, write a new CSV file
            csv
            .write(datas[splitNo], options)
            .pipe(fs.createWriteStream(outputFileDir +`output-${splitNo}.csv`));
        })
        resolve();
    });
  })
}

async function submitBulkUploadJobMultiple(operationType, extField){

  for(let i=0; i<outputFilesWritten; i++){
      await submitBulkUploadJob(operationType, extField, outputFileDir +`output-${i}.csv`);
  }
  return Promise.resolve();
}

function concatCSVAndWrite(csvStringsArray, outputFilePath) {
  console.log(new Date().toLocaleString() + ' merging all responses..');
  console.log('responses size -' + csvStringsArray.length);
  const promises = csvStringsArray.map((csvString, index) => {
    return new Promise((resolve) => {
      const dataArray = [];
      var headers = (index ==0)?true:false;
      return csv
          .parseString(csvString, {headers: headers})
          .on('data', function(data) {
            dataArray.push(data);
          })
          .on('end', function() {
            resolve(dataArray);
          });
      });
  });

  return Promise.all(promises)
      .then((results) => {

        return new Promise((resolve, reject) => {
          // clear out csvStrings Array, save some memory
          let rowCount = -1;
          csvStringsArray = [];
          console.log(new Date().toLocaleString() + ' all merging done, now writing to file');
          const csvStream = csv.format({headers: true});
          const writableStream = fs.createWriteStream(outputFilePath);

          writableStream.on('finish', function() {
            console.log('CSV writing complete')
            resolve({'headers' : Object.keys(results[0][0]) , 'rowCount': rowCount});
          });

          csvStream.pipe(writableStream);
          
          results.forEach((result, mainCsvIndex) => {
            result.forEach((data, index) => {
              if(mainCsvIndex == 0 || index !=0){
                rowCount++;
                csvStream.write(data);
              }
            });
          });
          csvStream.end();
          console.log('writing to csv');
          });
      });
}