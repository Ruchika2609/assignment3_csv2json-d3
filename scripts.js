
var fs = require('fs');

file="India2011.csv";
appendFile="IndiaSC2011.csv";
appendAnotherFile="IndiaST2011.csv";

//merge csv files function
function readAppend(file,appendFile,callback)
{
  // Use of sync method is neccessary to avoid corruption of appended file
  var data = fs.readFileSync(appendFile);
  // file and data are appended
  fs.appendFileSync(file, data);
    callback();
}

//call merge files
readAppend(file,appendFile,function(){
  //appending third file
  readAppend(file,appendAnotherFile,function(){
  readCSVFileAsync(function(){
  //  console.log("called in order");
  });
});
});


function readCSVFileAsync(callback){
  var readline = require('readline');
  var fs = require('fs');
  var header =[];
  var jsonData=[];
  var tempData={};
  var isHeader=true;
  //when reading large files- we can not use standard methods for file read and write.
  //we read a chunk of data manipulate and write it back immediately.
  const rl = readline.createInterface({
    input: fs.createReadStream('India2011.csv')
  });

// Read data line by line
  rl.on('line', function(line) {
    // store each row in the array lineRecords
    var lineRecords = line.trim().split(',');
    for(var i=0; i<lineRecords.length; i++){
      if(isHeader){
        header[i]=lineRecords[i];
      } else{
        tempData[header[i]]=lineRecords[i];
      }
    }
    jsonData.push(tempData);
    tempData={};
    isHeader=false;
    //  fs.writeFileSync("data.json",JSON.stringify(jsonData),encoding="utf8");

  });
  rl.on('close', function() {
    console.log("jsonData length="+jsonData.length);
    createoutputJSON(jsonData);

  });
  callback();
}

//filtering age wise literate population
function createoutputJSON( jsonArr ) {
  var output = [];
  var noOfRows = jsonArr.length;

  for (var i = 1; i < noOfRows;i++) {
    if(jsonArr[i]["Age-group"] != 'All ages' && jsonArr[i]["Age-group"] != 'Age not stated' &&  jsonArr[i]["Age-group"] != 'Age-group')
    {
      output[i] = {
        ageGroup : jsonArr[i]['Age-group'],
         Literate : parseInt(jsonArr[i]['Literate - Persons'])
      };
    }
  }

  var newArray = [];
  for (var i = 0; i < output.length; i++) {
    if (output[i] !== undefined && output[i] !== null && output[i] !== "") {
      newArray.push(output[i]);
    }
  }

  var temp = {};
  var obj = null;
  for(var i=0; i < newArray.length; i++) {
    obj=newArray[i];

    if(!temp[obj.ageGroup]) {
      temp[obj.ageGroup] = obj;
    } else {
      temp[obj.ageGroup].Literate += obj.Literate;
    }
  }
  var result = [];
  for (var prop in temp)
  result.push(temp[prop]);

  fs.writeFile("output.json", JSON.stringify(result,undefined, 2), function (err) {
    if (err) throw err;
    console.log('output JSON file has been successfully created');
  });
}
