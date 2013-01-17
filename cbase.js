var qs = require('querystring')
    , fs = require('fs')
    , http = require('http');

var EventEmitter = require('events').EventEmitter
    , queuer = new EventEmitter;

// replace the values between the quotes with your crunchbase api key
var apikey = '<API KEY>';
// put in the number of concurrent retrievals from crunchbase. Suggestion: 100 brings the system to a crawl. 10 was a good number for me.
var concurrency_nbr = 10;
var comp_list = [];

// once the previous company has been processed, start with the next one.
queuer.on('done', function() {
  console.log('number remaining: ' + comp_list.length + '\n');
	if (comp_list.length > 0) {
	    var next_item = comp_list.shift();
	    retrieveCBEntity(next_item);
	} else {
	    process.exit(0);
	}
    });

// Retrieving the list of companies from crunchbase takes a while, so I wrote a separate js file to cache this data locally (company_list)
function readFile() {
    console.log('reading company list...');
    comp_list = fs.readFileSync('company_list.csv').toString().split("\n");
    console.log('done\nreading records already processed....');

    // This file is the output file. The next few steps implement restart logic in case the process dies we do not have to reprocess these.
    processed_list = fs.readFileSync('cbdata.csv').toString().split("\n");
    console.log('done\nremoving companies already processed....part 1 of 2....');
    processed_list = processed_list.map(function(item) {
	    return item.slice(0,item.indexOf(","));
	});

    console.log('done\nremoving companies already processed....part 2 of 2....');
    comp_list = comp_list.filter(function(item) {
	    var idx = processed_list.indexOf(item);
	    if (idx % 100 == 0) {
		console.log('processed ' + idx + ' = ' + item + '\n');
	    }
	    if (idx == -1) {
		return true;
	    }
	});
    console.log('done\n');
}

// This function kicks off the concurrent threads up to the number you set for the concurrency_nbr
function stacker() {
    console.log('kicking off ' + concurrency_nbr + ' threads to process companies\n');
    for (var i = 0; i < concurrency_nbr; i++) {
	console.log('number remaining: ' + comp_list.length + '\n');
	var next_item = comp_list.shift();
        retrieveCBEntity(next_item);
    }
}

// This function first makes the call to crunchbase, assembles the response, calls processEntity to output the data and then emits
// a 'done' message to initiate processing of the next company on the queue
function retrieveCBEntity(company) {
    console.log('processing: ' + company + ' ..... ');
    
    http.request({
	    host: 'api.crunchbase.com'
		, path: '/v/1/company/' + company + '.js?' + qs.stringify({ api_key: apikey })
		}, function (res) {
	    var body = '';
	    res.setEncoding('utf8');
	    res.on('data', function (chunk) {
		    body += chunk;
		});
	    res.on('end', function () {
		    try {
			obj = JSON.parse(body);
			processEntity(obj);
		    } catch(e) {
			queuer.emit('done');
			return true;
		    }
		    queuer.emit('done');
		    return true;
		});
	}).end();

}

function processEntity(entityInfo) {
    
    // initialize variables
    var countr = 0;
    var zipcode = '';
    var latitude = '';
    var longitude = '';
    
    console.log(entityInfo);
    // loop through all the offices for the company
    entityInfo.offices.forEach( function (w) {
	    // always default to using the first address as the primary in case there is none identified with HQ
	    if (countr == 0) {
		zipcode = w.zip_code;
		latitude = w.latitude;
		longitude = w.longitude;
	    }
	    // if a Headquarters address is found, override the first record
	    if (w.description == "Headquarters") {
		zipcode = w.zip_code;
		latitude = w.latitude;
		longitude = w.longitude;
	    }
	    countr++;
	});
    
    // if the company has raised money, put each fund raising as a separate record in the output. Otherwise, just write out the company
    // information with zero values for the funding amounts.
    if (entityInfo.funding_rounds.length > 0) {
	
	entityInfo.funding_rounds.forEach( function (w) {
		fs.appendFileSync('cbdata.csv', entityInfo.permalink + ',' + zipcode + ',' + latitude + ',' + longitude);
		fs.appendFileSync('cbdata.csv', ',' + w.funded_year + ',' + w.funded_month + ',' + w.raised_amount + ',');
		fs.appendFileSync('cbdata.csv', w.raised_currency_code + ',' + w.round_code + '\n');
	    });
	console.log('Funding Rounds: ' + entityInfo.permalink + ' = ' + entityInfo.funding_rounds.length + '\n');
    } else {
      
	fs.appendFileSync('cbdata.csv', entityInfo.permalink + ',' + zipcode + ',' + latitude + ',' + longitude + ',0,0,0,USD,0\n');
	console.log('Funding Rounds: ' + entityInfo.permalink + ' = 0\n');
    }
}

// just read the input file to an array (readFile) and process the array (stacker)
readFile();
stacker();
