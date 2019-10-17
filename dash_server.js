var express = require('express'),
	app = express(),
	bodyParser = require('body-parser'),
	config = require('./db/config.js'),
	{ Pool } = require('pg'),
	pool = new Pool(config.pg_credentials),
	types = require('pg').types,
	{ createLogger, format, transports } = require('winston');

const logger = createLogger({
  level: 'info',
  format: format.combine(
      format.timestamp(),
      format.json()
    ),
  defaultMeta: {service: 'user-service'},
  transports: [
    new transports.File({ filename: './logs/dash_server.log', 
    	level: 'error',
    	timestamp: true })  ]
});

// Object mapping parameters for burndown-data endpoint to SQL queries in config.js 
const burndownQueryTypes = {'all-funds': 'all_funds_bd_query',
							'single-fund': 'single_fund_bd_query',
							'single-ledger': 'single_ledger_bd_query'};

// Use the type parser to cast integers returned by postgres to floats -- otherwise, Node seems to convert these to strings
types.setTypeParser(1700, val => parseFloat(val));

async function getTable (query, params=null) {
	/* Runs a static query against the PG database.*/
	try {
		let rowsObj = (params)? await pool.query(query, params): await pool.query(query);
		return {rows: rowsObj.rows, cols: rowsObj.fields};
	}
	catch (e) {
		logger.error(e);
	}
}


// Directory for index.html, etc.
app.use('/', express.static(__dirname + '/public'));
// Express middleware for static files, redirecting the <script> and <link> tags from index.html
app.use('/handsontable', express.static(__dirname + '/node_modules/handsontable/dist'));
app.use('/d3', express.static(__dirname + '/node_modules/d3/dist'));
app.use('/bootstrap-css', express.static(__dirname + '/node_modules/bootstrap/dist/css'));
app.use('/bootstrap-js', express.static(__dirname + '/node_modules/bootstrap/dist/js'));
app.use('/jquery', express.static(__dirname + '/node_modules/jquery/dist'));

app.get('/test', async (req, res) => {
	let data = await getTable(config.queries.orders_query);
	res.send(data);

});

// Endpoint for AJAX request for POL-level data
app.get('/orders-data', async (req, res) => {
	let data = await getTable(config.queries.orders_query);
	res.send(data);
});
// Endpoint for AJAX request for fund data
app.get('/funds-data', async (req, res) => {
	let data = await getTable(config.queries.funds_query);
	res.send(data);
});
// Endpoint for refresh timestamp data
app.get('/timestamp-data', async (req, res) => {
	let data = await getTable(config.queries.refresh_ts_query);
	res.send(data);
});
/* Endpoint for AJAX request for burndown (timeseries) data
 Expects 1 required URL parameter:
 	type={[single-fund, all-funds, single-ledger]}
 Accepts additional URL parameters:
 	ledger={ledger_name}
 	fundCode={fund_code}

*/
app.get('/burndown-data', async (req, res) => {
	let queryType = req.query.type,
		data;
	// validate query type param
	if (Object.keys(burndownQueryTypes).includes(queryType)) {
		if (queryType == 'all-funds') {
			data = await getTable(config.queries[burndownQueryTypes[queryType]]);
		}
		else {
			params = (req.query.fundCode)? req.query.fundCode : req.query.ledger;
			params = Array(4).fill(params);
			data = await getTable(config.queries[burndownQueryTypes[queryType]], 
									params);
		}
		res.send(data);
	} 
});

server = app.listen(3000);
