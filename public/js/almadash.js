/*
Provides tabular and graphical views of current, projected, and proposed spending on the collections budget.
Uses d3.js for rendering a "burndown" chart (rolling expenditures to date, plus projections).
Uses handsontable.js for rendering the data in chart form, in addition to an itemized list of orders.
*/
// for handsontable columns
const colWidths = 150;

// columns for display, mapped to columns returned from the database query
// using the Map structure because iteration preserves insertion order
const ordersColumnMap = new Map([['Order Type', 'order_type'], 
						['POL', 'po_line_reference'], 
						['Title', 'title'],
						['Renewal Date', 'renewal_date'],
						['Vendor', 'vendor_code'],
						['Fund Name', 'enc_fund_names'],
						['Fund Code', 'enc_fund_codes'],
						['Amount Projected / Proposed', 'encumbrance_amount'],
						['Amount Spent', 'expenditure_amount'],
						['Order Status', 'order_status']]);

const fundsColumnMap = new Map([['Ledger', 'ledger_name'],
								['Fund Name', 'fund_ledger_name'],
								['Fund Code', 'fund_ledger_code'],
						        ['Balance Available', 'alma_balance_available'],
						        ['Wishlist Balance Available', 'wishlist_balance_available']]);

//Object for mapping order statuses to text for the UI
const orderStatusMap = {nullInvoice: 'Not Yet Invoiced',
				   		multipleInvoices: 'Multiple',
				   		negotiationStatus: 'In Negotiation',
				   		licenseStatus: 'In License Review'}

// Defines properties for each table
const tableProps = [{endPoint: 'funds-data',  // server endpoint to retrieve data
						elementId: 'funds-table', // HTML ID of container
						columnMap: fundsColumnMap, // mapping db columns to table view
						renderAllRows: false, // turn off for better performance
						// need to set both width and height -- if height is not set, table is rendered to fill the browser window
						width: colWidths * (fundsColumnMap.size - 1) + 75,	// one column is hidden; add constant for row index 
						height: 400,
						hiddenColumns: {columns: [2]}, // hiding the fund code column, which is the key we use for db queries
						hooks: {afterSelectionEndByProp: fundSelectionListener} // listens for the user's selection of a cell on the table
					},
					{endPoint: 'orders-data',
						elementId: 'orders-table',
						columnMap: ordersColumnMap,
						renderAllRows: false, 
						width: colWidths * ordersColumnMap.size + 75, // add 100 for row index 
						height: 400
					}];
// Properties for the burndown chart
const chartProps = {margin: {top: 20, 
							right: 20, 
							bottom: 75, 
							left: 120
					},
					dollars: d3.format('$,.2f')}; // currency format for chart
// visual margins for the chart, relative to the SVG area
chartProps.width = 800 - chartProps.margin.left - chartProps.margin.right;
chartProps.height = 400 - chartProps.margin.top - chartProps.margin.bottom;
    						 

// Functions to compute each type of value for the chart's lines
// d is a data element bound to the points on the <path> element by the d3 selector: each datum corresponds to a date and a set of amounts valid for that date
const valueFunctions = {actual: (d) => d.total_alloc - d.daily_exp,
						projected: (d) => d.total_alloc - (d.daily_exp + d.daily_enc),
						proposed: (d) => d.total_alloc - (d.daily_exp + d.daily_enc + d.wishlist_proposed)
					};
// d3 time formatting utility, for use with data refresh timestamps
const formatTime = d3.timeFormat('%m-%d-%Y');

function orderStatusRenderer (instance, td, row, col, prop, value, cellProperties) {
/*Custom renderer for inserting HTML into the order status column.*/
	let element = document.createElement('div');
	// Set background color based on status, mapping to Bootstrap contextual backgrounds
	let bgColor;
	if (value == orderStatusMap.nullInvoice) bgColor = 'primary';
	else if (value == orderStatusMap.negotiationStatus) bgColor = 'warning';
	else if (value == orderStatusMap.licenseStatus) bgColor = 'info';
	else if (value == orderStatusMap.multipleInvoices) bgColor = 'danger';
	else bgColor = 'success';
	element.textContent = value;
	element.className = `alert alert-${bgColor}`;
	element.setAttribute('role', 'alert');
	Handsontable.dom.empty(td);
	td.append(element);
	return td;
}

function populateTable(data, props) {
	/* Sets up a handsontable instance. Code below is mostly boilerplate. */
	let container = document.getElementById(props.elementId);
	let table = new Handsontable(container, {
		data: data.rows,
		renderAllRows: props.renderAllRows,
		width: props.width,
		height: props.height,
		manualColumnResize: true,
		manualRowResize: true,
		rowHeaders: true,
		copyPaste:true,
		// Need to use the spread operator to convert the Map iterable to an array
		colHeaders: [...props.columnMap.keys()],
		// Map function to reassign db keys to more readable column names
		columns: [...props.columnMap.values()].map(column => {
			// Handles currency columns
			if (column.endsWith('amount') || column.endsWith('available')) {			
				return {data: column,
						editor: false, // This turns off editing for each column
						type: 'numeric',
     					 numericFormat: {
       						 pattern: '$0,0.00'}
       					}
			}
			// Handles date columns
			else if (column.endsWith('date')) {
				return {data: column,
						editor: false,
						type: 'date',
						dateFormat: 'MM-DD-YYYY',
						correctFormat: true,
						
					};
			}
			else if (column.endsWith('status')) {
				return {data: column,
						editor: false,
						renderer: orderStatusRenderer
					};
			}
			return {data: column,
					editor: false}
		}),
		hiddenColumns: props.hiddenColumns,
		colWidths: colWidths, // TO DO: make some columns smaller
		columnSorting: true,
		filters: true,
		dropdownMenu: ['filter_by_value', 
					'filter_by_condition',	// Works for limiting dates to a range, as long as they entered in the correct format
					'filter_action_bar'], 		// limit the options in the column drop-down to the filter functions
		licenseKey: 'non-commercial-and-evaluation' // necessary when using the free version of handsontable
	});

	// Optionally add event listeners to provided hooks
	if (props.hooks) {
		for (let hook in props.hooks) {
			// using .bind to partially apply the table to the listener function, so that we can reference it later
			let listenerFunc = props.hooks[hook].bind(null, table);
			Handsontable.hooks.add(hook, listenerFunc, table);
		}
	}

	// Creates a CSV export button
	let downButton = document.getElementById(`export-csv-${props.elementId}`),
	  	 exportPlugin1 = table.getPlugin('exportFile');
	//configure the export-to-CSV function
	downButton.addEventListener('click', function() {
    	exportPlugin1.downloadFile('csv', {
      		bom: false,
      		columnDelimiter: ',',
      		columnHeaders: false,
		    fileExtension: 'csv',
      		filename: `${props.elementId}_[YYYY]-[MM]-[DD]`, // filename contains a timestamp
      		mimeType: 'text/csv',
      		rowDelimiter: '\r\n',
      		rowHeaders: false
    	});
  	}); 	 
	// return the table and its id to the caller
	return [props.elementId, table];
}

function cleanData(data) {
	/* Converts arrays to strings and date strings to JS datetime objects. */
	data.rows = data.rows.map(row => {
		for (let key of Object.keys(row)) {
			// only for date columns with values --> ignore nulls
			if (key.endsWith('date') && (row[key] != null)) {
				let dt = new Date(Date.parse(row[key]));
				// apply d3 formatting function
				row[key] = formatTime(dt);
			}
			else if (Array.isArray(row[key])) {
				// Case: it's an invoice status
				if (key == 'order_status') {
					if (row[key].length > 1) row[key] = orderStatusMap.multipleInvoices;  // Multiple invoices
					else row[key] = row[key][0]; // single invoice status
				}
				// For arrays of funds, we need to convert to strings for filtering purposes
				// Use a delimiter to show multiple values		
				else row[key] = row[key].join(' || ');
			}
			// No invoice data
			else if ((row[key] === null) && (key == 'order_status')) row[key] = orderStatusMap.nullInvoice;
			// It will be a JSON object if it's for wishlist status, because we have two values to keep track of
			else if ((typeof row[key] == 'object') && (row[key] != null)) {
				if (!row[key].license_review_status) row[key] = orderStatusMap.negotiationStatus;
				else row[key] = orderStatusMap.licenseStatus;
			}
		}
		return row;
	});
	return data;
}

function fetchTableData(props) {
	/*Perform the AJAX request for the table data*/
	return fetch(props.endPoint)
		.then(response => response.json())
		.then(data => {
			/* data will be an object with two properties, 'rows' and 'cols'. 
			data.rows is an array of objects, where each property is a column name. 
			data.columns is an array of objects, in which the 'name' property gives the name of a column. */
			// We need to do additional cleaning of the orders data
			if (props.endPoint == 'orders-data') data = cleanData(data);
			let table = populateTable(data, props);
			return table;
		})
		.catch(e => console.log(e));
}

function fundSelectionListener(table, startRow, startColProp, endRow, endColProp) {
	/* Listens for a cell selection; checks for the presence of a column corresponding to either a fund name or ledger name. Updates an <input> element with the selection.*/
	// Do nothing if a multi-cell range is selected, or if the selection is not a ledger or fund column
	if ((startRow != endRow) || (startColProp != endColProp) || (!['ledger_name', 'fund_ledger_name'].includes(startColProp))) return

	else if (startColProp == 'fund_ledger_name') {
		// Get the selected element and other elements needed for a query
		let fundLedgerName = table.getDataAtProp('fund_ledger_name')[startRow]
			fundLedgerCode = table.getDataAtProp('fund_ledger_code')[startRow],
			ledgerName = table.getDataAtProp('ledger_name')[startRow];
		// Set the input value to the fund name, but bind the fund code to the element for retrieval by the submit function
		$('#fund-name').val(fundLedgerName).data('fund_ledger_code', fundLedgerCode);
		$('#ledger-name').val(ledgerName);
	}
	else {
		let ledgerName = table.getDataAtProp('ledger_name')[startRow];
		$('#ledger-name').val(ledgerName);
		// Remove the fund elements from input and data
		$('#fund-name').val('').data('fund_ledger_code', null);
	}

}

function initTables(tableProps) {
	// Loops through the tables, fetching data from the server
	Promise.all(tableProps.map(fetchTableData))
		.then(tables => {
			// Once we've done that, render the viz
			initChart(chartProps, new Map(tables));
		});
}

function setupChart(chartProps) {
	/*Contains code for setting up the d3 visualization. chartProps should be an object with properties for initializing the chart.*/

	// factory fn for creating d3.line functions 
	function lineFactory (xFunc, yFunc, valueName) {
		//xFunc and yFunc are d3.scale functions, converting values to points in Cartesian space
		// valueName is a key for the valueFunctions object, returning a function to calculate each data point for a given line
		return d3.line()
           		.x(function (d) { 
                	return xFunc(new Date(d.day));
            	})
          		.y(function (d) {
	                return yFunc(valueFunctions[valueName](d));
    	        });
	}


	//amount = Y axis. 
	let y = d3.scaleLinear()
    		.range([chartProps.height, 0]),
		//date of transaction (actual or expected) on the X axis
		//chartProps.dateRange should be an array: [minDate, maxDate]
		// dateRange is set (by the calling fn) after fetching the data initially from the server
		x = d3.scaleTime()
			.domain(chartProps.dateRange)
    		.range([0, chartProps.width]);

	//initiatilze axes with d3 helper functions
	let yAxisFunc = d3.axisLeft(y)
				.tickSizeInner(-chartProps.width)
    			.tickFormat(chartProps.dollars); // current format

	var xAxisFunc = d3.axisBottom(x);

	//initialize three line functions: 
	//one for the rolling balance based on expenditures
	//one based on encumbrances plus expenditures
	//one based on the above plus wishlist
	var linesObj = {actual: lineFactory(x, y, 'actual'),
					projected: lineFactory(x, y, 'projected'),
					proposed: lineFactory(x, y, 'proposed')};
	// add the SVG element and axes
	var chart = d3.select("#chart").append("svg")
    			.attr("width", chartProps.width + chartProps.margin.left + chartProps.margin.right)
    			.attr("height", chartProps.height + chartProps.margin.top + chartProps.margin.bottom)
    			.append("g")
    			.attr("class", "chart")
    			.attr("transform", "translate(" + chartProps.margin.left + "," + chartProps.margin.top + ")");

	chart.append('g')
        .attr('class', 'yaxis');

	chart.append('g')
        .attr('class', 'xaxis')
        .attr('transform', 'translate(0,' + chartProps.height  + ')');

    // initialize the X axis. We don't need to re-do this to re-draw the chart, since the date range stays the same.
    var X = d3.select(".xaxis").call(xAxisFunc);
	// rotate the ticks on the X axis
	X.selectAll('text')
        .style('text-anchor', 'end')
        .attr('dx', '-.8em')
        .attr('dy', '.15em')
        .attr('transform', 'rotate(-65)');    


     // create the legend and data key
     let legend = d3.select("#legend")
		.append('svg')
		.attr('width', 500)
		.attr('height', 100)
		.append('g')
		.attr('class', 'legend')
		.attr('transform', 'translate(50, 0)');
	// Starting point on the Y axis for the first legend, relative to the <g> element
	let legendY = 15;

	for (let value in valueFunctions) {
		// create a new legend path + text for each line shown in the chart	
		legend.append('path')
			.attr('class', `${value}-line`)
			.attr('d', d3.line()([[0, legendY], [30, legendY]]))

		legend.append('text')
			.attr('x', 35)
			.attr('y', legendY)
			.text(`${value} spend`);
		// Move the next one down by a fixed amount
		legendY += 20;	
	}

	displayMetadata();
	
	return [y, yAxisFunc, linesObj];

}

function updateYAxis(y, yAxisFunc, maxAmount) {
	/*Redraws the y axis. Assumes the axis elements have already been created as "g" elements on the SVG space. maxAmount should be a float.*/
	y.domain([0, maxAmount]);
	
	d3.select('.yaxis').call(yAxisFunc);
}

function displayMetadata() {
	/* Fetch stored timestamp for last data update on server side.*/
	fetch('timestamp-data')
		.then(response => response.json())
		.then(data => {
			d3.select('#data-refresh-list')
				.selectAll('li')
				.data(data.rows)
				.enter()
				.append('li')
				.text(d => {
					// Convert timestamp format
					let ts = formatTime(new Date(d.timestamp));
					return `${d.tablename} refreshed on ${ts}.`;
				})
		})
		.catch(e => console.log(e));
}

function drawLines(linesObj, data) {
	/* Appends path elements created from a data set. The linesObj should have the following structure:
		{lineName: lineFunc} 
	where lineName is the name of a line to be used as a class attribute, and lineFunc is the corresponding d3 line function to draw that line, based on the supplied data
		*/
	for (let key in linesObj) {
		let dataToUse;
		// remove any existing line of that type
		d3.select(".chart").select(`.${key}-line`).remove();
		// For actual spend, use only data valid up to the current date
		// TO DO: perform this filter in the SQL query itself, returning nulls for invalid dates
		if (key == 'actual') {
			dataToUse = data.filter(d => {
				return new Date(d.day) <= new Date(Date.now());
			});
		}
		else dataToUse = data;

		// append the new line of that type
		d3.select(".chart")
			.append("path")
			.attr("class", `${key}-line`)
			.datum(dataToUse)
			.attr("d", linesObj[key]);
	}
}

function makeChart(y, yAxisFunc, linesObj, data) {
	/* (Re)draws the chart with new data. */
	// the total allocation value is the same in every row
	let maxAmount = data[0].total_alloc;

	updateYAxis(y, yAxisFunc, maxAmount);

	drawLines(linesObj, data);

}

function fetchChartData(chartType, ledger='', fundCode='') {
	/*Perform the AJAX request for the table data*/
	return fetch(`burndown-data?type=${chartType}&ledger=${ledger}&fundCode=${fundCode}`)
		.then(response => response.json())
		.catch(e => console.log(e));
}

function getColIndex(table, key) {
	/*Helper function to get the numeric index of a given column in the table.*/
	let colMap = table.getColHeader();
	return colMap.indexOf(key);
}

function initChart(chartProps, tableMap) {
	/*Initial function to handle creation of the chart. 
	tableMap contains references to the handsontables created by initTables.
	chartProps is defined as a global object.*/
	fetchChartData('all-funds').then(data => {
		// Get the the burndown data for all funds and ledgers. 
		// Extract the date range from the ordered query results
		chartProps.dateRange = [new Date(data.rows[0].day), 
								new Date(data.rows[data.rows.length-1].day)];
		// Initialize the chart space.
		let [y, yAxisFunc, linesObj] = setupChart(chartProps);
		// Render the chart.
		makeChart(y, yAxisFunc, linesObj, data.rows);
		// Get a reference to the filter plugin for the orders table. Use this to update it based on the user's selection of a fund or ledger on the funds table.
		let ordersTable = tableMap.get('orders-table'),
			ordersFilterPlugin = ordersTable.getPlugin('filters'),
			fundCodeColIndex = getColIndex(ordersTable, 'Fund Code');
		// initialize listeners for chart updates
		$('#do-update').click(e => {
				// clear the filter on the orders table
				ordersFilterPlugin.clearConditions(fundCodeColIndex);
				// get currently selected values for ledger & fund
				let ledgerName = $('#ledger-name').val(),
					fundLedgerCode = $('#fund-name').data('fund_ledger_code');
				if (fundLedgerCode) {
					// new arguments for redrawing the chart based on the selection
					chartArgs = ['single-fund', null, fundLedgerCode];
					// set the filter on the orders table
					ordersFilterPlugin.addCondition(fundCodeColIndex, 
												'contains',
												[fundLedgerCode]);
				}
				else if (ledgerName) {
					chartArgs = ['single-ledger', ledgerName];
					ordersFilterPlugin.addCondition(fundCodeColIndex, 
												'contains',
												[ledgerName]);
				}
				else {
					chartArgs = ['all-funds'];
				}
				ordersFilterPlugin.filter();
				// fetch new data based on the selection then update the chart
				fetchChartData(...chartArgs).then(data => {
					makeChart(y, yAxisFunc, linesObj, data.rows);					
				})
				.catch(err => console.log(err));
		});
		$('#do-reset').click(e => {
			// Resets the fund/ledger selection to all funds/ledgers 
			$('#fund-name').val('').data('fund_ledger_code', '');
			$('#ledger-name').val('');
			$('#do-update').click();
		});
		$('.nav-link').mouseup(e => {
			// This hack is necessary -- at least on a Mac -- to trigger rendering of the table when the user clicks one of the bootstrap nav tabs
			let tab = e.currentTarget.id,
				// get the table associated with the newly active tab
				tableKey = `${tab.split('-')[0]}-table`;
			// call the render method, wait a split second, then call it again.
			tableMap.get(tableKey).render();
			window.setTimeout(() => {
				tableMap.get(tableKey).render();
			}
			, 200);
		});
	})
	.catch(e => console.log(e));
}

initTables(tableProps);