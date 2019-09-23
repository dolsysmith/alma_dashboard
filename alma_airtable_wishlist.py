# coding: utf-8

'''
Code for synchronizing an Airtable database with Alma Analytics and a local postgres db (for use by the almadash.js app.)
'''
import requests
import json
import pandas as pd
from lxml import etree
import sqlalchemy
import re
import datetime
import yaml
from pathlib import Path
import logging
from logging import FileHandler
import async_fetch
import asyncio


# Path should lead to the dashboard home directory. Can be changed for testing purposes.
path = Path('./')

# Loading the config objects from YAML
with open(path / 'db/config.yml', 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# Create the postgres engine
# Credentials are in the config file 
engine = sqlalchemy.create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**config['pg_credentials']))

# Rate limit for Airtable
RATE_LIMIT = config['airtable']['rate_limit']

# Set up logging to use a file on disk
wishlist_log = logging.getLogger('wishlist')
wishlist_log.setLevel(logging.INFO)
file_handler = FileHandler(path / config['log_file'])
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
wishlist_log.addHandler(file_handler)
# For debugging
logging.getLogger().addHandler(logging.StreamHandler())

def parse_result(data):
    '''Given a string of XML, parses it and removes the defaults namespace.'''
    # Hack for handling the default namespace
    # https://developers.exlibrisgroup.com/forum/posts/list/478.page
    cleaned_text = data.replace('xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"', '')
    # Need to encode the result first and set up the parser, otherwise lxml throws an error
    xml = cleaned_text.encode('utf-8')
    parser = etree.XMLParser(ns_clean=True, recover=True, encoding='utf-8')
    root = etree.fromstring(xml, parser=parser)
    return root

def xml_to_table(root, columns=None):
    '''Converts an XML Analytics table to a pandas Dataframe.'''
    # If we are paging results, only need to get the columns the first time
    if not columns:
        # Register the namespace map, omitting the empty namespace
        nsmap = {n[0]: n[1] for n in root.xpath('//namespace::*') if n[0]}
        # Get the column headings, which are not elsewhere present in the doc
        columns = dict(zip(root.xpath('.//xsd:element/@name', namespaces=nsmap),  
              root.xpath('.//@saw-sql:columnHeading', namespaces=nsmap)))
    # Build a list of dicts to convert to a dataframe
    # Using this structure so that we can handle missing child nodes in a given row -- pandas will insert NaN values
    records = []
    # Iterate over the rows in the report
    for node in root.xpath('.//Row'):
        # All the children should be cell values with tags like Column0, Column1, etc.
        children = node.xpath('*')
        # Each row is a dictionary mapping its column name to its value
        row = {v: None for v in columns.values()} # This was we make sure we get all columns even when they're empty
        for child in children:
            row[columns[child.tag]] = child.text
        records.append(row)
    return records, columns

def get_report(path, report_name, params, headers):
    '''Given a path to an Analytics report, fetches the report via API and using the above method and converts it to a DataFrame. Handles paging when necessary.'''
    # Don't pass the path as a parameter, or else requests will encode it in a way that OBIEE doesn't like
    # Get the first page of results
    r = requests.get(config['analytics']['base_url'] + config['analytics']['get_url'] + path + report_name,
                 params=params,
                 headers=headers)
    try:
        if r.status_code != 200:
            raise AssertionError('Request failed')
        root=parse_result(r.text)
        table, columns = xml_to_table(root)
        df = pd.DataFrame.from_records(table)
        # Token provided only in the first page of results
    except Exception as e:
        wishlist_log.error('Analytics API error -- {}: {}'.format(e.args, r.text))
        # Return empty DataFrame to avoid typerror when checking return value
        return pd.DataFrame()   
    token = root.find('.//ResumptionToken')
    if token is not None:
        token = token.text
        is_finished = root.xpath('//IsFinished')[0].text
        # Repeat until the "IsFinished flag is set to true
        while is_finished == 'false':
            # after the first query, if there is a resumption token, use that instead of the path
            r = requests.get(config['analytics']['base_url'] + config['analytics']['get_url'] + "?token={}".format(token),
                 headers=headers)
            try:
                if r.status_code != 200:
                    raise AssertionError('Paginated request failed')
                root = parse_result(r.text)
                # Pass in the column dict from the first page of results
                table, columns = xml_to_table(root, columns)
                # Concat with the previous tables
                df = pd.concat([df, pd.DataFrame.from_records(table)])
                is_finished = root.xpath('//IsFinished')[0].text 
            except Exception as e:
                wishlist_log.error('Analytics API error -- {}: {}'.format(e.args, r.text))
                return pd.DataFrame()
    return df.drop('0', axis=1) # Drop the extra index column added by the API

def replace_chars(col_name):
    '''Replaces spaces, parens, and hyphens in a column name with single underscores.'''
    col_name = re.sub(r'-', ' ', col_name)
    col_name = re.sub(r'\s', '_', col_name)
    return re.sub(r'\(|\)', '', col_name)

def clean_col_names(df):
    '''Cleans up the columns names from Analytics, making them safe for SQL.'''
    columns = [c.strip().lower() for c in df.columns]
    columns = [replace_chars(c) for c in columns]
    df.columns = columns
    return df

def compute_ledger_name(fund_code):
    '''Assumes each fund code in Alma starts with a six digit ledger name. TO DO: Don't hard code this here.'''
    return fund_code[:6]

def get_alma_funds():
    '''Fetches a list of active, allocated funds from the Alma acquisitions API.
    Used to supplement Analytics reports with incorrect encumbrance amounts.'''
    params = {'limit': 100}
    headers = {'Authorization': 'apikey {}'.format(config['acquisitions']['api_key']),
          'Accept': 'application/json'}
    funds = requests.get(config['acquisitions']['base_url'], headers=headers, params=params)
    try:
        funds = funds.json()
        # Return a DataFrame with the fund code and available balance
        funds_table = pd.DataFrame.from_records([{k: v for k, v in f.items() 
                                            if k in ['code', 'available_balance']} 
                                        for f in funds['fund']])
        # Convert string to float for balance
        funds_table.available_balance = funds_table.available_balance.astype(float)
        funds_table = funds_table.rename(columns={'available_balance': 'balance_available'})
        return funds_table
    except Exception as e:
        print(e)
        wishlist_log.error('Acquisitions API error -- {}: {}'.format(e.args, r.text))
        return pd.DataFrame()

# Don't use while Analytics bug prohibits accurate encumbrance reporting
#def compute_balance_available(df):
#    '''Computes remaining balance, using Alma Analytics columns.'''
#    df['balance_available'] = df.transaction_allocation_amount - (df.transaction_encumbrance_amount + df.transaction_expenditure_amount)
#    return df
def compute_balance_available(df):
    '''Add balance available column to fund table from Analytics, fetching the balance from the Alma API.'''
    balance_table = get_alma_funds()
    if balance_table.empty:
        raise AssertionError('Could not fetch updated balance from Alma. Funds table will not be updated.') 
    return df.merge(balance_table, left_on='fund_ledger_code',
                                    right_on='code').drop('code', axis=1)

def normalize_dates(df, fiscal_year_start, date_column):
    '''Given a DataFrame, a fiscal year start date, and a date column, normalizes those dates that fall before the start of the current fiscal year to dates in the current year.
    Dates are normalized by adding a year.'''
    # Get the (fiscal) year for the current fiscal year start date
    fiscal_year = pd.to_datetime(fiscal_year_start).to_period('A-JUN').year
    # Calculate the difference between the current fiscal year and the fiscal year of the dates in date_column
    df['fiscal_year_delta'] = fiscal_year - df[date_column].dt.to_period('A-JUN').dt.year
    # Create a temporary column to hold the pandas offset objects -- used to calculate the delta between years
    df['offsets'] = df.fiscal_year_delta.apply(lambda x: pd.offsets.DateOffset(years=x) if x > 0 else pd.offsets.DateOffset(years=0))
    # Recalculate the date_column, using the offset 
    df[date_column] = df[date_column] + df.offsets
    return df.drop(['offsets', 'fiscal_year_delta'], axis=1)

def fetch_analytics_data():
    '''Main function to refresh data from Alma. Fetches Analytics reports, converts them to pandas DataFrames, and saves them in a dictionary.'''
    headers = {'Authorization': 'apikey {}'.format(config['analytics']['api_key'])}
    params = {'limit': 1000}
    reports = {}
    for report_name in config['analytics']['report_names']:
        try:
            report = get_report(config['analytics']['path'], 
                                report_name,
                                params=params,
                                headers=headers)
            # Test for error on API 
            if report.empty:
                raise AssertionError('Report {} not retrieved'.format(report_name))
            report = clean_col_names(report)
            # Cast the amount type to float
            for c in report.columns:
                if c.endswith('amount'):
                    report[c] = report[c].astype('float')
                elif c.endswith('date'):
                    report[c] = pd.to_datetime(report[c],
                                      errors='coerce')
            # Compute the ledger column --> We don't do this in Analytics, because the API doesn't return custom column names
            if 'fund_ledger_code' in report.columns:
                report['ledger_name'] = report.fund_ledger_code.apply(compute_ledger_name)
            # Add the balance available from the Alma API's (workaround for Analytics bug)
            if report_name == 'funds_table':
                report = compute_balance_available(report)
            reports[report_name] = report
        except Exception as e:
            # If we can't get a particular report, log the error, skip it, and continue. 
            # That way we can fall back on the last loaded good data.
            wishlist_log.error(e.args)
            continue
    if 'pol_table' in reports:
        # For the table of POL's normalize the renewal dates
        reports['pol_table'] = normalize_dates(reports['pol_table'].copy(), 
                                       config['fiscal_period']['start_date'],
                                               'renewal_date')
    return reports

def load_analytics_data(reports):
    '''Loads a dictionary of pandas DataFrames to a local postgres database.'''
    # Need explicitly to DROP the tables before re-loading, because otherwise the materialized views will throw a dependency error
    drop_query = 'drop table if exists {table_name} cascade'
    for r in reports:
        engine.execute(drop_query.format(table_name=r))
    for name, table in reports.items():
        try:
            # Add timestamp
            table['timestamp'] = datetime.datetime.today()
            table.to_sql(name, engine, 
                 if_exists='replace',
                 index=False)
        except Exception as e:
            wishlist_log.error('SQL error on {} table: {}'.format(name, e))


def refresh_views():
    '''Recreates the materialized views to reflect the updated data.'''
    # Drop the dates view, since it won't be dropped in the DROP TABLE CASCADE call above
    engine.execute('drop materialized view if exists dates')
    # Maps parameters to the names of queries and query paramaters
    param_dict = {'dates_view': {'start_date': config['fiscal_period']['start_date'],
                            'last_valid_renewal': config['fiscal_period']['last_valid_renewal']},
             'expenditures_view': {'start_date': config['fiscal_period']['start_date'],
                                  'end_date': config['fiscal_period']['end_date']},
             'encumbrances_view': {}}
    for key, value in config['sql'].items():
        # Load the SQL for creating each view
        with open(path / 'db/{}'.format(value), 'r') as f:
            query = f.read()
            try:
                engine.execute(query, **param_dict[key])
            except Exception as e:
                wishlist_log.error('SQL error on mat view {}: {}'.format(key, e))

def check_results(results):
    '''Error handler to check results of batch updates and log errors.
    Removes any errors from the list of results before returning the pruned list.'''
    good_results = []
    for result in results:
        try:
            assert 'id' in result['response']
        except AssertionError:
            wishlist_log.error('Airtable API error: failures {} in POST operation: {}'.format(result['response'], result['url']))
            continue
        good_results.append(result)
    return good_results

def wrap_param_fn(col_map):
    '''Wrapper function using closures to bind a column map to the function for creating parametrized POST/PATCH queries with async_batch.'''
    def param_fn(row):
        '''Creates the payload for the POST request for Airtable.
        First return value is an empty parameters object for the request function.'''
        params = {}
        data = {'fields': {col_map[k]: v for k,v in row.items() 
                                        if k in col_map}
                }
        return params, data
    return param_fn

def convert_airtable_results(results):
    '''Helper function to make a pandas DataFrame out of the results returned from Airtable API operations. Adds the unique Airtable row id as an additional column.'''
    table = pd.DataFrame.from_records([r['response']['fields'] for r in results])
    table['id'] = pd.Series([r['response']['id'] for r in results])
    return table

def load_table_init(table, col_map, url, headers, loop, rate_limit=RATE_LIMIT, file_path=path / 'airtable/data'):
    '''Makes the initial load of a DataFrame into a corresponding Airtable table.
    Only fields present in col_map will be used.
    First argument should be a DataFrame.
    col_map should be a dictionary mapping the DataFrame columns to Airtable columns
    Unique record ideas will be added to the original DataFrame and returned for future reference.'''
    # Store the results returned from Airtable in order to extract the ID's
    results = []
    # Iterable for the async_fetch function
    rows = [i._asdict() for i in table.itertuples(index=False)]
    param_fn = wrap_param_fn(col_map)
    for batched_result in async_fetch.run_batch(loop,
                                                rows,
                                                param_fn,
                                                url,
                                                headers,
                                                file_path,
                                                rate_limit=rate_limit,
                                                batch_size=100,
                                                http_type='POST'):
        results.extend(batched_result)
    # Error checking
    return check_results(results)



def update_airtable(table, url, headers, loop, rate_limit=RATE_LIMIT, file_path=path / 'airtable/data'):
    '''Updates an existing Airtable table, given a DataFrame. DataFrame should contain an Airtable row Id and the table field value to update.
    Columns should be <id> and <[Airtable field name]>'''
    def update_params(row):
        params = {}
        data = {'fields': {k: v for k, v in row.items() 
                          if k != 'id'}}
        return params, data
    rows = [i._asdict() for i in table.itertuples(index=False)]
    results = []
    for batched_result in async_fetch.run_batch(loop,
                                                 rows,
                                                 update_params,
                                                 url,
                                                 headers,
                                                 file_path,
                                                 rate_limit=rate_limit,
                                                 batch_size=100,
                                                 http_type='PATCH'):
        results.extend(batched_result)
    # Check results
    return check_results(results)

def get_airtable_rows(url, headers, params):
    '''Retrieve a set of resulst from Airtable (single GET request).'''
    resp = requests.get(url,
            params=params,
            headers=headers)
    try:
        if resp.status_code != 200: 
            raise AssertionError('Airtable API error: GET request failed for {}'.format(url))
        data = resp.json()
        table = pd.DataFrame.from_records([r['fields'] for r in data['records']])
        # Add the unique ID returned 
        table['id'] = pd.Series([r['id'] for r in data['records']])
        return table
    except Exception as e:
        wishlist_log.error(e)
        return pd.DataFrame()

def fetch_new_orders(wishlist_funds_table, orders_url, allocations_url, headers):
    '''Get rows from the Airtable wishlist orders table and joins with the table of wishlist fund allocations.
    Argument should be a DataFrame containing updated fund information.'''
    # If the row has a POL, ignore it
    params = {'filterByFormula': '{pol_number} = ""'}
    try:
        wishlist_orders_table = get_airtable_rows(orders_url,
                                         headers,
                                          params)
        if wishlist_orders_table.empty:
            raise AssertionError('Error fetching orders from Airtable.')
        # Drop the license column, because it contains nested data
        wishlist_orders_table = wishlist_orders_table.drop('license', axis=1)    
        # Get the allocations for these orders
        params = {}
        wishlist_allocations_table = get_airtable_rows(allocations_url,
                                         headers,
                                          params)
        if wishlist_allocations_table.empty:
            raise AssertionError('Error fetching order allocations from Airtable')
        # First, unroll the lists containing the order ids and fund ids --- these should each have only a single value, since each row corresponds to one allocation
        wishlist_allocations_table.order_id = wishlist_allocations_table.order_id.apply(lambda x: x[0])
        wishlist_allocations_table.fund_to_allocate = wishlist_allocations_table.fund_to_allocate.apply(lambda x: x[0])
        # Merge on the table of funds
        # wishlist_allocations_table is an intermediate one, so we flag the columns so we can drop them later
        wishlist_allocations_table = wishlist_allocations_table.merge(wishlist_funds_table, 
                                 left_on='fund_to_allocate', 
                                 right_on='id',
                                 suffixes=('_merge', ''))
        # Now merge with the orders table
        wishlist_orders_table = wishlist_orders_table.merge(wishlist_allocations_table, 
                            left_on='id',
                           right_on='order_id',
                           suffixes=('', '_merge'))
        # Remove the extraneous columns
        wishlist_orders_table = wishlist_orders_table.drop([c for c in wishlist_orders_table.columns if c.endswith('merge')], axis=1)    
        # Add timestamp
        wishlist_orders_table['timestamp'] = datetime.datetime.today()
        # Save to the postgres db
        return wishlist_orders_table.to_sql('wishlist_orders_table', engine, if_exists='replace', index=False)
    except Exception as e:
        wishlist_log.error(e)

def do_airtable_updates(reports, init=False):
    '''Parent function for handling Airtable updates: getting, patching, and posting data.
    Reports should be a dictionary of DataFrames returned from the fetch_analytics_data function.
    Set the init flag to True if starting a new Airtable database.
    '''
    GET_HEADERS = {'Authorization': 'Bearer {api_key}'.format(api_key=config['airtable']['api_key'])}
    # Use for patch, put, and post
    PATCH_HEADERS = {'Authorization': 'Bearer {api_key}'.format(api_key=config['airtable']['api_key']),
                          'Content-Type': 'application/json'}
    AT_URL = config['airtable']['base_url']
    # Airtable API rate limit: requests per second
    RATE_LIMIT = 5
    # Mapping Analytics table columns to Airtable columns
    fund_table_col_map = config['airtable']['fund_table_col_map']
    # Get the event loop to pass the async functions
    loop = asyncio.get_event_loop()
    # Do the initial load of Alma funds
    if init:
        # Optional: Limit to a subset of funds
        # Remove these entries from the config.yml file if not needed
        if config['airtable'].get('fund_names') and config['airtable'].get('ledger_names'):
            funds_to_load = reports['funds_table'].loc[reports['funds_table'].fund_ledger_name.isin(config['airtable']['fund_names']) | 
                                              reports['funds_table'].ledger_name.isin(config['airtable']['ledger_names'])].copy()
        else:
            funds_to_load = reports['funds_table']
        airtable_funds = load_table_init(funds_to_load, 
                               fund_table_col_map,
                               AT_URL.format(table_name='funds_available'),
                               PATCH_HEADERS,
                               loop)
    else:
        # Get the stable Airtable row id's for updating with new Alma data
        with open(path / 'db/{}'.format(config['airtable']['sql']['update_at_funds']), 'r') as f:
            # This query should join the Airtable fund id and fund code with the latest balance on the fund from Alma
            update_at_funds_query = f.read()         
        try:
            funds_to_update = pd.read_sql(update_at_funds_query, engine)
            # If the funds table doesn't exist locally, get it from Airtable first
        except Exception as e:
            wishlist_log.error('Unable to get Airtable funds stored locally. Fetching remote.')
            funds_to_update = get_airtable_rows(AT_URL.format(table_name='funds_available'),
                                                GET_HEADERS,
                                                params={})
        # Update these funds on Airtable
        # TO DO: don't hard code column names
        airtable_funds = update_airtable(funds_to_update[['id', 'alma_balance_available']], 
                    AT_URL.format(table_name='funds_available') + '/{id}',
                    PATCH_HEADERS,
                    loop)
    try:
        # Load the fund information and associated Airtable id's for use in updating
        airtable_funds = convert_airtable_results(airtable_funds)
        airtable_funds['timestamp'] = datetime.datetime.today()
        airtable_funds.to_sql('airtable_funds', engine, if_exists='replace', index=False)
    except Exception as e:
        wishlist_log.error('Error loading Airtable funds to postgres: {}'.format(e))
    # Get the latest order information
    return fetch_new_orders(airtable_funds,
                            orders_url=AT_URL.format(table_name='new_orders'),
                            allocations_url=AT_URL.format(table_name='allocation_to_orders'),
                            headers=GET_HEADERS)
# Main program loop
if __name__ == '__main__':
    # 1. Get latest data from Analytics
    print('getting latest data from Analytics...')
    reports = fetch_analytics_data()
    # 2. Load local postgres tables
    print('loading postgres tables...')
    load_analytics_data(reports)
    # 3. Update materialized views for faster search
    print('updating postgres views')
    refresh_views()
    # 4. a. Update Airtable data with Alma funds
    #.   b. Load local postgres tables with new order on Airtable
    print('getting Airtable data and updating...')
    do_airtable_updates(reports)


