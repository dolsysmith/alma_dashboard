import aiohttp 
import asyncio
from throttler import Throttler
import json
from pathlib import Path
from urllib import parse

def chunk_list(items, n): 
    '''Create a chunked list of size n. Last segment may be of length less than n.'''
    for i in range(0, len(items), n):  
        yield items[i:i + n] 

def wrap_request(http_type='post'):    
    '''Curries the put_record function to use one of the allowable methods of the aiohttp ClientSession object: put, post, patch'''
    async def put_record(client, results, param_fn, base_url, headers, row):
        '''Makes a single async PUT request, given one or more system ids. 
        client should be an instance of the aiohttp ClientSession class.
        param_fn should be a function that returns a dictionary of parameters for the URL and/or an object to pass as the payload, given the data passed in row. If no payload is desired, the param_fn should return None as the second return value. It can also return an empty dictionary as the first return value if the passed data are meant to be part of the un-parametrized URL, in which case the string formatting function will add it.
        row should be a dictionary of the form {key: value} where the key corresponds to either to a parameter key or a placeholder in the base_url string, and value is the value to assign. It may include other data elements to be passed to the param_fn to create a payload, as necessary.'''
        params, data = param_fn(row)
        base_url = base_url.format(**row)
        arguments = {'params': params,
                    'headers': headers}
        if data is not None:
            arguments['json'] = data
        client_fn = getattr(client, http_type)
        async with client_fn(base_url, **arguments) as session:
            if (session.status != 200) or (session.content_type != 'application/json'):
                error_message = await session.text()
                results.append({'url': str(session.url),
                                'response': error_message})
                return
            else:
                response = await session.json()
        results.append({'url': str(session.url),
                        'response': response}) 
    return put_record

async def fetch_record(client, results, param_fn, base_url, headers, row):
    '''Makes a single async request, given one or more system ids. 
    client should be an instance of the aiohttp ClientSession class.
    param_fn should be a function that returns a dictionary of parameters for the URL, given the data passed in row. It should return an empty dictionary f the passed data are meant to be part of the un-parametrized URL, in which case the string formatting function will add them.
    row should be a dictionary of the form {key: value} where the key corresponds to either to a parameter key or a placeholder in the base_url string, and value is the value to assign.'''
    params = param_fn(row)
    base_url = base_url.format(**row)
    async with client.get(base_url, params=params, headers=headers) as session:
        if session.status != 200:
            results.append({'url': str(session.url),
                    'response': session.status})
            return
        elif session.content_type == 'application/json':
            response = await session.json()
        else:
            response = await session.text()
    results.append({'url': str(session.url),
            'response': response})

async def throttle_request(throttler, async_fn, *args, **kwargs):
    '''Throttles the request. This allows us to re-use the clientsession on each call. '''
    async with throttler:
        return await async_fn(*args, **kwargs)

async def get_records(loop, rows, results, *args, rate_limit=25, http_type='GET'):
    '''From a list of system id's, makes async requests to retrieve the data. 
    loop should be an instance of the asyncio event loop.
    rows should be a list, used to generate requests, with a URL parametrized by param_fn.
    results should be a list to which response data will be added, one at a time.
    rate limit value is used to throttle the calls to a specified rate per second.
    http_type is used to determine which async aiohttp method to use: GET or POST.'''
    throttler = Throttler(rate_limit=rate_limit)
    if http_type == 'GET':
        async_fn = fetch_record
    else:
        async_fn = wrap_request(http_type.lower())
    async with aiohttp.ClientSession() as client:
        awaitables = [loop.create_task(throttle_request(throttler, 
                                                      async_fn,
                                                      client,
                                                      results, 
                                                      *args,
                                                      row=row)) for row in rows]
        await asyncio.gather(*awaitables)
    return len(results)

def run_batch(loop, rows, param_fn, base_url, headers, path_to_files, rate_limit=25, batch_size=1000, http_type='GET'):
    '''Runs an async fetch in batches set by batch_size, saving the results in JSON format to the specified path.
    param_fn should be a function for parametrized base_url based on each id in ids.
    rows should be a list of dictionaries, each dict containing one or more key-value pairs for constructing the URL.'''
    path_to_files = Path(path_to_files)
    for i, batch in enumerate(chunk_list(rows, batch_size)):
        # Reset the results each time through
        results = []
        # Run the loop on the current batch
        loop.run_until_complete(get_records(loop, batch, results, param_fn, base_url, headers, rate_limit=rate_limit, http_type=http_type))
        # Print the first 1000 characters of the last response, in case it's an error message
        print("Head of last result: {}".format(json.dumps(results[-1])[:1000]))
        # Write this batch to the disk
        print("Saving batch {} to disk".format(i))
        with open(path_to_files / 'results_batch-{}.json'.format(i), 'w') as f:
            json.dump(results, f)
        # Yield the batch to the caller for further processing
        yield results

async def test_urls(loop, urls, rate_limit=25):
    '''
    Asynchronously test a batch of URL's, recording their status. 
    Urls should be a list of dictionaries, containing url as a value associated with the key 'url.'
    Each dictionary will be updated with the URL status.
    '''
    async def url_test(client, url):
        '''
        Function that encapsulates the async request.
        '''
        # Un-escape the URL before making the request
        url = parse.unquote_plus(url)
        try:
            async with client.get(url) as session:
                return {'status': session.status}
        except Exception as e:
            return {'status': e}

    # Throttles the requests
    throttler = Throttler(rate_limit=rate_limit)
    # Re-uses the same async client
    async with aiohttp.ClientSession() as client:
        awaitables = [loop.create_task(throttle_request(throttler, 
                                                      url_test,
                                                      client,
                                                      url['url'])) for url in urls]
        results = await asyncio.gather(*awaitables)
    for i, result in enumerate(results):
        urls[i].update(result)
    return urls