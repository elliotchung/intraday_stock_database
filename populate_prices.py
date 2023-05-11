import config
import json
import requests
from datetime import datetime
import aiohttp, asyncpg, asyncio
import time

headers={'Authorization': f"Bearer {config.access_token}", 'Accept': 'application/json'}

async def write_to_db(connection, data):
    await connection.copy_records_to_table('stock_price', records=data)

async def get_price(pool, stock_id, symbol_param):
    try: 
        async with pool.acquire() as connection:
            async with aiohttp.ClientSession() as session:
                async with session.get(url=config.API_URL_TIME_SALES, 
                                    params=symbol_param, 
                                    headers=headers) as response:
                    resp = await response.read()
                    response = json.loads(resp)
                    data = [(stock_id, datetime.strptime(quote['time'], '%Y-%m-%dT%H:%M:%S'), 
                        round(quote['open'],2), round(quote['high'],2), round(quote['low'],2), round(quote['close'],2), 
                        quote['volume']) for quote in response['series']['data']]
                    await write_to_db(connection, data)
    except Exception as e:
        print("Unable to get url {} due to {}.".format(symbol_param, e.__class__))

async def get_prices(pool, symbol_params):
    try:
        # schedule aiohttp requests to run concurrently for all symbols
        ret = await asyncio.gather(*[get_price(pool, stock_id, symbol_params[stock_id]) for stock_id in symbol_params])
        print("Finalized all. Returned  list of {} outputs.".format(len(ret)))
    except Exception as e:
        print(e)

async def get_stocks():
    # create database connection pool
    pool = await asyncpg.create_pool(user=config.DB_USER, 
                                     password=config.DB_PASS, 
                                     database=config.DB_NAME, 
                                     host=config.DB_HOST, 
                                     command_timeout=60)
    
    # get a connection
    async with pool.acquire() as connection:
        stocks = await connection.fetch("""
                                        SELECT * FROM stock LIMIT 10;
                                        """)
        symbol_params = {}
        for stock in stocks:
            symbol_params[stock['id']] = {'symbol': f"{stock['symbol']}", 
                                            'interval': '5min', 
                                            'start': '2023-04-03 09:30', 
                                            'end': '2023-04-03 10:00', 
                                            'session_filter': 'open'}
    print(symbol_params)
    await get_prices(pool, symbol_params)


start = time.time()

loop = asyncio.get_event_loop()
loop.run_until_complete(get_stocks())

end = time.time()

print(f"Time elapsed: {end - start}")
