import requests
import urllib
import pandas as pd
import os
import zipfile
import sqlalchemy as db
import json
import datetime
import argparse

def download_cboe_data(filepath, date = datetime.datetime.today()):
    splited = date.strftime("%Y-%m-%d").split('-')
    year = splited[0]
    month = splited[1]
    datestr = ''.join(splited)
    links = {'BZX':"https://markets.cboe.com/us/equities/market_statistics/short_sale/{}/{}/BATSshvol{}.txt.zip-dl?mkt=bzx".format(year, month, datestr), 
    		 'BYX':"https://markets.cboe.com/us/equities/market_statistics/short_sale/{}/{}/BYXXshvol{}.txt.zip-dl?mkt=byx".format(year, month, datestr), 
    		 'EDGA':"https://markets.cboe.com/us/equities/market_statistics/short_sale/{}/{}/EDGAshvol{}.txt.zip-dl?mkt=edga".format(year, month, datestr), 
    		 'EDGX':"https://markets.cboe.com/us/equities/market_statistics/short_sale/{}/{}/EDGXshvol{}.txt.zip-dl?mkt=edgx".format(year, month, datestr)}

    for exchange, link in links.items():
        try:
            print("Downloading data... Exch: {} Date: {}".format(exchange, datestr))
            response = urllib.request.urlopen(link)
        except Exception as e:
            print("Error occured during download: ", e)
            continue
        #save data to zipfile format
        zippedData = response.read()
        output = open('{}_{}'.format(exchange,datestr),'wb')  #
        output.write(zippedData)
        output.close()

        #read zipfile
        zfobj = zipfile.ZipFile('{}_{}'.format(exchange, datestr))
        for name in zfobj.namelist():
            uncompressed = zfobj.read(name)
            output = open('DIX_data/{}_{}.txt'.format(exchange, datestr),'wb')
            output.write(uncompressed)
            output.close()
        zfobj.close()
        os.remove('{}_{}'.format(exchange,datestr))

def process_cboe_data(filepath, con, debug, date = datetime.datetime.today()):
    date_column = date.strftime("%Y-%m-%d")
    datestr = date.strftime("%Y%m%d")
    exchanges = ['BZX', 'BYX', 'EDGA', 'EDGX']
    dfs = []
    print("Processing CBOE data for date: ", date_column)
    try:
	    for exch in exchanges:
	        with open(filepath + exch + '_' + datestr + '.txt') as f:
	            next(f)
	            output = []
	            for line in f:
	                token = line.split('|')
	                if len(token) <=1:
	                    continue
	                output.append({'Symbol': token[1],
	                               exch+'_'+'ShortVolume': int(token[2]),
	                               exch+'_'+'TotalVolume': int(token[3]),
	                               })
	            dfs.append(pd.DataFrame(output))

	    merged = dfs[0]
	    for df in dfs[1:]:
	    	merged = pd.merge(merged, df, on='Symbol', how='outer')
	    merged = merged.fillna(0)
	    merged['Date'] = date_column
	    merged['CBOE_ShortVolume'] = 0
	    merged['CBOE_TotalVolume'] = 0
	    for exch in exchanges:
	        merged[exch+'_ShortRatio'] = merged[exch+'_ShortVolume'] / merged[exch + '_TotalVolume']
	        merged['CBOE_ShortVolume'] = merged['CBOE_ShortVolume'] + merged[exch + '_ShortVolume']
	        merged['CBOE_TotalVolume'] = merged['CBOE_TotalVolume'] + merged[exch + '_TotalVolume']
	    merged['CBOE_ShortRatio'] = merged['CBOE_ShortVolume'] / merged['CBOE_TotalVolume']
	    merged = merged[merged['CBOE_TotalVolume'] > 1000]  # min threshold to add to database 
	    merged = merged.fillna(0)
	    if debug:
	        print(merged)
	    else:
	    	merged.to_sql('Lit', con, if_exists='append')
    except Exception as e:
       print("Failed to add {} data into database due to {}".format(date_column, e))

def setup_cboe(start_date, end_date, filepath, con, debug):
    dates = pd.bdate_range(start=start_date, end=end_date)
    con.execute("DROP TABLE IF EXISTS Lit")
    for date in dates:
        #download_cboe_data(filepath, date)
        process_cboe_data(filepath, con, debug, date)

def update_cboe(filepath, con, debug):
    df = pd.read_sql_query("SELECT * FROM Lit WHERE Symbol = 'AAPL' ", con) # use aapl data as example to get last updated date since aapl is shorted everyday
    last_update = df.sort_values('Date').Date.iloc[-1]
    print("Last Updated On: ", last_update)
    dates = pd.bdate_range(start=last_update, end=datetime.datetime.today())
    for date in dates[1:]:
        print("Updating ", date)
        download_cboe_data(filepath, date)
        process_cboe_data(filepath, con, date, debug)

if __name__ == '__main__':
    # load config
    debug = False
    config = json.load(open("config.json","r"))
    start_date = config['start_date']
    end_date = config['end_date']
    filepath = config['cboe_data_path']
    db_path = config['db_path']
    con = db.create_engine(db_path)

    #setup or update database based on user choice
    parser = argparse.ArgumentParser()
    parser.add_argument('--setup', action='store_true', help='Initial Setup for Database')
    args = parser.parse_args()
    if args.setup:
        setup_cboe(start_date, end_date, filepath, con, debug)
    else:
        update_cboe(filepath, con, debug)