import requests
import urllib
import pandas as pd
import os
import zipfile
import sqlalchemy as db
import time
import datetime
import argparse
import os
import json
import traceback
import sys

def download_finra_data(filepath, date = datetime.datetime.today()):
    datestr = date.strftime("%Y%m%d")
    links = {'NASDAQCHI':'http://regsho.finra.org/FNQCshvol{}.txt'.format(datestr),
        'NASDAQCAR':'http://regsho.finra.org/FNSQshvol{}.txt'.format(datestr),
        'NYSE':'http://regsho.finra.org/FNYXshvol{}.txt'.format(datestr),
        'NMS': 'http://regsho.finra.org/CNMSshvol{}.txt'.format(datestr)}
    for exch, link in links.items():
        try:
            response = urllib.request.urlopen(link)
            zippedData = response.read()
            output = open(filepath + exch + '_' + datestr + '.txt','wb')  #
            output.write(zippedData)
            output.close()
        except:
            print("Failed download for ", date)
            continue

def process_finra_data(filepath, con, debug, date = datetime.datetime.today()):
    date_column = date.strftime("%Y-%m-%d")
    datestr = date.strftime("%Y%m%d")
    exchanges = ['NASDAQCAR', 'NASDAQCHI', 'NYSE']
    df = pd.DataFrame(columns=['Symbol'])
    no_vol_exch  = []
    try:
        print("Processing FINRA data for date: ", date_column)
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
                                   exch+'_'+'TotalVolume': int(token[4]),
                                   })
                if len(output)<=1:
                    no_vol_exch.append(exch)
                else:
                    df = pd.merge(df, pd.DataFrame(output), on='Symbol', how= 'outer')
        merged = df
        merged['Date'] = date_column
        merged['NMS_ShortVolume'] = 0
        merged['NMS_TotalVolume'] = 0
        for exch in exchanges:
            if exch in no_vol_exch:
                merged[exch+'_ShortRatio'] = 0
                merged[exch+'_ShortVolume'] = 0
                merged[exch+'_TotalVolume'] = 0
            else:
                merged[exch+'_ShortRatio'] = merged[exch+'_ShortVolume'] / merged[exch+'_TotalVolume']
                merged['NMS_ShortVolume'] = merged['NMS_ShortVolume'] + merged[exch + '_ShortVolume']
                merged['NMS_TotalVolume'] = merged['NMS_TotalVolume'] + merged[exch + '_TotalVolume']
        merged['NMS_ShortRatio'] = merged['NMS_ShortVolume'] / merged['NMS_TotalVolume']
        display_cols = ['Symbol', 'NMS_ShortRatio', 'NASDAQCAR_ShortRatio', 'NASDAQCHI_ShortRatio', 'NYSE_ShortRatio', 'NMS_ShortVolume']
        merged =merged[merged['NMS_TotalVolume'] > 1000]
        merged = merged.fillna(0)
        if debug:
            print(merged)
        else:
            merged.to_sql('Dark', con, if_exists='append')
    except Exception as e:
        print(" Failed to add {} data into database due to {}".format(date_column, e))
        # print ('-'*60)
        # traceback.print_exc(file=sys.stdout)
        # print ('-'*60)

def setup(start_date, end_date, filepath, con, debug):
    dates = pd.bdate_range(start=start_date, end=end_date)
    for date in dates:
        download_finra_data(filepath, date)
        process_finra_data(filepath, con, debug, date)

def update(filepath, con, debug):
    df = pd.read_sql_query("SELECT * FROM Dark WHERE Symbol = 'AAPL' ", con) # use aapl data as example to get last updated date since aapl is shorted everyday
    last_update = df.sort_values('Date').Date.iloc[-1]
    print("Last Updated On: ", last_update)
    dates = pd.bdate_range(start=last_update, end=datetime.datetime.today())
    for date in dates[1:]:
        print("Updating ", date)
        download_finra_data(filepath, date)
        process_finra_data(filepath, con, debug, date)

if __name__ == '__main__':
    # load config
    debug = False
    config = json.load(open("config.json","r"))
    start_date = config['start_date']
    end_date = config['end_date']
    filepath = config['finra_data_path']
    db_path = config['db_path']
    # "sqlite:///C:/Users/zheji/Desktop/TradingTools/shortData.sqlite"
    con = db.create_engine(db_path)

    #setup or update database based on user choice
    parser = argparse.ArgumentParser()
    parser.add_argument('--setup', action='store_true', help='Initial Setup for Database')
    args = parser.parse_args()
    if args.setup:
        setup(start_date, end_date, filepath, con, debug)
    else:
        update(filepath, con)