''' 
NAME: Janit Sriganeshaelankovan 
CREATED: July 3, 2018 - 21:36 (EDT)
GOAL: Yahoo Company Stats Scraper
ENVIRONMENT: Base
LAST UPDATE: January 13, 2019 - 22:40 (EDT)
'''




import re
import requests 
from bs4 import BeautifulSoup as soup
import time
from collections import OrderedDict
import csv
import pandas as pd 


''' YAHOO COMPANY STATISTICS '''
#GET THE METERICS
meterics = {}
with open('Yahoo_StatMeterics.txt', 'r') as f:
    for x in f:
        met = x.split(':')
        meterics[met[0]] = met[1].strip('\n').strip(' ')

meterics = OrderedDict(sorted(meterics.items(), key=lambda t: t[0]))
meterics_keys = sorted(meterics.keys())



#GET THE INFO
recheck = []  # used for rechecking failed tickers 

filename = '{}'.format(time.strftime('%m_%d_%y'))
    
with open('{}.txt'.format(filename), 'w') as f:
    f.write('TICKER,')
    for key, val in meterics.items():
        f.write(key + ',')        
    f.write('\n')

        
def get_stats(tickers_list):
    for idx, ticker in enumerate(tickers_list):
        time.sleep(1)
        print('WORKING ON {}: {}'.format(idx, ticker))
        
        resp = requests.get(r'https://ca.finance.yahoo.com/quote/{}/key-statistics'.format(ticker))
        html = resp.text
        page_soup = soup(html, 'lxml')
        body = page_soup.body
        data = str(body.find_all('script'))
        values = data.split(r'"QuoteSummaryStore"')
        
        try:
            with open('{}.txt'.format(filename), 'a',  encoding='utf-8') as f:
                f.write(ticker + ',')
                for key, val in meterics.items():
            #        print('{}, {}'.format(key, val))
                    pattern = '"%s":{(.*?)}' % (val,)
                    d = re.findall(pattern, values[1])
                    
                    if d:
                        pattern2 = r'"fmt":(.*)'
                        value = re.findall(pattern2, d[0])
                    else:
                        value = d 
                    
                    if value:
                        v =  value[0].split(',', 1)[0]
                        v = v.strip('""')
        #                print('{}: {}'.format(key, v))
                        f.write(v + ',')
                    else:
                        value_none = 'NAN'
        #                print('{}: {}'.format(key, value_none))
                        f.write(value_none + ',')
                f.write('\n')
        except Exception as e:
            print('{} was not found: {}'.format(ticker, str(e)))
            recheck.append(ticker)
            
        
        
#CONVERT TXT FILE TO CSV 
with open('{}.txt'.format(filename), 'r') as csvfile:
        csvfile1 = csv.reader(csvfile, delimiter=',')
        with open('{}.txt'.format(filename).replace('.txt','.csv'), 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            for row in csvfile1:
                writer.writerow(row)



#GET ADDITIONAL INFO
filename = '01_13_19'
df = pd.read_csv('{}.csv'.format(filename))
#print(df.columns.values)

df.set_index('TICKER', inplace=True)
#print(df.index.name)

#print(df.dtypes)

meterics_tobeFormatted = ['AverageVol10Day', 'AverageVol3Month', 'EBITDA', 
                          'EnterpriseValue', 'Float', 'GrossProfit', 
                          'LeveredFreeCashFlow', 'MarketCap', 'NetIncometoCommon', 
                          'OperatingCashFlow', 'Revenue', 'SharesOutstanding', 
                          'SharesShort', 'TotalDebt', 'TotalCash']

pd.set_option('display.float_format', lambda x: '%.2f' % x)  # supress scientific notation, returns 2 decimal places

def value_to_float(x):  # CONVERT k,M,B,T TO NUMERIC 
    if type(x) == float or type(x) == int:
        return x
    if 'k' in x:
        if len(x) > 1:
            return float(x.replace('k', '')) * 1000
        return 1000.0
    if 'M' in x:
        if len(x) > 1:
            return float(x.replace('M', '')) * 1000000
        return 1000000.0
    if 'B' in x:
        return float(x.replace('B', '')) * 1000000000
    if 'T' in x:
        return float(x.replace('T', '')) * 1000000000000
    return 'NAN'

for mets in meterics_tobeFormatted:
    df[mets] = df[mets].apply(value_to_float)


df.assign(Sector="", Industry="", BusinessSummary="", Website="", FullTimeEmployees="")


df['Sector'] = df['TotalCash'].apply(str)
df['Industry'] = df['TotalCash'].apply(str)
df['BusinessSummary'] = df['TotalCash'].apply(str)
df['Website'] = df['TotalCash'].apply(str)
df['FullTimeEmployees'] = df['TotalCash'].apply(str)


groups = {'Sector':"sector", 'Industry':"industry", 'BusinessSummary':"longBusinessSummary", 'Website':"website"}

ind = df.index.get_values()
missed = []
missed2 = []  # for second run 
for idx, i in enumerate(ind):
    print('{}:{}'.format(idx, i))
    resp = requests.get(r'https://ca.finance.yahoo.com/quote/{}'.format(i))
    html = resp.text
    page_soup = soup(html, 'lxml')
    body = page_soup.body
    data = str(body.find_all('script'))
    values = data.split(r'"summaryProfile"')
    
    try:
        if values:
            for key, val in groups.items():
                sentence = values[1].split(r'"%s":' % (val, ))
                matches = re.findall(r'\"(.+?)\"',sentence[1])
                if matches[0]:
                    if key == 'Website':
                        try:
                            m = matches[0].split('www.')
                            df.loc[i, key] = m[1]
                        except:
                            m = matches[0].split('www.')
                            df.loc[i, key] = m[0]
                    else:
                        df.loc[i, key] = matches[0]
                else:
                    print("NO {} INFO FOUND ON {}".format(key, i))
                    df.loc[i, key] = 'NAN'
            
            pattern = '"fullTimeEmployees":("*\w*\d*"*)' 
            d = re.findall(pattern, values[1])
            
            try:
                df.loc[i, 'FullTimeEmployees'] = d[0]
            except:
                print("NO EMPLOYEE NUMBER INFO FOUND ON {}".format(i))
                df.loc[i, 'FullTimeEmployees'] = 'NAN' 
        else:
            print("NO DATA FOUND ON {}".format(i))
            df.loc[i, 'Sector'] = 'NAN'
            df.loc[i, 'Industry'] = 'NAN'
            df.loc[i, 'BusinessSummary'] = 'NAN'
            df.loc[i, 'Website'] = 'NAN'
            df.loc[i, 'FullTimeEmployees'] = 'NAN'
            continue
        
    except Exception as e:
        print('{}:{}'.format(i, str(e)))
        missed.append(i)
    
df.to_csv('{}_moreinfo.csv'.format(filename))

























