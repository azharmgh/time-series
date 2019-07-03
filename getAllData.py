
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta


firsttime = True
filepath = r'C:\Users\azhar\time-series\data2016-2019-1.csv'

def getData(targeturl, startdate):

    # Get the page
    #response = requests.get("http://ets.aeso.ca/ets_web/ip/Market/Reports/PublicSummaryAllReportServlet?beginDate=06012018&endDate=06052018&contentType=html")
    response = requests.get(targeturl)
    doc = BeautifulSoup(response.text, 'html.parser')

    # Grab all of the rows
    row_tags = doc.find_all('tr')
    list_rows = []
    for row in row_tags:
        cells = row.find_all('td')
        str_cells = str(cells)
        #clean = re.compile('<.*?>')
        clean2 = (re.sub('<.*?>', '',str_cells))
        clean3 = clean2.replace(']','').replace('[','')
        list_rows.append(clean3)
        #print(clean3) 
    df = pd.DataFrame(list_rows)
    df1 = df[0].str.split(',', expand=True)
    df3 = df1.iloc[6:]
    nocols = df3.shape[1]
    
    #some days missing hour 2 or duplicate hour 2
    if (nocols > 27 ):
        print('more than 27 columns')
        df3 = df3.iloc[:,[0,1,2,3,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27]]
    
    if (nocols < 27):
        print('lessl than 27 columns')
        df3 = df3.iloc[:,[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]]
        df3[26]=0.0
        
        
    df3.columns = ['PoolParticipantID', 'AssetType', 'AssetID', 'Hour1', 'Hour2', 'Hour3', 'Hour4','Hour5','Hour6', 'Hour7', 'Hour8',
                    'Hour9', 'Hour10', 'Hour11', 'Hour12', 'Hour13', 'Hour14', 'Hour15', 'Hour16', 'Hour17', 'Hour18', 'Hour19',
                    'Hour20', 'Hour21', 'Hour22', 'Hour23', 'Hour24']

    hour_vars = ['Hour1', 'Hour2', 'Hour3', 'Hour4','Hour5','Hour6', 'Hour7', 'Hour8',
                    'Hour9', 'Hour10', 'Hour11', 'Hour12', 'Hour13', 'Hour14', 'Hour15', 'Hour16', 'Hour17', 'Hour18', 'Hour19',
                    'Hour20', 'Hour21', 'Hour22', 'Hour23', 'Hour24']

    #unpivot the data set to make it more compatible with time series
    df_up = df3.melt(id_vars=['PoolParticipantID','AssetType', 'AssetID'], value_vars=hour_vars, var_name='hours', value_name='MWh')
   

    df_up = df_up.replace(' ', '', regex=True)
    df_up = df_up[df_up['AssetID'].str.match('^ENC3$')]
   
    global firsttime
    global filepath
   
    
    records = df_up.shape[0]
    
    if (records > 0):
       if (firsttime):
            df_up['Date'] = startdate
            df_up['TimeStamp'] = df_up.apply(lambda row:getTimeStamp(row['Date'],row['hours']), axis=1 )
            df_up.to_csv(filepath, index = None, header=True)
            firsttime = False
       else:
            df_up['Date'] = startdate
            df_up['TimeStamp'] = df_up.apply(lambda row:getTimeStamp(row['Date'],row['hours']), axis=1 )
            with open(filepath, 'a') as f:
                df_up.to_csv(f, index= None, header=False)
    else:
        print("no ENC3 records found")



def getAllData():
    targeturl = 'http://ets.aeso.ca/ets_web/ip/Market/Reports/PublicSummaryAllReportServlet?beginDate={}&endDate={}&contentType=html'
    startDate =  datetime(2016, 1, 1)
    endDate   =  datetime(2019,6,30)
    d1 = startDate
    i = 0
    while d1 < endDate:
        d1 = startDate + timedelta(days=i)
        i = i + 1
        #print (d1.date())
        #print (d1.strftime("%m%d%Y"))
        retval = targeturl.format(d1.strftime("%m%d%Y"), d1.strftime("%m%d%Y"))
        print(retval)
        getData(retval, d1) 

def getTimeStamp(startdate,hourstr):
    
    dhours = {'Hour1':'01:00:00', 'Hour2':'02:00:00', 'Hour3':'03:00:00', 'Hour4':'04:00:00', 'Hour5':'05:00:00'
         , 'Hour6':'06:00:00', 'Hour7':'07:00:00', 'Hour8':'08:00:00', 'Hour9':'09:00:00', 'Hour10':'10:00:00'
         , 'Hour11':'11:00:00', 'Hour12':'12:00:00', 'Hour13':'13:00:00', 'Hour14':'14:00:00', 'Hour15':'15:00:00'
         , 'Hour16':'16:00:00', 'Hour17':'17:00:00', 'Hour18':'18:00:00', 'Hour19':'19:00:00', 'Hour20':'20:00:00'
         , 'Hour21':'21:00:00', 'Hour22':'22:00:00', 'Hour23':'23:00:00', 'Hour24':'00:00:00'}
    
    dt = startdate.strftime('%Y-%m-%d') 
    dd =  dt + ' ' + dhours[hourstr]
    return dd



def main():    
    getAllData()
   


if __name__ == "__main__":
    main()

