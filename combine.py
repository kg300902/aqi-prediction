from aqiavhand import avg_data_2013,avg_data_2014,avg_data_2015,avg_data_2016
import requests
import sys
import pandas as pd
from bs4 import BeautifulSoup
import os
import csv


def met_data(month, year):
    
    file_html = open(r'C:\Users\gkuna\Desktop\pro aqi\PRO AQI\Html_Data/{}/{}.html'.format(year,month), 'rb')##open the html file for required month and year
    plain_text = file_html.read()## we have to deffine read function for reading file

    tempD = []
    finalD = []

    soup = BeautifulSoup(plain_text, "lxml")#we pass file text 
    for table in soup.findAll('table', {'class': 'medias mensuales numspan'}):#we have to extract table and table class is medias mensuales numspan
        for tbody in table:# in html code inside tbody have tr#t body list of table rows
            for tr in tbody:# inside tr we have text table head and pickup all the texts
                a = tr.get_text()#pickup all the texts
                tempD.append(a)#appending it to temporary data variable

    rows = len(tempD) / 15# we have 15 features in one row ,,, for tempd tempd is a list ....gives no of rows

    for times in range(round(rows)):#iteraating throuh each and every row
        newtempD = []
        for i in range(15):
            newtempD.append(tempD[0])
            tempD.pop(0)
        finalD.append(newtempD)

    length = len(finalD)

    finalD.pop(length - 1)
    finalD.pop(0)

    for a in range(len(finalD)):
        finalD[a].pop(6)
        finalD[a].pop(13)
        finalD[a].pop(12)
        finalD[a].pop(11)
        finalD[a].pop(10)
        finalD[a].pop(9)
        finalD[a].pop(0)

    return finalD

def data_combine(year, cs):
    for a in pd.read_csv(r'PRO AQI/Real-Data/real_' + str(year) + '.csv', chunksize=cs):
        df = pd.DataFrame(data=a)
        mylist = df.values.tolist()
    return mylist


if __name__ == "__main__":
    if not os.path.exists("PRO AQI/Real-Data"):
        os.makedirs("PRO AQI/Real-Data")
    for year in range(2013, 2017):
        final_data = []
        with open('PRO AQI/Real-Data/real_' + str(year) + '.csv', 'w') as csvfile:
            wr = csv.writer(csvfile, dialect='excel')
            wr.writerow(
                ['T', 'TM', 'Tm', 'SLP', 'H', 'VV', 'V', 'VM', 'PM 2.5'])
        for month in range(1, 13):
            temp = met_data(month, year)
            final_data = final_data + temp
            
        pm = getattr(sys.modules[__name__], 'avg_data_{}'.format(year))()

        if len(pm) == 364:
            pm.insert(364, '-')

        for i in range(len(final_data)-1):
            # final[i].insert(0, i + 1)
            final_data[i].insert(8, pm[i])

        with open('PRO AQI/Real-Data/real_' + str(year) + '.csv', 'a') as csvfile:
            wr = csv.writer(csvfile, dialect='excel')
            for row in final_data:
                flag = 0
                for elem in row:
                    if elem == "" or elem == "-":
                        flag = 1
                if flag != 1:
                    wr.writerow(row)
                    
    data_2013 = data_combine(2013, 600)
    data_2014 = data_combine(2014, 600)
    data_2015 = data_combine(2015, 600)
    data_2016 = data_combine(2016, 600)
     
    total=data_2013+data_2014+data_2015+data_2016
    
    with open(r'PRO AQI/Real-Data/Real_Combine.csv', 'w') as csvfile:
        wr = csv.writer(csvfile, dialect='excel')
        wr.writerow(
            ['T', 'TM', 'Tm', 'SLP', 'H', 'VV', 'V', 'VM', 'PM 2.5'])
        wr.writerows(total)
        
        
df=pd.read_csv(r'PRO AQI/Real-Data/Real_Combine.csv')
df.head(20)

