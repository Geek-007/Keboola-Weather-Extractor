# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 13:17:47 2017

@author: Milan
"""
from keboola import docker
import requests
import pandas as pd
import numpy as np
import time 


COOR_DICT = {"Prague":{"lat":'50.0755381',"lon":'14.43780049999998'},
             "Brno":{"lat":'49.1951',"lon":'16.6068'},
             "Ostrava": {"lat":'49.8209',"lon":'18.2625'},
             "Bratislava": {"lat":'48.1486',"lon":'17.1077'}
             }
#timestamped = pd.to_datetime('2017-04-07 12:00:00',utc=True)
def generateUrl(city='Prague',desired_time='current'):
    global SECRET_KEY
    TOKEN = SECRET_KEY
    URL_PREFIX = 'https://api.darksky.net/forecast/'
    
    if desired_time == 'current':
        URL_PARAMS = {
            "lat":COOR_DICT[city]['lat'],
            "lon":COOR_DICT[city]['lon'],
            }
        URL_SUFFIX = '?exclude=flags&lang=cs&units=auto'
        return URL_PREFIX+TOKEN+'/'+URL_PARAMS['lat']+','+URL_PARAMS['lon']+URL_SUFFIX
    else:
        URL_PARAMS = {
                "lat":COOR_DICT[city]['lat'],
                "lon":COOR_DICT[city]['lon'],
                "time":str(desired_time)
                }
        URL_SUFFIX = '?exclude=currently,flags&lang=cs&units=auto'
        return URL_PREFIX+TOKEN+'/'+URL_PARAMS['lat']+','+URL_PARAMS['lon']+','+URL_PARAMS['time']+URL_SUFFIX
      
# For all timestamps in the desiredDataRange

def getHistoricWeatherValues(city='Prague',start_time='2017-01-01 12:00:00',end_time='2017-01-01 12:00:00'):
    if start_time == 'current':
        desiredDataRangeUNIX = ['current']
    else:
        desiredDataRange = pd.date_range(start_time,end_time)
        desiredDataRangeUNIX = desiredDataRange.astype(np.int64) // 10**9
    
    frameList = []
    for record in desiredDataRangeUNIX:
        desiredURL = generateUrl(city,desired_time=record)
        r = requests.get(desiredURL)
        rJson = r.json()
        frameList.append(pd.DataFrame.from_dict(rJson['hourly']['data']))
        time.sleep(0.1)
        
    allFrames = pd.concat(frameList)
    allFrames['datetimeUtc'] = pd.to_datetime(allFrames['time'],unit='s')
    allFrames['city'] = city
    allFrames['lat'] = COOR_DICT[city]['lat']
    allFrames['lon'] = COOR_DICT[city]['lon']
    return allFrames

def concatWeatherFrames(list_of_frames):
    concatFrame = pd.concat(list_of_frames)
    concatFrame.drop_duplicates(subset=['time','city'],inplace=True)
    return concatFrame

# main function
if __name__ == '__main__': 
    """ IMPORT """
    try:
        weatherFrame = pd.read_csv('in/tables/weatherTable.csv')
    except FileNotFoundError:
        weatherFrame = None
    cfg = docker.Config()
    parameters = cfg.get_parameters()
    LIST_OF_CITIES = parameters.get('listOfCities')
    FROM_TIME = parameters.get('fromTime')
    TO_TIME = parameters.get('toTime')
    SECRET_KEY = parameters.get('secretKey')
    #STOPPING = parameters.get('stopAt1000Requests') 
    
    """ PROCESS """
    #requestCounter = 0
    outputWeatherFrameList = [weatherFrame]
    for city in LIST_OF_CITIES:
        outputWeatherFrameList.append(getHistoricWeatherValues(city=city,start_time=FROM_TIME,end_time=TO_TIME))
    outputWeatherFrame = concatWeatherFrames(outputWeatherFrameList) 
    
    """ EXPORT """
    outputWeatherFrame.to_csv('out/tables/outputWeatherFrame.csv',index=None,encoding='utf-8')


