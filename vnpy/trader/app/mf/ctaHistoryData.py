# encoding: UTF-8

"""
本模块中主要包含：
1. 从通联数据下载历史行情的引擎
2. 用来把MultiCharts导出的历史数据载入到MongoDB中用的函数
3. 增加从通达信导出的历史数据载入到MongoDB中的函数
"""

from datetime import datetime, timedelta
from time import time

import pymongo

from vnpy.trader.vtGlobal import globalSetting
from vnpy.trader.vtConstant import *
from vnpy.trader.vtObject import VtBarData
from .ctaBase import SETTING_DB_NAME, TICK_DB_NAME, MINUTE_DB_NAME, DAILY_DB_NAME


#----------------------------------------------------------------------
def downloadEquityDailyBarts(self, symbol):
    """
    下载股票的日行情，symbol是股票代码
    """
    print u'开始下载%s日行情' %symbol
    
    # 查询数据库中已有数据的最后日期
    cl = self.dbClient[DAILY_DB_NAME][symbol]
    cx = cl.find(sort=[('datetime', pymongo.DESCENDING)])
    if cx.count():
        last = cx[0]
    else:
        last = ''
    # 开始下载数据
    import tushare as ts
    
    if last:
        start = last['date'][:4]+'-'+last['date'][4:6]+'-'+last['date'][6:]
        
    data = ts.get_k_data(symbol,start)
    
    if not data.empty:
        # 创建datetime索引
        self.dbClient[DAILY_DB_NAME][symbol].ensure_index([('datetime', pymongo.ASCENDING)], 
                                                            unique=True)                
        
        for index, d in data.iterrows():
            bar = VtBarData()
            bar.vtSymbol = symbol
            bar.symbol = symbol
            try:
                bar.open = d.get('open')
                bar.high = d.get('high')
                bar.low = d.get('low')
                bar.close = d.get('close')
                bar.date = d.get('date').replace('-', '')
                bar.time = ''
                bar.datetime = datetime.strptime(bar.date, '%Y%m%d')
                bar.volume = d.get('volume')
            except KeyError:
                print d
            
            flt = {'datetime': bar.datetime}
            self.dbClient[DAILY_DB_NAME][symbol].update_one(flt, {'$set':bar.__dict__}, upsert=True)            
        
        print u'%s下载完成' %symbol
    else:
        print u'找不到合约%s' %symbol

#----------------------------------------------------------------------
def loadMcCsv(fileName, dbName, symbol):
    """将Multicharts导出的csv格式的历史数据插入到Mongo数据库中"""
    import csv
    
    start = time()
    print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)
    
    # 锁定集合，并创建索引
    client = pymongo.MongoClient(globalSetting['mongoHost'], globalSetting['mongoPort']) 
    collection = client[dbName][symbol]
    collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)   
    
    # 读取数据和插入到数据库
    reader = csv.DictReader(file(fileName, 'r'))
    for d in reader:
        bar = VtBarData()
        bar.vtSymbol = symbol
        bar.symbol = symbol
        bar.open = float(d['Open'])
        bar.high = float(d['High'])
        bar.low = float(d['Low'])
        bar.close = float(d['Close'])
        bar.date = datetime.strptime(d['Date'], '%Y-%m-%d').strftime('%Y%m%d')
        bar.time = d['Time']
        bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
        bar.volume = d['TotalVolume']

        flt = {'datetime': bar.datetime}
        collection.update_one(flt, {'$set':bar.__dict__}, upsert=True)  
        print bar.date, bar.time
    
    print u'插入完毕，耗时：%s' % (time()-start)
#----------------------------------------------------------------------
def TBPLUSDataImport(fileName,dataDir,backupDir,to5m=True, to15m=True, to30m=True, toH1=True, toD1=True):
    import dataUtils
    import csv
    import shutil
    import traceback 

    client = pymongo.MongoClient(globalSetting['mongoHost'], globalSetting['mongoPort'])	

    start = time()
    symbol=fileName.split('_')[0]
    dbName=MINUTE_DB_NAME

    print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)
    collection = client[dbName][symbol]
    collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)   

    # 读取数据和插入到数据库
    f=file(dataDir+'/'+fileName, 'r')
    reader = csv.reader(f)
    newfile = open(backupDir+'/'+fileName+'_1M.csv','w')
    newfile.writelines('DateTime,Open,High,Low,Close,Vol,Val\n')	
    for d in reader:
        bar = VtBarData()
        bar.vtSymbol = symbol
        bar.symbol = symbol
        bar.open = float(d[2])
        bar.high = float(d[3])
        bar.low = float(d[4])
        bar.close = float(d[5])
        bar.barsize = 1

        n1=int(float(d[1])*10000)
        h1=int(n1/100)
        m1=int(n1%100)			
        daytime_str="%s %02d:%02d:00" %(d[0],h1,m1)

        daytime=datetime.strptime(daytime_str, '%Y%m%d %H:%M:%S')
        #daytime=daytime+timedelta(minutes=1)				
        bar.date = daytime.strftime('%Y%m%d')		
        bar.time = daytime.strftime('%H:%M:%S')		
        bar.datetime = daytime
        bar.volume = d[6]
        bar.openInterest = d[7]

        flt = {'datetime': bar.datetime}
        collection.update_one(flt, {'$set':bar.__dict__}, upsert=True)  		

        newfile.writelines(','.join([bar.date+' '+bar.time,d[2],d[3],d[4],d[5],bar.volume,bar.openInterest])+'\n')

    f.close()
    newfile.close()		
    try:
        shutil.move(dataDir+'/'+fileName, backupDir)		
    except:
        traceback.print_exc()
        import os
        os.remove(dataDir+'/'+fileName)	
    print u'1m 插入完毕，耗时：%s' % (time()-start)

    if to5m:
        start = time()
        span=5
        dbName=MINUTE5_DB_NAME
        print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)

        # 锁定集合，并创建索引		    
        collection = client[dbName][symbol]
        collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True) 
        iFile = backupDir+'/'+fileName+'_1M.csv'
        oFile = backupDir+'/'+fileName+'_5M.csv'
        dataUtils.resample(span, iFile, oFile, collection, symbol)
        print u'5m 插入完毕，耗时：%s' % (time()-start)

    if to15m:
        start = time()
        span=15
        dbName=MINUTE15_DB_NAME
        print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)

        # 锁定集合，并创建索引		    
        collection = client[dbName][symbol]
        collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True) 
        iFile = backupDir+'/'+fileName+'_1M.csv'
        oFile = backupDir+'/'+fileName+'_15M.csv'
        dataUtils.resample(span, iFile, oFile, collection, symbol)
        print u'15m 插入完毕，耗时：%s' % (time()-start)	

    if to30m:
        start = time()
        span=30
        dbName=MINUTE30_DB_NAME
        print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)

        # 锁定集合，并创建索引		    
        collection = client[dbName][symbol]
        collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True) 
        iFile = backupDir+'/'+fileName+'_1M.csv'
        oFile = backupDir+'/'+fileName+'_30M.csv'
        dataUtils.resample(span, iFile, oFile, collection, symbol)
        print u'30m 插入完毕，耗时：%s' % (time()-start)	

    if toH1:
        start = time()
        span=60
        dbName=H1_DB_NAME
        print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)

        # 锁定集合，并创建索引		    
        collection = client[dbName][symbol]
        collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True) 
        iFile = backupDir+'/'+fileName+'_1M.csv'
        oFile = backupDir+'/'+fileName+'_H1.csv'
        dataUtils.resample(span, iFile, oFile, collection, symbol)
        print u'H1 插入完毕，耗时：%s' % (time()-start)

    if toD1:
        start = time()
        span=60
        dbName=H1_DB_NAME
        print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)

        # 锁定集合，并创建索引		    
        collection = client[dbName][symbol]
        collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True) 
        iFile = backupDir+'/'+fileName+'_1M.csv'
        oFile = backupDir+'/'+fileName+'_H1.csv'
        dataUtils.resample('D1', iFile, oFile, collection, symbol)
        print u'H1 插入完毕，耗时：%s' % (time()-start)	
        
#----------------------------------------------------------------------
def autoLoadTBCsv2DB():
    import os
    path = os.path.abspath(os.path.dirname(__file__))
    tbDataDir = os.path.join(path, 'historicalData', 'TB') 	
    tbDataBackupDir = os.path.join(path, 'historicalData', 'TBbackup')

    for f in os.listdir(tbDataDir):
        if(os.path.isfile(tbDataDir + '/' + f)):
            TBPLUSDataImport(f,tbDataDir,tbDataBackupDir)
            
#----------------------------------------------------------------------
def loadTbCsv(fileName, dbName, symbol):
    """将TradeBlazer导出的csv格式的历史分钟数据插入到Mongo数据库中"""
    import csv
    
    start = time()
    print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)
    
    # 锁定集合，并创建索引
    client = pymongo.MongoClient(globalSetting['mongoHost'], globalSetting['mongoPort'])
    collection = client[dbName][symbol]
    collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)   
    
    # 读取数据和插入到数据库
    reader = csv.reader(file(fileName, 'r'))
    for d in reader:
        bar = VtBarData()
        bar.vtSymbol = symbol
        bar.symbol = symbol
        bar.open = float(d[1])
        bar.high = float(d[2])
        bar.low = float(d[3])
        bar.close = float(d[4])
        bar.date = datetime.strptime(d[0].split(' ')[0], '%Y/%m/%d').strftime('%Y%m%d')
        bar.time = d[0].split(' ')[1]+":00"
        bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
        bar.volume = d[5]
        bar.openInterest = d[6]

        flt = {'datetime': bar.datetime}
        collection.update_one(flt, {'$set':bar.__dict__}, upsert=True)  
        print bar.date, bar.time
    
    print u'插入完毕，耗时：%s' % (time()-start)
    
 #----------------------------------------------------------------------
def loadTbPlusCsv(fileName, dbName, symbol):
    """将TB极速版导出的csv格式的历史分钟数据插入到Mongo数据库中"""
    import csv    

    start = time()
    print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol) 

    # 锁定集合，并创建索引
    client = pymongo.MongoClient(globalSetting['mongoHost'], globalSetting['mongoPort'])
    collection = client[dbName][symbol]
    collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)      

    # 读取数据和插入到数据库
    reader = csv.reader(file(fileName, 'r'))
    for d in reader:
        bar = VtBarData()
        bar.vtSymbol = symbol
        bar.symbol = symbol
        bar.open = float(d[2])
        bar.high = float(d[3])
        bar.low = float(d[4])
        bar.close = float(d[5])
        bar.date = str(d[0])
        
        tempstr=str(round(float(d[1])*10000)).split(".")[0].zfill(4)
        bar.time = tempstr[:2]+":"+tempstr[2:4]+":00"
        
        bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
        bar.volume = d[6]
        bar.openInterest = d[7]
        flt = {'datetime': bar.datetime}
        collection.update_one(flt, {'$set':bar.__dict__}, upsert=True)  
        print bar.date, bar.time    

    print u'插入完毕，耗时：%s' % (time()-start)

#----------------------------------------------------------------------
def loadTdxCsv(fileName, dbName, symbol):
    """将通达信导出的csv格式的历史分钟数据插入到Mongo数据库中"""
    import csv
    
    start = time()
    print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)
    
    # 锁定集合，并创建索引
    client = pymongo.MongoClient(globalSetting['mongoHost'], globalSetting['mongoPort'])
    collection = client[dbName][symbol]
    collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)   
    
    # 读取数据和插入到数据库
    reader = csv.reader(file(fileName, 'r'))
    for d in reader:
        bar = VtBarData()
        bar.vtSymbol = symbol
        bar.symbol = symbol
        bar.open = float(d[2])
        bar.high = float(d[3])
        bar.low = float(d[4])
        bar.close = float(d[5])
        bar.date = datetime.strptime(d[0], '%Y/%m/%d').strftime('%Y%m%d')
        bar.time = d[1][:2]+':'+d[1][2:4]+':00'
        bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
        bar.volume = d[6]
        bar.openInterest = d[7]

        flt = {'datetime': bar.datetime}
        collection.update_one(flt, {'$set':bar.__dict__}, upsert=True)  
        print bar.date, bar.time
    
    print u'插入完毕，耗时：%s' % (time()-start)

    
