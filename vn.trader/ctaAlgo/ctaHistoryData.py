# encoding: UTF-8

"""
本模块中主要包含：
1. 从通联数据下载历史行情的引擎
2. 用来把MultiCharts导出的历史数据载入到MongoDB中用的函数
3. 增加从通达信导出的历史数据载入到MongoDB中的函数
"""

from datetime import datetime, timedelta
import pymongo
from time import time
from multiprocessing.pool import ThreadPool

from ctaBase import *
from vtConstant import *
from vtFunction import loadMongoSetting
from datayesClient import DatayesClient


# 以下为vn.trader和通联数据规定的交易所代码映射 
VT_TO_DATAYES_EXCHANGE = {}
VT_TO_DATAYES_EXCHANGE[EXCHANGE_CFFEX] = 'CCFX'     # 中金所
VT_TO_DATAYES_EXCHANGE[EXCHANGE_SHFE] = 'XSGE'      # 上期所 
VT_TO_DATAYES_EXCHANGE[EXCHANGE_CZCE] = 'XZCE'       # 郑商所
VT_TO_DATAYES_EXCHANGE[EXCHANGE_DCE] = 'XDCE'       # 大商所
DATAYES_TO_VT_EXCHANGE = {v:k for k,v in VT_TO_DATAYES_EXCHANGE.items()}


########################################################################
class HistoryDataEngine(object):
	"""CTA模块用的历史数据引擎"""

	#----------------------------------------------------------------------
	def __init__(self):
		"""Constructor"""
		host, port, logging = loadMongoSetting()

		self.dbClient = pymongo.MongoClient(host, port)
		self.datayesClient = DatayesClient()

	#----------------------------------------------------------------------
	def lastTradeDate(self):
		"""获取最近交易日（只考虑工作日，无法检查国内假期）"""
		today = datetime.now()
		oneday = timedelta(1)

		if today.weekday() == 5:
			today = today - oneday
		elif today.weekday() == 6:
			today = today - oneday*2        

		return today.strftime('%Y%m%d')

	#----------------------------------------------------------------------
	def readFuturesProductSymbol(self):
		"""查询所有期货产品代码"""
		cx = self.dbClient[SETTING_DB_NAME]['FuturesSymbol'].find()
		return set([d['productSymbol'] for d in cx])    # 这里返回的是集合（因为会重复）

	#----------------------------------------------------------------------
	def readFuturesSymbol(self):
		"""查询所有期货合约代码"""
		cx = self.dbClient[SETTING_DB_NAME]['FuturesSymbol'].find()
		return [d['symbol'] for d in cx]    # 这里返回的是列表

	#----------------------------------------------------------------------
	def downloadFuturesSymbol(self, tradeDate=''):
		"""下载所有期货的代码"""
		if not tradeDate:
			tradeDate = self.lastTradeDate()

		self.dbClient[SETTING_DB_NAME]['FuturesSymbol'].ensure_index([('symbol', pymongo.ASCENDING)], 
				                                                     unique=True)


		path = 'api/market/getMktMFutd.json'

		params = {}
		params['tradeDate'] = tradeDate

		data = self.datayesClient.downloadData(path, params)

		if data:
			for d in data:
				symbolDict = {}
				symbolDict['symbol'] = d['ticker']
				symbolDict['productSymbol'] = d['contractObject']
				flt = {'symbol': d['ticker']}

				self.dbClient[SETTING_DB_NAME]['FuturesSymbol'].update_one(flt, {'$set':symbolDict}, 
								                                           upsert=True)
			print u'期货合约代码下载完成'
		else:
			print u'期货合约代码下载失败'

	#----------------------------------------------------------------------
	def downloadFuturesDailyBar(self, symbol):
		"""
		下载期货合约的日行情，symbol是合约代码，
		若最后四位为0000（如IF0000），代表下载连续合约。
		"""
		print u'开始下载%s日行情' %symbol

		# 查询数据库中已有数据的最后日期
		cl = self.dbClient[DAILY_DB_NAME][symbol]
		cx = cl.find(sort=[('datetime', pymongo.DESCENDING)])
		if cx.count():
			last = cx[0]
		else:
			last = ''

		# 主力合约
		if '0000' in symbol:
			path = 'api/market/getMktMFutd.json'

			params = {}
			params['contractObject'] = symbol.replace('0000', '')
			params['mainCon'] = 1
			if last:
				params['startDate'] = last['date']
		# 交易合约
		else:
			path = 'api/market/getMktFutd.json'

			params = {}
			params['ticker'] = symbol
			if last:
				params['startDate'] = last['date']

		# 开始下载数据
		data = self.datayesClient.downloadData(path, params)

		if data:
			# 创建datetime索引
			self.dbClient[DAILY_DB_NAME][symbol].ensure_index([('datetime', pymongo.ASCENDING)], 
						                                      unique=True)                

			for d in data:
				bar = CtaBarData()
				bar.vtSymbol = symbol
				bar.symbol = symbol
				try:
					bar.exchange = DATAYES_TO_VT_EXCHANGE.get(d.get('exchangeCD', ''), '')
					bar.open = d.get('openPrice', 0)
					bar.high = d.get('highestPrice', 0)
					bar.low = d.get('lowestPrice', 0)
					bar.close = d.get('closePrice', 0)
					bar.date = d.get('tradeDate', '').replace('-', '')
					bar.time = ''
					bar.datetime = datetime.strptime(bar.date, '%Y%m%d')
					bar.volume = d.get('turnoverVol', 0)
					bar.openInterest = d.get('openInt', 0)
				except KeyError:
					print d

				flt = {'datetime': bar.datetime}
				self.dbClient[DAILY_DB_NAME][symbol].update_one(flt, {'$set':bar.__dict__}, upsert=True)            

				print u'%s下载完成' %symbol
		else:
			print u'找不到合约%s' %symbol

	#----------------------------------------------------------------------
	def downloadAllFuturesDailyBar(self):
		"""下载所有期货的主力合约日行情"""
		start = time()
		print u'开始下载所有期货的主力合约日行情'

		productSymbolSet = self.readFuturesProductSymbol()

		print u'代码列表读取成功，产品代码：%s' %productSymbolSet

		# 这里也测试了线程池，但可能由于下载函数中涉及较多的数据格
		# 式转换，CPU开销较大，多线程效率并无显著改变。
		#p = ThreadPool(10)
		#p.map(self.downloadFuturesDailyBar, productSymbolSet)
		#p.close()
		#p.join()

		for productSymbol in productSymbolSet:
			self.downloadFuturesDailyBar(productSymbol+'0000')

		print u'所有期货的主力合约日行情已经全部下载完成, 耗时%s秒' %(time()-start)

	#----------------------------------------------------------------------
	def downloadFuturesIntradayBar(self, symbol):
		"""下载期货的日内分钟行情"""
		print u'开始下载%s日内分钟行情' %symbol

		# 日内分钟行情只有具体合约
		path = 'api/market/getFutureBarRTIntraDay.json'

		params = {}
		params['instrumentID'] = symbol
		params['unit'] = 1

		data = self.datayesClient.downloadData(path, params)

		if data:
			today = datetime.now().strftime('%Y%m%d')

			# 创建datetime索引
			self.dbClient[MINUTE_DB_NAME][symbol].ensure_index([('datetime', pymongo.ASCENDING)], 
						                                       unique=True)                
			_exchange = DATAYES_TO_VT_EXCHANGE.get(data[0].get('exchangeCD', ''), '')
			for d in data[0].get('barBodys',[]):
				bar = CtaBarData()
				bar.vtSymbol = symbol
				bar.symbol = symbol
				try:
					bar.exchange = _exchange
					bar.open = d.get('openPrice', 0)
					bar.high = d.get('highPrice', 0)
					bar.low = d.get('lowPrice', 0)
					bar.close = d.get('closePrice', 0)
					bar.date = today
					bar.time = d.get('barTime', '')
					bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M')
					bar.volume = d.get('totalVolume', 0)
					bar.openInterest = 0
				except KeyError:
					print d

				flt = {'datetime': bar.datetime}
				print ' '.join(['%s:%s' % item for item in bar.__dict__.items()])
				self.dbClient[MINUTE_DB_NAME][symbol].update_one(flt, {'$set':bar.__dict__}, upsert=True)            

			print u'%s下载完成' %symbol
		else:
			print u'找不到合约%s' %symbol   

	#----------------------------------------------------------------------
	def downloadEquitySymbol(self, tradeDate=''):
		"""下载所有股票的代码"""
		if not tradeDate:
			tradeDate = self.lastTradeDate()

		self.dbClient[SETTING_DB_NAME]['EquitySymbol'].ensure_index([('symbol', pymongo.ASCENDING)], 
				                                                    unique=True)


		path = 'api/market/getMktEqud.json'

		params = {}
		params['tradeDate'] = tradeDate

		data = self.datayesClient.downloadData(path, params)

		if data:
			for d in data:
				symbolDict = {}
				symbolDict['symbol'] = d['ticker']
				flt = {'symbol': d['ticker']}

				self.dbClient[SETTING_DB_NAME]['EquitySymbol'].update_one(flt, {'$set':symbolDict}, 
								                                          upsert=True)
			print u'股票代码下载完成'
		else:
			print u'股票代码下载失败'

	#----------------------------------------------------------------------
	def downloadEquityDailyBar(self, symbol):
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
		path = 'api/market/getMktEqud.json'

		params = {}
		params['ticker'] = symbol
		if last:
			params['beginDate'] = last['date']

		data = self.datayesClient.downloadData(path, params)

		if data:
			# 创建datetime索引
			self.dbClient[DAILY_DB_NAME][symbol].ensure_index([('datetime', pymongo.ASCENDING)], 
						                                      unique=True)                

			for d in data:
				bar = CtaBarData()
				bar.vtSymbol = symbol
				bar.symbol = symbol
				try:
					bar.exchange = DATAYES_TO_VT_EXCHANGE.get(d.get('exchangeCD', ''), '')
					bar.open = d.get('openPrice', 0)
					bar.high = d.get('highestPrice', 0)
					bar.low = d.get('lowestPrice', 0)
					bar.close = d.get('closePrice', 0)
					bar.date = d.get('tradeDate', '').replace('-', '')
					bar.time = ''
					bar.datetime = datetime.strptime(bar.date, '%Y%m%d')
					bar.volume = d.get('turnoverVol', 0)
				except KeyError:
					print d

				flt = {'datetime': bar.datetime}
				self.dbClient[DAILY_DB_NAME][symbol].update_one(flt, {'$set':bar.__dict__}, upsert=True)            

			print u'%s下载完成' %symbol
		else:
			print u'找不到合约%s' %symbol    


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
			bar = CtaBarData()
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
	host, port, logging= loadMongoSetting()

	client = pymongo.MongoClient(host, port)    
	collection = client[dbName][symbol]
	collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)   

	# 读取数据和插入到数据库
	reader = csv.DictReader(file(fileName, 'r'))
	for d in reader:
		bar = CtaBarData()
		bar.vtSymbol = symbol
		bar.symbol = symbol
		bar.open = float(d['Open'])
		bar.high = float(d['High'])
		bar.low = float(d['Low'])
		bar.close = float(d['Close'])
		bar.date = datetime.strptime(d['Date'], '%Y/%m/%d').strftime('%Y%m%d')
		bar.time = d['Time']
		bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
		bar.volume = d['TotalVolume']

		flt = {'datetime': bar.datetime}
		collection.update_one(flt, {'$set':bar.__dict__}, upsert=True)  
		print bar.date, bar.time

	print u'插入完毕，耗时：%s' % (time()-start)

#----------------------------------------------------------------------
def loadMinuteTxt(fileName, dbName, symbol):
	"""将Multicharts导出的csv格式的历史数据插入到Mongo数据库中"""
	import csv
	import pandas as pd

	start = time()
	print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)

	# 锁定集合，并创建索引
	host, port, logging = loadMongoSetting()

	client = pymongo.MongoClient(host, port)
	collection = client[dbName][symbol]
	collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)

	# 读取数据和插入到数据库
	lines = open(fileName,'r').readlines()
	lines = [line.replace('\t',',') for line in lines]
	newfile = open('swap.txt','w')
	newfile.writelines('Date,Time,Open,High,Low,Close,Vol,Val\r\n')
	newfile.writelines(lines)
	newfile.close()
	reader = csv.DictReader(file('swap.txt', 'r',))

	for d in reader:
		bar = CtaBarData()
		bar.vtSymbol = symbol
		bar.symbol = symbol
		bar.open = float(d['Open'])
		bar.high = float(d['High'])
		bar.low = float(d['Low'])
		bar.close = float(d['Close'])
		bar.date = datetime.strptime(d['Date'], '%Y/%m/%d').strftime('%Y%m%d')
		bar.time = d['Time']
		bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M')
		bar.volume = d['Vol']
		bar.openInterest = d['Val']

		flt = {'datetime': bar.datetime}
		collection.update_one(flt, {'$set':bar.__dict__}, upsert=True)
		print bar.date, bar.time

	print u'插入完毕，耗时：%s' % (time()-start)

def loadTBMinCsv(fileName, dbName, symbol):
	"""将TB导出的csv格式的历史分钟数据插入到Mongo数据库中"""
	import csv

	start = time()
	print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)

	# 锁定集合，并创建索引
	host, port, logging = loadMongoSetting()

	client = pymongo.MongoClient(host, port)    
	collection = client[dbName][symbol]
	collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)   

	# 读取数据和插入到数据库
	reader = csv.reader(file(fileName, 'r'))
	newfile = open(fileName+'.out','w')
	newfile.writelines('Date,Time,Open,High,Low,Close,Vol,Val\n')	
	for d in reader:
		bar = CtaBarData()
		bar.vtSymbol = symbol
		bar.symbol = symbol
		bar.open = float(d[2])
		bar.high = float(d[3])
		bar.low = float(d[4])
		bar.close = float(d[5])
		
		n1=int(float(d[1])*10000)
		h1=int(n1/100)
		m1=int(n1%100)			
		daytime_str="%s %02d:%02d:00" %(d[0],h1,m1)
		
		daytime=datetime.strptime(daytime_str, '%Y%m%d %H:%M:%S')
		daytime=daytime+timedelta(minutes=1)				
		bar.date = daytime.strftime('%Y%m%d')		
		bar.time = daytime.strftime('%H:%M:%S')		
		bar.datetime = daytime
		bar.volume = d[6]
		bar.openInterest = d[7]

		flt = {'datetime': bar.datetime}
		collection.update_one(flt, {'$set':bar.__dict__}, upsert=True)  		
		
		newfile.writelines(','.join([bar.date,bar.time,d[2],d[3],d[4],d[5],bar.volume,bar.openInterest])+'\n')
		
	newfile.close()		

	print u'插入完毕，耗时：%s' % (time()-start)
	
	
# TB导出的csv文件，K线是为开始时间，例如5分k先，9:30分，代表9:30-9:35的时间段。国内博易大师、同花顺的K先，9:35代表9:30-9:35，本例是把TB导成博易大师格式的k线
def loadTBMinCsvByFileNameT(fileName, dataDir, backupDir, to5m=True, to15m=True, to30m=True, toH1=True, toD1=True):
	"""将TB导出的csv格式的历史分钟数据插入到Mongo数据库中"""
	import csv
	import shutil
	import traceback 
	import pandas as pd
	import numpy as np

	start = time()
	symbol=fileName.split('_')[0]
	dbName=MINUTE_DB_NAME
	
	print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)

	# 锁定集合，并创建索引
	host, port, logging = loadMongoSetting()

	client = pymongo.MongoClient(host, port)    
	collection = client[dbName][symbol]
	collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)   

	# 读取数据和插入到数据库
	f=file(dataDir+'/'+fileName, 'r')
	reader = csv.reader(f)
	newfile = open(backupDir+'/'+fileName+'_1M.csv','w')
	newfile.writelines('DateTime,Open,High,Low,Close,Vol,Val\n')	
	for d in reader:
		bar = CtaBarData()
		bar.vtSymbol = symbol
		bar.symbol = symbol
		bar.open = float(d[2])
		bar.high = float(d[3])
		bar.low = float(d[4])
		bar.close = float(d[5])
		
		n1=int(float(d[1])*10000)
		h1=int(n1/100)
		m1=int(n1%100)			
		daytime_str="%s %02d:%02d:00" %(d[0],h1,m1)
		
		daytime=datetime.strptime(daytime_str, '%Y%m%d %H:%M:%S')
		daytime=daytime+timedelta(minutes=1)				#分钟加1
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

	print u'插入完毕，耗时：%s' % (time()-start)	
	

# TB导出的csv文件，K线是为开始时间，例如5分k先，9:30分，代表9:30-9:35的时间段。国内博易大师、同花顺的K先，9:35代表9:30-9:35，本例是保留TB本来的格式	
def loadTBMinCsvByFileName(fileName, dataDir, backupDir, to5m=True, to15m=True, to30m=True, toH1=True, toD1=True):
	"""将TB导出的csv格式的历史分钟数据插入到Mongo数据库中"""
	import csv
	import shutil
	import traceback 
	import pandas as pd
	import numpy as np

	start = time()
	symbol=fileName.split('_')[0]
	dbName=MINUTE_DB_NAME
	
	print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)

	# 锁定集合，并创建索引
	host, port, logging = loadMongoSetting()

	client = pymongo.MongoClient(host, port)    
	collection = client[dbName][symbol]
	collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)   

	# 读取数据和插入到数据库
	f=file(dataDir+'/'+fileName, 'r')
	reader = csv.reader(f)
	newfile = open(backupDir+'/'+fileName+'_1M.csv','w')
	newfile.writelines('DateTime,Open,High,Low,Close,Vol,Val\n')	
	for d in reader:
		bar = CtaBarData()
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
	
	#------------生成5M csv并插入数据库------------------
	ohlc_dict = {                                                                                                             
	'Open':'first',                                                                                                    
	'High':'max',                                                                                                       
	'Low':'min',                                                                                                        
	'Close': 'last',                                                                                                    
	'Vol': 'sum'
	}	
	df=pd.read_csv(backupDir+'/'+fileName+'_1M.csv', index_col=0, header=0,sep=',')
	df.index=pd.to_datetime(df.index)	
	if to5m:		
		df_out=df.resample('5T', how=ohlc_dict, closed='left', label='left')
		df_out=df_out.dropna(how='any')
		df_out.to_csv(backupDir+'/'+fileName+'_5M.csv')
		
	try:
		shutil.move(dataDir+'/'+fileName, backupDir)		
	except:
		traceback.print_exc()
		import os
		os.remove(dataDir+'/'+fileName)

	print u'插入完毕，耗时：%s' % (time()-start)	
	


def TBDataImport(fileName,dataDir,backupDir,to5m=True, to15m=True, to30m=True, toH1=True, toD1=True):
	import dataUtils
	import csv
	import shutil
	import traceback 
	
	host, port, logging = loadMongoSetting()	
	client = pymongo.MongoClient(host, port)
	
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
		bar = CtaBarData()
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
		
def autoLoadTBCsv2DB():
	import os
	path = os.path.abspath(os.path.dirname(__file__))
	tbDataDir = os.path.join(path, 'historicalData', 'TB') 	
	tbDataBackupDir = os.path.join(path, 'historicalData', 'TBbackup')
	
	for f in os.listdir(tbDataDir):
		if(os.path.isfile(tbDataDir + '/' + f)):
			TBDataImport(f,tbDataDir,tbDataBackupDir)
			
	

def loadDayTxt(fileName, dbName, symbol):
	"""将Multicharts导出的csv格式的历史数据插入到Mongo数据库中"""
	import csv
	import pandas as pd

	start = time()
	print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)

	# 锁定集合，并创建索引
	host, port, logging = loadMongoSetting()

	client = pymongo.MongoClient(host, port)
	collection = client[dbName][symbol]
	collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)

	# 读取数据和插入到数据库
	lines = open(fileName,'r').readlines()
	lines = [line.replace('\t',',') for line in lines]
	newfile = open('swap.txt','w')
	newfile.writelines('Date,Open,High,Low,Close,Vol,Val\r\n')
	newfile.writelines(lines)
	newfile.close()
	reader = csv.DictReader(file('swap.txt', 'r',))

	for d in reader:
		bar = CtaBarData()
		bar.vtSymbol = symbol
		bar.symbol = symbol
		bar.open = float(d['Open'])
		bar.high = float(d['High'])
		bar.low = float(d['Low'])
		bar.close = float(d['Close'])
		bar.date = datetime.strptime(d['Date'], '%Y/%m/%d').strftime('%Y%m%d')
		bar.time = "15:00"
		bar.datetime = datetime.strptime(bar.date, '%Y%m%d')
		bar.volume = d['Vol']
		bar.openInterest = d['Val']

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
	host, port, logging = loadMongoSetting()

	client = pymongo.MongoClient(host, port)    
	collection = client[dbName][symbol]
	collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)   

	# 读取数据和插入到数据库
	reader = csv.reader(file(fileName, 'r'))
	for d in reader:
		bar = CtaBarData()
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


if __name__ == '__main__':
	## 简单的测试脚本可以写在这里
	from time import sleep
	'''
	e = HistoryDataEngine()
	sleep(1)
	e.downloadEquityDailyBar('000001')
	e.downloadEquitySymbol()
	e.downloadFuturesSymbol()
	e.downloadAllFuturesDailyBar()
	e.downloadFuturesIntradayBar('m1705')
	'''
	
	#loadTBMinCsv('D:/work/vnpy/vn.trader/ctaAlgo/historicalData/rb1705_1Min.csv', MINUTE_DB_NAME, 'rb1705')
	autoLoadTBCsv2DB()
	
	# 这里将项目中包含的股指日内分钟线csv导入MongoDB，作者电脑耗时大约3分钟
	#loadMcCsv('IF0000_1min.csv', MINUTE_DB_NAME, 'IF0000')
		#e.downloadFuturesIntradayBar('au1606')

	# 这里将项目中包含的股指日内分钟线csv导入MongoDB，作者电脑耗时大约3分钟
	#loadMcCsv('IF0000_1min.csv', MINUTE_DB_NAME, 'IF0000')
	# loadMinuteTxt('TXTMIN1/SQag06.TXT', MINUTE_DB_NAME, 'ag1706')
	# loadMinuteTxt('TXTMIN5/SQag06.TXT', MINUTE5_DB_NAME, 'ag1706')
	# loadDayTxt('TXTDAY/SQag06.TXT',DAILY_DB_NAME,'ag1706')
	# loadMinuteTxt('TXTMIN1/SQauS06.TXT', MINUTE_DB_NAME, 'au1706')
	# loadMinuteTxt('TXTMIN5/SQauS06.TXT', MINUTE5_DB_NAME, 'au1706')
	# loadDayTxt('TXTDAY/SQauS06.TXT', DAILY_DB_NAME, 'au1706')
	# loadMinuteTxt('TXTMIN1/SQrb05.TXT', MINUTE_DB_NAME, 'rb1705')
	# loadMinuteTxt('TXTMIN5/SQrb05.TXT', MINUTE5_DB_NAME, 'rb1705')
	# loadDayTxt('TXTDAY/SQrb05.TXT', DAILY_DB_NAME, 'rb1705')
