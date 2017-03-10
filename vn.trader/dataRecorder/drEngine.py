# encoding: UTF-8

'''
本文件中实现了行情数据记录引擎，用于汇总TICK数据，并生成K线插入数据库。

使用DR_setting.json来配置需要收集的合约，以及主力合约代码。
'''

import copy
import json
import os
from datetime import datetime

from drBase import *
from eventEngine import *
from vtFunction import todayDate
from vtGateway import VtSubscribeReq, VtLogData

import time
########################################################################
class DrEngine(object):
	"""数据记录引擎"""

	settingFileName = 'DR_setting.json'
	path = os.path.abspath(os.path.dirname(__file__))
	settingFileName = os.path.join(path, settingFileName)    

	#----------------------------------------------------------------------
	def __init__(self, mainEngine, eventEngine):
		"""Constructor"""
		self.mainEngine = mainEngine
		self.eventEngine = eventEngine

		# 当前日期
		self.today = todayDate()

		# 主力合约代码映射字典，key为具体的合约代码（如IF1604），value为主力合约代码（如IF0000）
		self.activeSymbolDict = {}

		# Tick对象字典
		self.tickDict = {}

		# K线对象字典
		self.barDict = {}
		self.m5barDict = {}
		self.daybarDict = {}

		#负责执行数据库插入的单独线程相关
		self.active = False                     #工作状态
		self.queue = Queue()                    #队列
		self.thread = Thread(target=self.run)   #线程

		# 交易时间字典
		self.timeDict = {}

		# 每分钟的的第一个tick的成交量
		self.firstvolumes ={}

		# 载入设置，订阅行情
		self.loadSetting()

    def saveSetting(self, setting):
	    """保存设置"""
	    setting['working'] = self.working
	    with open(self.settingFileName, 'w') as f:
		    try:
			    str = json.dumps(setting, indent=2)
			    f.write(str)
		    except:
			    pass
	    return True
        
	#----------------------------------------------------------------------
	def loadSetting(self):
		"""载入设置"""
		with open(self.settingFileName) as f:
			drSetting = json.load(f)

			# 如果working设为False则不启动行情记录功能
            self.working = drSetting['working']
            if not self.working:
				return

			if 'tick' in drSetting:
				l = drSetting['tick']

				for setting in l:
					symbol = setting[0]
					vtSymbol = symbol

					req = VtSubscribeReq()
					req.symbol = setting[0]

					# 针对LTS和IB接口，订阅行情需要交易所代码
					if len(setting)>=3:
						req.exchange = setting[2]
						vtSymbol = '.'.join([symbol, req.exchange])

					# 针对IB接口，订阅行情需要货币和产品类型
					if len(setting)>=5:
						req.currency = setting[3]
						req.productClass = setting[4]

					self.mainEngine.subscribe(req, setting[1])

					drTick = DrTickData()           # 该tick实例可以用于缓存部分数据（目前未使用）
					self.tickDict[vtSymbol] = drTick

			if 'bar' in drSetting:
				l = drSetting['bar']

				for setting in l:
					symbol = setting[0]
					vtSymbol = symbol

					req = VtSubscribeReq()
					req.symbol = symbol                    

					if len(setting)>=3:
						req.exchange = setting[2]
						vtSymbol = '.'.join([symbol, req.exchange])

					if len(setting)>=5:
						req.currency = setting[3]
						req.productClass = setting[4]                    

					self.mainEngine.subscribe(req, setting[1])  

					bar = DrBarData() 
					self.barDict[vtSymbol] = bar
					m5bar = DrBarData()
					self.m5barDict[symbol] = m5bar
					daybar = DrBarData()
					self.daybarDict[symbol] = daybar

			if 'active' in drSetting:
				d = drSetting['active']

				# 注意这里的vtSymbol对于IB和LTS接口，应该后缀.交易所
				for activeSymbol, vtSymbol in d.items():
					self.activeSymbolDict[vtSymbol] = activeSymbol

			# 启动数据插入线程
			self.start()

			# 注册事件监听
			self.registerEvent()    

	#----------------------------------------------------------------------
	def procecssTickEvent(self, event):
		"""处理行情推送"""
		tick = event.dict_['data']
		vtSymbol = tick.vtSymbol

		# 转化Tick格式
		drTick = DrTickData()
		d = drTick.__dict__
		for key in d.keys():
			if key != 'datetime':
				d[key] = tick.__getattribute__(key)
		drTick.datetime = datetime.strptime(' '.join([tick.date, tick.time]), '%Y%m%d %H:%M:%S.%f')            

		#按照不同合约分类时间list
		ninetoeleven=["bu","rb","hc","ru"]#9点到11点的合约
		ninetohalfeleven=["p","j","m","y","a","b","jm","i","SR","CF","RM","MA","ZC","FG","OI"]#9点到11点半的合约
		ninetoone = ["cu","al","zn","pb","sn","ni"]  # 9点到1点的合约列表
		ninetohalftwo=["ag","au"]#9点到2点半的合约
		#过滤中没有加入国债合约！
		whether_in_list=False
		for instrument in ninetoeleven:
			if instrument in vtSymbol:
				time_f=datetime.now()
				if (time_f >= datetime.today().replace(hour=9,minute=0,second=0) and time_f <= datetime.today().replace(hour=15,minute=0,second=0)) or (time_f>=datetime.today().replace(hour=21,minute=0,second=0) and time_f<=datetime.today().replace(hour=23,minute=0,second=0)):
					# 更新Tick数据
					if vtSymbol in self.tickDict:
						self.insertData(TICK_DB_NAME, vtSymbol, drTick)

						if vtSymbol in self.activeSymbolDict:
							activeSymbol = self.activeSymbolDict[vtSymbol]
							self.insertData(TICK_DB_NAME, activeSymbol, drTick)

						# 发出日志
						self.writeDrLog(u'记录Tick数据%s，时间:%s, last:%s, bid:%s, ask:%s'
						                %(drTick.vtSymbol, drTick.time, drTick.lastPrice, drTick.bidPrice1, drTick.askPrice1))

					# 更新分钟线数据
					if vtSymbol in self.barDict:
						bar = self.barDict[vtSymbol]

						# 如果第一个TICK或者新的一分钟
						if not bar.datetime or bar.datetime.minute != drTick.datetime.minute:
							if bar.vtSymbol:
								newBar = copy.copy(bar)
								self.insertData(MINUTE_DB_NAME, vtSymbol, newBar)

								if vtSymbol in self.activeSymbolDict:
									activeSymbol = self.activeSymbolDict[vtSymbol]
									self.insertData(MINUTE_DB_NAME, activeSymbol, newBar)

								self.writeDrLog(u'记录分钟线数据%s，时间:%s, O:%s, H:%s, L:%s, C:%s'
								                %(bar.vtSymbol, bar.time, bar.open, bar.high,
								                  bar.low, bar.close))

							bar.vtSymbol = drTick.vtSymbol
							bar.symbol = drTick.symbol
							bar.exchange = drTick.exchange

							bar.open = drTick.lastPrice
							bar.high = drTick.lastPrice
							bar.low = drTick.lastPrice
							bar.close = drTick.lastPrice

							bar.date = drTick.date
							bar.time = drTick.time
							bar.datetime = drTick.datetime
							bar.volume = drTick.volume
							bar.openInterest = drTick.openInterest
						# 否则继续累加新的K线
						else:
							bar.high = max(bar.high, drTick.lastPrice)
							bar.low = min(bar.low, drTick.lastPrice)
							bar.close = drTick.lastPrice
		for instrument in ninetohalfeleven:
			if instrument in vtSymbol:
				time_f = datetime.now()
				if (time_f >= datetime.today().replace(hour=9, minute=0, second=0) and time_f <= datetime.today().replace(hour=15, minute=0, second=0)) or (time_f >= datetime.today().replace(hour=21, minute=0, second=0) and time_f <= datetime.today().replace(hour=23, minute=30, second=0)):
					# 更新Tick数据
					if vtSymbol in self.tickDict:
						self.insertData(TICK_DB_NAME, vtSymbol, drTick)

						if vtSymbol in self.activeSymbolDict:
							activeSymbol = self.activeSymbolDict[vtSymbol]
							self.insertData(TICK_DB_NAME, activeSymbol, drTick)

						# 发出日志
						self.writeDrLog(u'记录Tick数据%s，时间:%s, last:%s, bid:%s, ask:%s'
						                % (
						                        drTick.vtSymbol, drTick.time, drTick.lastPrice, drTick.bidPrice1, drTick.askPrice1))

					# 更新分钟线数据
					if vtSymbol in self.barDict:
						bar = self.barDict[vtSymbol]

						# 如果第一个TICK或者新的一分钟
						if not bar.datetime or bar.datetime.minute != drTick.datetime.minute:
							if bar.vtSymbol:
								newBar = copy.copy(bar)
								self.insertData(MINUTE_DB_NAME, vtSymbol, newBar)

								if vtSymbol in self.activeSymbolDict:
									activeSymbol = self.activeSymbolDict[vtSymbol]
									self.insertData(MINUTE_DB_NAME, activeSymbol, newBar)

								self.writeDrLog(u'记录分钟线数据%s，时间:%s, O:%s, H:%s, L:%s, C:%s'
								                % (bar.vtSymbol, bar.time, bar.open, bar.high,
								                   bar.low, bar.close))

							bar.vtSymbol = drTick.vtSymbol
							bar.symbol = drTick.symbol
							bar.exchange = drTick.exchange

							bar.open = drTick.lastPrice
							bar.high = drTick.lastPrice
							bar.low = drTick.lastPrice
							bar.close = drTick.lastPrice

							bar.date = drTick.date
							bar.time = drTick.time
							bar.datetime = drTick.datetime
							bar.volume = drTick.volume
							bar.openInterest = drTick.openInterest
							# 否则继续累加新的K线
						else:
							bar.high = max(bar.high, drTick.lastPrice)
							bar.low = min(bar.low, drTick.lastPrice)
							bar.close = drTick.lastPrice
		for instrument in ninetoone:
			if instrument in vtSymbol:
				time_f = datetime.now()
				if (time_f >= datetime.today().replace(hour=9, minute=0, second=0) and time_f <= datetime.today().replace(hour=15, minute=0, second=0)) or (time_f >= datetime.today().replace(hour=21, minute=0, second=0) and time_f <= datetime.today().replace(hour=23, minute=59, second=59)) or (time_f<=datetime.today().replace(hour=1, minute=0, second=0)):
					# 更新Tick数据
					if vtSymbol in self.tickDict:
						self.insertData(TICK_DB_NAME, vtSymbol, drTick)

						if vtSymbol in self.activeSymbolDict:
							activeSymbol = self.activeSymbolDict[vtSymbol]
							self.insertData(TICK_DB_NAME, activeSymbol, drTick)

						# 发出日志
						self.writeDrLog(u'记录Tick数据%s，时间:%s, last:%s, bid:%s, ask:%s'
						                % (
						                        drTick.vtSymbol, drTick.time, drTick.lastPrice, drTick.bidPrice1, drTick.askPrice1))

					# 更新分钟线数据
					if vtSymbol in self.barDict:
						bar = self.barDict[vtSymbol]

						# 如果第一个TICK或者新的一分钟
						if not bar.datetime or bar.datetime.minute != drTick.datetime.minute:
							if bar.vtSymbol:
								newBar = copy.copy(bar)
								self.insertData(MINUTE_DB_NAME, vtSymbol, newBar)

								if vtSymbol in self.activeSymbolDict:
									activeSymbol = self.activeSymbolDict[vtSymbol]
									self.insertData(MINUTE_DB_NAME, activeSymbol, newBar)

								self.writeDrLog(u'记录分钟线数据%s，时间:%s, O:%s, H:%s, L:%s, C:%s'
								                % (bar.vtSymbol, bar.time, bar.open, bar.high,
								                   bar.low, bar.close))

							bar.vtSymbol = drTick.vtSymbol
							bar.symbol = drTick.symbol
							bar.exchange = drTick.exchange

							bar.open = drTick.lastPrice
							bar.high = drTick.lastPrice
							bar.low = drTick.lastPrice
							bar.close = drTick.lastPrice

							bar.date = drTick.date
							bar.time = drTick.time
							bar.datetime = drTick.datetime
							bar.volume = drTick.volume
							bar.openInterest = drTick.openInterest
							# 否则继续累加新的K线
						else:
							bar.high = max(bar.high, drTick.lastPrice)
							bar.low = min(bar.low, drTick.lastPrice)
							bar.close = drTick.lastPrice
		for instrument in ninetohalftwo:
			if instrument in vtSymbol:
				time_f = datetime.now()
				if (time_f >= datetime.today().replace(hour=9, minute=0, second=0) and time_f <= datetime.today().replace(hour=15, minute=0, second=0)) or (time_f >= datetime.today().replace(hour=21, minute=0, second=0) and time_f <= datetime.today().replace(hour=24, minute=0, second=0)) or (time_f<=datetime.today().replace(hour=2, minute=30, second=0)):
					# 更新Tick数据
					if vtSymbol in self.tickDict:
						self.insertData(TICK_DB_NAME, vtSymbol, drTick)

						if vtSymbol in self.activeSymbolDict:
							activeSymbol = self.activeSymbolDict[vtSymbol]
							self.insertData(TICK_DB_NAME, activeSymbol, drTick)

						# 发出日志
						self.writeDrLog(u'记录Tick数据%s，时间:%s, last:%s, bid:%s, ask:%s'
						                % (
						                        drTick.vtSymbol, drTick.time, drTick.lastPrice, drTick.bidPrice1, drTick.askPrice1))

					# 更新分钟线数据
					if vtSymbol in self.barDict:
						bar = self.barDict[vtSymbol]

						# 如果第一个TICK或者新的一分钟
						if not bar.datetime or bar.datetime.minute != drTick.datetime.minute:
							if bar.vtSymbol:
								newBar = copy.copy(bar)
								self.insertData(MINUTE_DB_NAME, vtSymbol, newBar)

								if vtSymbol in self.activeSymbolDict:
									activeSymbol = self.activeSymbolDict[vtSymbol]
									self.insertData(MINUTE_DB_NAME, activeSymbol, newBar)

								self.writeDrLog(u'记录分钟线数据%s，时间:%s, O:%s, H:%s, L:%s, C:%s'
								                % (bar.vtSymbol, bar.time, bar.open, bar.high,
								                   bar.low, bar.close))

							bar.vtSymbol = drTick.vtSymbol
							bar.symbol = drTick.symbol
							bar.exchange = drTick.exchange

							bar.open = drTick.lastPrice
							bar.high = drTick.lastPrice
							bar.low = drTick.lastPrice
							bar.close = drTick.lastPrice

							bar.date = drTick.date
							bar.time = drTick.time
							bar.datetime = drTick.datetime
							bar.volume = drTick.volume
							bar.openInterest = drTick.openInterest
							# 否则继续累加新的K线
						else:
							bar.high = max(bar.high, drTick.lastPrice)
							bar.low = min(bar.low, drTick.lastPrice)
							bar.close = drTick.lastPrice
		for instrument in ninetoeleven:
			if instrument in vtSymbol:
				whether_in_list=True
		for instrument in ninetohalfeleven:
			if instrument in vtSymbol:
				whether_in_list=True
		if whether_in_list==False:#如果不在特殊列表里就只存白天的数据
			time_f = datetime.now()
			if (time_f >= datetime.today().replace(hour=9, minute=0, second=0) and time_f <= datetime.today().replace(hour=15, minute=0, second=0)):
				# 更新Tick数据
				if vtSymbol in self.tickDict:
					self.insertData(TICK_DB_NAME, vtSymbol, drTick)

					if vtSymbol in self.activeSymbolDict:
						activeSymbol = self.activeSymbolDict[vtSymbol]
						self.insertData(TICK_DB_NAME, activeSymbol, drTick)

					# 发出日志
					self.writeDrLog(u'记录Tick数据%s，时间:%s, last:%s, bid:%s, ask:%s'
					                % (
					                        drTick.vtSymbol, drTick.time, drTick.lastPrice, drTick.bidPrice1,
					                        drTick.askPrice1))

				# 更新分钟线数据
				if vtSymbol in self.barDict:
					bar = self.barDict[vtSymbol]

					# 如果第一个TICK或者新的一分钟
					if not bar.datetime or bar.datetime.minute != drTick.datetime.minute:
						if bar.vtSymbol:
							newBar = copy.copy(bar)
							self.insertData(MINUTE_DB_NAME, vtSymbol, newBar)

							if vtSymbol in self.activeSymbolDict:
								activeSymbol = self.activeSymbolDict[vtSymbol]
								self.insertData(MINUTE_DB_NAME, activeSymbol, newBar)

							self.writeDrLog(u'记录分钟线数据%s，时间:%s, O:%s, H:%s, L:%s, C:%s'
							                % (bar.vtSymbol, bar.time, bar.open, bar.high,
							                   bar.low, bar.close))

						bar.vtSymbol = drTick.vtSymbol
						bar.symbol = drTick.symbol
						bar.exchange = drTick.exchange

						bar.open = drTick.lastPrice
						bar.high = drTick.lastPrice
						bar.low = drTick.lastPrice
						bar.close = drTick.lastPrice

						bar.date = drTick.date
						bar.time = drTick.time
						bar.datetime = drTick.datetime
						bar.volume = drTick.volume
						bar.openInterest = drTick.openInterest
						# 否则继续累加新的K线
					else:
						bar.high = max(bar.high, drTick.lastPrice)
						bar.low = min(bar.low, drTick.lastPrice)
						bar.close = drTick.lastPrice
	#----------------------------------------------------------------------
	def registerEvent(self):
		"""注册事件监听"""
		self.eventEngine.register(EVENT_TICK, self.procecssTickEvent)

	#----------------------------------------------------------------------
	def insertData(self, dbName, collectionName, data):
		"""插入数据到数据库（这里的data可以是CtaTickData或者CtaBarData）"""
		self.queue.put((dbName, collectionName, data.__dict__))

	#----------------------------------------------------------------------
	def run(self):
		"""运行插入线程"""
		while self.active:
			try:
				dbName, collectionName, d = self.queue.get(block=True, timeout=1)
				self.mainEngine.dbInsert(dbName, collectionName, d)
			except Empty:
				pass
	#----------------------------------------------------------------------
	def start(self):
		"""启动"""
		self.active = True
		self.thread.start()

	#----------------------------------------------------------------------
	def stop(self):
		"""退出"""
		if self.active:
			self.active = False
			self.thread.join()

	#----------------------------------------------------------------------
	def writeDrLog(self, content):
		"""快速发出日志事件"""
		log = VtLogData()
		log.logContent = content
		event = Event(type_=EVENT_DATARECORDER_LOG)
		event.dict_['data'] = log
		self.eventEngine.put(event)   


def procecssBar(self,bar):
	vtSymbol = bar.vtSymbol
	if vtSymbol in self.m5barDict :
		m5bar = self.m5barDict[vtSymbol]
		if not  m5bar.datetime or bar.datetime.minute % 5 == 1:
			m5bar.vtSymbol = bar.vtSymbol
			m5bar.symbol = bar.vtSymbol
			m5bar.exchange = bar.exchange

			m5bar.open = bar.open
			m5bar.high = bar.high
			m5bar.low = bar.low
			m5bar.close = bar.close
			m5bar.date = bar.date
			m5bar.time = bar.time
			m5bar.datetime = bar.datetime
			m5bar.volume = bar.volume
			m5bar.openInterest = bar.openInterest
		else:
			m5bar.high = max(m5bar.high, bar.high)
			m5bar.low = min(m5bar.low, bar.low)
			m5bar.close = bar.close
			m5bar.volume = m5bar.volume + bar.volume
			m5bar.openInterest = bar.openInterest

		if bar.datetime.minute % 5 == 0:
			newBar = copy.copy(m5bar)
			newBar.datetime = bar.datetime.replace(second=0,microsecond=0)
			newBar.date = bar.date
			newBar.time = bar.time
			self.insertData(MINUTE5_DB_NAME, vtSymbol, newBar)

			if vtSymbol in self.activeSymbolDict:
				activeSymbol = self.activeSymbolDict[vtSymbol]
				self.insertData(MINUTE5_DB_NAME, activeSymbol, newBar)

			self.writeDrLog(u'记录5分钟线数据%s，时间:%s, O:%s, H:%s, L:%s, C:%s'
			                %(newBar.vtSymbol, newBar.time, newBar.open, newBar.high,
			                  newBar.low, newBar.close))

#-----------------------------------------------------------------------
	def tickInTime(self,d):
		isSymbol = False
		isTime = False
		for symbol, times in self.timeDict.items():
			if symbol == d.vtSymbol:
				isSymbol = True
			for time in times:
				start = datetime.strptime(time[0],"%H:%M")
				end = datetime.strptime(time[1],"%H:%M")
				time1 = datetime.strptime(d.time,"%H:%M:%S.%f").replace(second=0,microsecond=0)
				if time1 >= start and time1 <=end :
					isTime = True
		if isSymbol and isTime:
			return True


		#-----------------------------------------------------------------------
	def barInTime(self,d):
		isSymbol = False
		isTime = False
		for symbol, times in self.timeDict.items():
			if symbol == d.vtSymbol:
				isSymbol = True
			for time in times:
				start = datetime.strptime(time[0],"%H:%M")
				end = datetime.strptime(time[1],"%H:%M")
				time1 = datetime.strptime(d.time,"%H:%M:%S.%f").replace(second=0,microsecond=0)
				if time1 > start and time1 <=end or time1 == datetime.strptime("00:00","%H:%M"):
					isTime = True
		if isSymbol and isTime:
			return True




