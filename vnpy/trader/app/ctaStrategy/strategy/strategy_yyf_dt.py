# encoding: UTF-8

"""
DualThrust交易策略
"""

import datetime
import math
import copy
import talib
import numpy as np

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import EMPTY_STRING
from vnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate

from vnpy.trader.vtConstant import (EMPTY_STRING, EMPTY_UNICODE, 
                                    EMPTY_FLOAT, EMPTY_INT)

########################################################################
class YYFDualThrustStrategy(CtaTemplate):
    """DualThrust交易策略"""
    className = 'YYFDualThrustStrategy'
    author = u'用Python的交易员'

    # 策略参数
    lots = 1
    k1 = 0.4
    k2 = 0.6
    d1 = 1
    d2 = 1
    
    initCapital = 10000
    riskPercent=0.03
    totalEquity = initCapital

    initDays = 10

    # 策略变量
    bar = None                  # K线对象
    barMinute = EMPTY_STRING    # K线当前的分钟
    barList = []                # K线对象的列表

    dayBar = None
   
    rangeUp = 0
    rangeDn = 0
    
    longEntry = 0
    shortEntry = 0
    
    bufferSize = 10                    # 需要缓存的数据的大小
    bufferCount = 0                     # 目前已经缓存了的数据的计数

    highArray = np.zeros(bufferSize)    # K线最高价的数组
    lowArray = np.zeros(bufferSize)     # K线最低价的数组
    closeArray = np.zeros(bufferSize)   # K线收盘价的数组
    openArray = np.zeros(bufferSize)    # K线开盘价的数组

    HHValue = 0                         # N天最高价的最高价
    HCValue = 0                         # N天收盘价的最高价
    LLValue = 0                         # N天最低价的最低价
    LCValue = 0                         # N天收盘价的最低价    

    longEntered = False
    shortEntered = False

    orderList = []                      # 保存委托代码的列表

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol','riskPercent',
                 'k1','d1',
                 'k2','d2']    

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'range',
               'longEntry',
               'shortEntry',
               'exitTime']  

    #----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(YYFDualThrustStrategy, self).__init__(ctaEngine, setting) 
        
        self.barList = []

    #----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' %self.name)
    
        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.loadBar(self.initDays)
        for bar in initData:
            self.onBar(bar)

        self.putEvent()

    #----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略启动' %self.name)
        self.putEvent()

    #----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略停止' %self.name)
        self.putEvent()

    #----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        # 计算K线
        tickMinute = tick.datetime.minute

        if tickMinute != self.barMinute:    
            if self.bar:
                self.onBar(self.bar)

            bar = VtBarData()              
            bar.vtSymbol = tick.vtSymbol
            bar.symbol = tick.symbol
            bar.exchange = tick.exchange

            bar.open = tick.lastPrice
            bar.high = tick.lastPrice
            bar.low = tick.lastPrice
            bar.close = tick.lastPrice

            bar.date = tick.date
            bar.time = tick.time
            bar.datetime = tick.datetime    # K线的时间设为第一个Tick的时间

            self.bar = bar                  # 这种写法为了减少一层访问，加快速度
            self.barMinute = tickMinute     # 更新当前的分钟
        else:                               # 否则继续累加新的K线
            bar = self.bar                  # 写法同样为了加快速度

            bar.high = max(bar.high, tick.lastPrice)
            bar.low = min(bar.low, tick.lastPrice)
            bar.close = tick.lastPrice

    #----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        
        d=self.calculateTradeResult()
        if d.has_key('capital'):
            self.totalEquity=self.totalEquity+d['capital']
            print self.initCapital,self.totalEquity,d['capital']
    
        if self.totalEquity<=0:
            return        
        
        stoploss=self.k1*self.rangeUp+self.k2*self.rangeDn
        if stoploss!=0 :
            self.lots=math.floor(self.totalEquity*self.riskPercent/(self.ctaEngine.size*stoploss))
        if self.lots==0:
            self.lots=1          
        
        # 撤销之前发出的尚未成交的委托（包括限价单和停止单）
        for orderID in self.orderList:
            self.cancelOrder(orderID)
        self.orderList = []        
        
                
        # 新的一天
        if bar.datetime.hour==14 and bar.datetime.minute==59:     #夜盘是从21:00开始，早盘从9:00开始，回测时第一根dayBar从早上9点开始，后续都是从14:59分后，自动增加一个dayBar
        #if lastBar.datetime.hour==15 :
            if self.dayBar:
                self.dayBar.high = max(self.dayBar.high, bar.high)
                self.dayBar.low = min(self.dayBar.low, bar.low)
                self.dayBar.close=bar.close                
                self.onDayBar(self.dayBar)
            
            # 14:59之后的算作下一天的Bar
            self.dayBar = VtBarData()
            self.dayBar.date = bar.datetime.date() + datetime.timedelta(days=1)
            self.dayBar.exchange = bar.exchange           
        else:
            if self.dayBar:               
                if self.dayBar.open == EMPTY_FLOAT:  #表明是今天的第一根Bar。
                    self.dayBar.open = bar.open
                    self.dayBar.high = bar.high
                    self.dayBar.low = bar.low
                    self.dayBar.close = bar.close                
                else:    
                    self.dayBar.high = max(self.dayBar.high, bar.high)
                    self.dayBar.low = min(self.dayBar.low, bar.low)
                    self.dayBar.close=bar.close
        
        # 尚未到收盘
        if not self.rangeUp  or not  self.dayBar  or self.dayBar.open == EMPTY_FLOAT :
            return
        
        print bar.datetime,self.rangeUp,self.rangeDn,self.longEntry,self.shortEntry,self.lots,self.totalEquity,stoploss,self.dayBar.open,self.dayBar.high,self.dayBar.low,self.dayBar.close,bar.open,bar.high,bar.low,bar.close
        
        self.longEntry = self.dayBar.open + self.k1 * self.rangeUp
        self.shortEntry = self.dayBar.open - self.k2 * self.rangeDn         

        if self.pos == 0:
            vtOrderID = self.buy(self.longEntry, self.lots, stop=True)
            self.orderList.append(vtOrderID)

            vtOrderID = self.short(self.shortEntry, self.lots, stop=True)
            self.orderList.append(vtOrderID)

        # 持有多头仓位
        elif self.pos > 0:

            # 多头止损单
            vtOrderID = self.sell(self.shortEntry, abs(self.pos), stop=True)
            #print bar.datetime,'long stoploss:',self.shortEntry,self.pos
            self.orderList.append(vtOrderID)
            
            # 空头开仓单
            vtOrderID = self.short(self.shortEntry, self.lots, stop=True)
            #print bar.datetime,'short open:',self.shortEntry,self.lots
            self.orderList.append(vtOrderID)
            
        # 持有空头仓位
        elif self.pos < 0:

            # 空头止损单
            vtOrderID = self.cover(self.longEntry, abs(self.pos), stop=True)
            self.orderList.append(vtOrderID)
            
            # 多头开仓单

            vtOrderID = self.buy(self.longEntry, self.lots, stop=True)
            self.orderList.append(vtOrderID)  
            
       
 
        # 发出状态更新事件
        self.putEvent()

    #----------------------------------------------------------------------
    def onDayBar(self,dayBar):
        # 保存K线数据
        self.closeArray[0:self.bufferSize-1] = self.closeArray[1:self.bufferSize]
        self.highArray[0:self.bufferSize-1] = self.highArray[1:self.bufferSize]
        self.lowArray[0:self.bufferSize-1] = self.lowArray[1:self.bufferSize]
        self.openArray[0:self.bufferSize-1] = self.openArray[1:self.bufferSize]
        self.closeArray[-1] = dayBar.close
        self.highArray[-1] = dayBar.high
        self.lowArray[-1] = dayBar.low
        self.openArray[-1] = dayBar.open
        
        #print "dayBar:",dayBar.date,dayBar.open,dayBar.high,dayBar.low,dayBar.close
    
        self.bufferCount += 1
        if self.bufferCount < self.bufferSize:
            return
    
        # 计算指标数值
        if self.d1==1:
            self.HHValue = self.highArray[-1]
            self.HCValue = self.closeArray[-1]
            self.LLValue = self.lowArray[-1]
            self.LCValue = self.closeArray[-1]            
        else:
            self.HHValue = talib.MAX(self.highArray, timeperiod=self.d1)[-1]
            self.HCValue = talib.MAX(self.closeArray, timeperiod=self.d1)[-1]
            self.LLValue = talib.MIN(self.lowArray, timeperiod=self.d1)[-1]
            self.LCValue = talib.MIN(self.closeArray, timeperiod=self.d1)[-1]
        
        self.rangeUp = max(self.HHValue - self.LCValue, self.HCValue - self.LLValue)       
       
        if self.d2==1:
            self.HHValue = self.highArray[-1]
            self.HCValue = self.closeArray[-1]
            self.LLValue = self.lowArray[-1]
            self.LCValue = self.closeArray[-1]            
        else: 
            self.HHValue = talib.MAX(self.highArray, timeperiod=self.d2)[-1]
            self.HCValue = talib.MAX(self.closeArray, timeperiod=self.d2)[-1]
            self.LLValue = talib.MIN(self.lowArray, timeperiod=self.d2)[-1]
            self.LCValue = talib.MIN(self.closeArray, timeperiod=self.d2)[-1]
    
        self.rangeDn = max(self.HHValue - self.LCValue, self.HCValue - self.LLValue)   
           

        self.putEvent()        

    #----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    #----------------------------------------------------------------------
    #----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交推送（必须由用户继承实现）"""
        # 对于无需做细粒度委托控制的策略，可以忽略onOrder
        print trade.tradeTime,"---onTrade----> pos=",self.pos," orderID=",trade.orderID, " price=", trade.price
        self.tradeList.append(copy.copy(trade))
        pass

    #----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass