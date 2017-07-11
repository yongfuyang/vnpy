# encoding: UTF-8

"""
基于turtle的交易策略，适合用在股指上，
展示了OCO委托和5分钟K线聚合的方法。

注意事项：
1. 作者不对交易盈利做任何保证，策略代码仅供参考
2. 本策略需要用到talib，没有安装的用户请先参考www.vnpy.org上的教程安装
3. 将IF0000_1min.csv用ctaHistoryData.py导入MongoDB后，直接运行本文件即可回测策略
"""

from __future__ import division

import talib
import numpy as np

#from ctaBase import *
#from ctaBase import CtaBarData as VtBarData
#from ctaTemplate import CtaTemplate


import datetime
from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import EMPTY_STRING
from vnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate
#from datatime import datetime as datetime.




########################################################################
class YYFTurtleStrategy(CtaTemplate):
    """基于King Keltner通道的交易策略"""
    className = 'YYFTurtleStrategy'
    author = u'用Python的交易员'


    engine = None
    # 策略参数


    initDays = 10

    RiskRatio = 1.0 #% Risk Per N ( 0 - 100)
    ATRLength = 20 #平均波动周期 ATR Length
    boLength = 20 #短周期 BreakOut Length
    fsLength = 55 #长周期 FailSafe Length
    teLength = 10 #离市周期 Trailing Exit Length
    LastProfitableTradeFilter = False #使用入市过滤条件

    # 策略变量
    bar = None                  # 1分钟K线对象
    barMinute = EMPTY_STRING    # K线当前的分钟
    NBar = None              # 1分钟K线对象

    # Helk added
    timeFrame = 30
    currentBucket = None

    BigPointValue = 300
    MinPoint = 0.1                       # 最小变动单位 ?
    AvgTR = None                    # ATR
    TR = None
    N  = None                   # N 值
    TotalEquity = 1000000                    # 按最新收盘价计算出的总资产
    TurtleUnits = None                    # 交易单位
    DonchianHi = None               # 唐奇安通道上轨，延后1个Bar
    DonchianLo = None              # 唐奇安通道下轨，延后1个Bar
    fsDonchianHi = None             # 唐奇安通道上轨，延后1个Bar，长周期
    fsDonchianLo = None             # 唐奇安通道下轨，延后1个Bar，长周期  
    ExitHighestPrice = None               # 离市时判断需要的N周期最高价
    ExitLowestPrice = None                # 离市时判断需要的N周期最低价
    myEntryPrice = None                   # 开仓价格
    myExitPrice = None                    # 平仓价格
    SendOrderThisBar = False           # 当前Bar有过交易
    preEntryPrice = 0         # 前一次开仓的价格
    PreBreakoutFailure = False   # 前一次突破是否失败
    BarsSinceLastEntry = 0 # 最近一次加仓距离当前Bar的Bar数目
    stopOrderID=None
    exitOrderID=None
    

    bufferSize = 100                    # 需要缓存的数据的大小
    bufferCount = 0                     # 目前已经缓存了的数据的计数
    highArray = np.zeros(bufferSize)    # K线最高价的数组
    lowArray = np.zeros(bufferSize)     # K线最低价的数组
    closeArray = np.zeros(bufferSize)   # K线收盘价的数组

    buyOrderID = None                   # OCO委托买入开仓的委托号
    shortOrderID = None                 # OCO委托卖出开仓的委托号
    orderList = []                      # 保存委托代码的列表


    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'ATRLength',
                 'kkDev',
                 'atrLength',
                 'timeFrame']    

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'atrValue',
               'kkMid',
               'kkUp',
               'kkDown']  

    #----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(YYFTurtleStrategy, self).__init__(ctaEngine, setting)
        
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
        # 聚合为1分K钟K线
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
        # 如果当前是一个5分钟走完


        time = bar.datetime #datetime.datetime.strptime(bar.datetime, '%Y%m%d %H:%M:%S')
                #
        year = time.year
        month = time.month
        day = time.day
        hour = time.hour
        minute = time.minute

        if self.timeFrame == 0: # 1day
            if hour >= 20:
                bucket =  datetime.datetime(year, month, day) + timedelta(days=1)
                
            else:
                
                bucket =  datetime.datetime(year, month, day)
        else :
            bucket =  datetime.datetime(year, month, day, hour, minute-minute%self.timeFrame)


        if self.currentBucket != bucket:
            
            self.currentBucket = bucket
            # 如果已经有聚合5分钟K线
            if self.NBar:         
                # 推送5分钟线数据
                self.onNBar(self.NBar)
                
                
            # 清空5分钟线数据缓存
            self.NBar = None
        
            NBar = VtBarData()                
            NBar.vtSymbol = bar.vtSymbol
            NBar.symbol = bar.symbol
            NBar.exchange = bar.exchange
        
            NBar.open = bar.open
            NBar.high = bar.high
            NBar.low = bar.low
            NBar.close = bar.close
        
            NBar.date = bar.date
            NBar.time = bar.time
            NBar.datetime = bucket
        
            self.NBar = NBar                
        else:
            NBar = self.NBar
            NBar.high = max(NBar.high, bar.high)
            NBar.low = min(NBar.low, bar.low)
            NBar.close = bar.close    
    #----------------------------------------------------------------------
    def onNBar(self, bar):
        # 撤销之前发出的尚未成交的委托（包括限价单和停止单）
        for orderID in self.orderList:
            self.cancelOrder(orderID)
        self.orderList = []
    
        # 保存K线数据
        self.closeArray[0:self.bufferSize-1] = self.closeArray[1:self.bufferSize]
        self.highArray[0:self.bufferSize-1] = self.highArray[1:self.bufferSize]
        self.lowArray[0:self.bufferSize-1] = self.lowArray[1:self.bufferSize]
    
        self.closeArray[-1] = bar.close
        self.highArray[-1] = bar.high
        self.lowArray[-1] = bar.low


    
        self.bufferCount += 1
        if self.bufferCount < self.bufferSize:
            return
    
        # 计算指标数值
        '''
        self.AvgTR = talib.ATR(self.highArray, 
                                  self.lowArray, 
                                  self.closeArray,
                                  self.ATRLength)
        '''
        
        self.TR = talib.TRANGE(self.highArray, 
                                  self.lowArray, 
                                  self.closeArray)
        
        self.AvgTR = talib.SMA(self.TR,self.ATRLength)
        
        self.N = self.AvgTR[-1]

        self.TurtleUnits = 1  #((self.TotalEquity)*self.RiskRatio/100) // (self.N * self.BigPointValue)


        #DonChianArray = self.closeArray[-self.fsLength-1:-1]
        self.DonchianHi = talib.MAX(self.highArray, self.boLength)[-1]
        self.DonchianLo = talib.MIN(self.lowArray, self.boLength)[-1]
        self.fsDonchianHi = talib.MAX(self.highArray, self.fsLength)[-1]
        self.fsDonchianLo = talib.MIN(self.lowArray, self.fsLength)[-1]

        self.ExitHighestPrice = talib.MAX(self.highArray,self.teLength)[-1]
        self.ExitLowestPrice = talib.MIN(self.lowArray,self.teLength)[-1]

        if(self.TurtleUnits==0):
            self.TurtleUnits=1
    
        # 判断是否要进行交易
    
        self.SendOrderThisBar = False

        '''
        if(self.pos == 0): 

            orderID = self.buy(self.fsDonchianHi,self.TurtleUnits, True)
            self.orderList.append(orderID)

            orderID = self.short(self.fsDonchianLo,self.TurtleUnits, True)
            self.orderList.append(orderID)
        '''         

        # 当不使用过滤条件，或者使用过滤条件并且条件为PreBreakoutFailure为True进行后续操作
        _entryPrice=self.preEntryPrice
        if(self.pos == 0 and ((not self.LastProfitableTradeFilter) or (self.PreBreakoutFailure))):
            orderID = self.buy(self.DonchianHi,self.TurtleUnits, True)
            self.orderList.append(orderID)                
        
            orderID = self.short(self.DonchianLo,self.TurtleUnits, True)
            self.orderList.append(orderID)
        
        elif (self.pos > 0):
            #self.exitOrderID = self.sell(self.ExitLowestPrice, abs(self.pos), True)  #设置全部退出的stop单
            #self.orderList.append(self.exitOrderID) 
        
            exitPrice=max(self.ExitLowestPrice,_entryPrice - 2*self.N)
            self.stopOrderID = self.sell(exitPrice, abs(self.pos), True) #设置止损stop单
            print self.stopOrderID," exitPrice=",exitPrice," ExitHighestPrice=",self.ExitHighestPrice," _entryPrice=",_entryPrice
            self.orderList.append(self.stopOrderID)            
        
            for i in range(1):
                orderID = self.buy(_entryPrice + 0.5*(i+1)*self.N,self.TurtleUnits, True) # 设置加仓的stop单
                self.orderList.append(orderID)
            
        elif (self.pos < 0):
            #self.exitOrderID = self.cover(self.ExitHighestPrice, abs(self.pos), True)  #设置全部退出的stop单
            #self.orderList.append(self.exitOrderID)  
            
            exitPrice=min(self.ExitHighestPrice,_entryPrice + 2*self.N)
            self.stopOrderID = self.cover(exitPrice, abs(self.pos), True) #设置止损stop单
            print self.stopOrderID," exitPrice=",exitPrice," ExitHighestPrice=",self.ExitHighestPrice," _entryPrice=",_entryPrice
            self.orderList.append(self.stopOrderID) 
            
            for i in range(1):
                orderID = self.short(_entryPrice - 0.5*(i+1)*self.N , self.TurtleUnits, True) # 设置加仓的stop单
                self.orderList.append(orderID)            
        
        print bar.datetime," o=",bar.open," c=",bar.close," h=",bar.high," l=",bar.low

        print bar.datetime,"up=",self.DonchianHi," dn=",self.DonchianLo," lup=",self.fsDonchianHi,"ldn=",self.fsDonchianLo,"N=",self.N," high=", bar.high, "low=",bar.low,"preEntryPrice=",self.preEntryPrice,"ExitHighestPrice=",self.ExitHighestPrice,"ExitLowestPrice=",self.ExitLowestPrice,"olist=",self.orderList,"pos=",self.pos

        self.BarsSinceLastEntry += 1 
        self.putEvent()        

    #-----------------------------------------------()-----------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    #----------------------------------------------------------------------
    def onTrade(self, trade):
        # 多头开仓成交后，撤消空头委托
        print trade.tradeTime,"---onTrade----> pos=",self.pos," orderID=",trade.orderID, " stopOrderID=", trade.tradeTime,trade.stopOrderID,trade.price
        
        if trade.stopOrderID== self.stopOrderID:
            self.PreBreakoutFailure = True
            self.cancelOrder(self.exitOrderID)
            
        if trade.stopOrderID== self.exitOrderID:
            self.cancelOrder(self.stopOrderID)
        
        if self.pos==0:
            self.cancelOrder(self.stopOrderID)
            if self.stopOrderID in self.orderList:
                self.orderList.remove(self.stopOrderID)
            
        if self.pos > 0:
                       
            self.BarsSinceLastEntry=0
            self.myEntryPrice = trade.price
            self.preEntryPrice = self.myEntryPrice
            self.SendOrderThisBar = True
            self.PreBreakoutFailure = False 
            
            
                        

        # 反之同样
        elif self.pos < 0:
                         
            self.BarsSinceLastEntry=0
            self.myEntryPrice = trade.price
            self.preEntryPrice = self.myEntryPrice
            self.SendOrderThisBar = True
            self.PreBreakoutFailure = False  
        
 
        
        # 发出状态更新事件
        self.putEvent()
        
    #----------------------------------------------------------------------
    def sendOcoOrder(self, buyPrice, shortPrice, volume):
        """
        发送OCO委托
        
        OCO(One Cancel Other)委托：
        1. 主要用于实现区间突破入场
        2. 包含两个方向相反的停止单
        3. 一个方向的停止单成交后会立即撤消另一个方向的
        """
        # 发送双边的停止单委托，并记录委托号
        self.buyOrderID = self.buy(buyPrice, volume, True)
        self.shortOrderID = self.short(shortPrice, volume, True)
        
        # 将委托号记录到列表中
        self.orderList.append(self.buyOrderID)
        self.orderList.append(self.shortOrderID)

    #----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass    
    
if __name__ == '__main__':
    # 提供直接双击回测的功能
    # 导入PyQt4的包是为了保证matplotlib使用PyQt4而不是PySide，防止初始化出错
    from ctaBacktesting import *
    from PyQt4 import QtCore, QtGui
    
    # 创建回测引擎
    engine = BacktestingEngine()
    
    # 设置引擎的回测模式为K线
    engine.setBacktestingMode(engine.BAR_MODE)

    # 设置回测用的数据起始日期
    engine.setStartDate('20120101')
    
    # 设置产品相关参数
    engine.setSlippage(0.2)     # 股指1跳
    engine.setRate(1.0/10000)   # 万0.3
    engine.setSize(300)         # 股指合约大小 
    #engine.setPriceTick(0.2)    # 股指最小价格变动       
    
    # 设置使用的历史数据库
    engine.setDatabase(MINUTE_DB_NAME, 'IF0000')
    
    # 在引擎中创建策略对象
    d = {"timeFrame":30, "currentBucket": None}
    engine.initStrategy(YYFTurtleStrategy, d)
    
    # 开始跑回测
    engine.runBacktesting()
    
    # 显示回测结果
    engine.showBacktestingResult()