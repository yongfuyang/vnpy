# encoding: UTF-8

"""
基于King Keltner通道的交易策略，适合用在股指上，
展示了OCO委托和5分钟K线聚合的方法。

注意事项：
1. 作者不对交易盈利做任何保证，策略代码仅供参考
2. 本策略需要用到talib，没有安装的用户请先参考www.vnpy.org上的教程安装
3. 将IF0000_1min.csv用ctaHistoryData.py导入MongoDB后，直接运行本文件即可回测策略
"""

from __future__ import division

import talib
import numpy as np

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import EMPTY_STRING
from vnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate

# Helk added
import datetime


########################################################################
class YYFKkStrategy(CtaTemplate):
    """基于King Keltner通道的交易策略"""
    className = 'YYFKkStrategy'
    author = u'用Python的交易员'

    # 策略参数
    kkLength = 20           # 计算通道中值的窗口数
    atrLength = 45           # 
    kkDev = 2             # 计算通道宽度的偏差
    trailingPrcnt = 0.8     # 移动止损
    initDays = 10           # 初始化数据所用的天数
    fixedSize = 1           # 每次交易的数量

    # 策略变量
    bar = None                  # 1分钟K线对象
    barMinute = EMPTY_STRING    # K线当前的分钟
    NBar = None              # 1分钟K线对象

    # Helk added
    timeFrame = 5
    currentBucket = None

    entryPrice = 0
    initStopATRs = 2
    breakEvenStopATRs = 2.5
    trailingStopATRs = 3
    bigPointValue= 300 # Should be changed if goods changed

    bufferSize = 100                    # 需要缓存的数据的大小
    bufferCount = 0                     # 目前已经缓存了的数据的计数
    highArray = np.zeros(bufferSize)    # K线最高价的数组
    lowArray = np.zeros(bufferSize)     # K线最低价的数组
    closeArray = np.zeros(bufferSize)   # K线收盘价的数组
    highAfterEntry = np.zeros(bufferSize)
    lowAfterEntry = np.zeros(bufferSize)
    
    atrValue = 0                        # 最新的ATR指标数值
    kkMid = 0                           # KK通道中轨
    kkUp = 0                            # KK通道上轨
    kkDown = 0                          # KK通道下轨
    intraTradeHigh = 0                  # 持仓期内的最高点
    intraTradeLow = 0                   # 持仓期内的最低点

    buyOrderID = None                   # OCO委托买入开仓的委托号
    shortOrderID = None                 # OCO委托卖出开仓的委托号
    orderList = []                      # 保存委托代码的列表

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'kkLength',
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
        super(YYFKkStrategy, self).__init__(ctaEngine, setting)
        
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
        # 聚合为1分钟K线
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
                bucket =  datetime.datetime(year, month, day) + datetime.timedelta(days=1)
                
            else:
                #bucket =  datetime.datetime(year, month, day)
                bucket =  datetime.datetime(year, month, day)
        else :
            bucket =  datetime.datetime(year, month, day, hour, minute-minute%self.timeFrame)


        if self.currentBucket != bucket:
            
            self.currentBucket = bucket
            # 如果已经有聚合5分钟K线
            if self.NBar:
                # 将最新分钟的数据更新到目前5分钟线中
                NBar = self.NBar
                NBar.high = max(NBar.high, bar.high)
                NBar.low = min(NBar.low, bar.low)
                NBar.close = bar.close
                
                # 推送5分钟线数据
                self.onNBar(NBar)
                
                # 清空5分钟线数据缓存
                self.NBar = None
            else:         #处理第一根bar
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
                NBar.datetime = bar.datetime 
            
                self.NBar = NBar                
        else:
            # 如果没有缓存则新建
            if not self.NBar:
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
                NBar.datetime = bar.datetime 
                
                self.NBar = NBar
            else:
                NBar = self.NBar
                NBar.high = max(NBar.high, bar.high)
                NBar.low = min(NBar.low, bar.low)
                NBar.close = bar.close    
    #----------------------------------------------------------------------
    def onNBar(self, bar):
        """收到5分钟K线"""
        # 撤销之前发出的尚未成交的委托（包括限价单和停止单）
        for orderID in self.orderList:
            self.cancelOrder(orderID)
        self.orderList = []
    
        # 保存K线数据
        self.closeArray[0:self.bufferSize-1] = self.closeArray[1:self.bufferSize]
        self.highArray[0:self.bufferSize-1] = self.highArray[1:self.bufferSize]
        self.lowArray[0:self.bufferSize-1] = self.lowArray[1:self.bufferSize]
        self.highAfterEntry[0:self.bufferSize-1] = self.highAfterEntry[1:self.bufferSize]
        self.lowAfterEntry[0:self.bufferSize-1] = self.lowAfterEntry[1:self.bufferSize]
    
        self.closeArray[-1] = bar.close
        self.highArray[-1] = bar.high
        self.lowArray[-1] = bar.low
        
    
        self.bufferCount += 1
        if self.bufferCount < self.bufferSize:
            return
    
        # 计算指标数值
        self.atrValue = talib.ATR(self.highArray, 
                                  self.lowArray, 
                                  self.closeArray,
                                  self.atrLength)[-1]
        self.kkMid = talib.MA(self.closeArray, self.kkLength)[-1]
        self.kkUp = self.kkMid + self.atrValue * self.kkDev
        self.kkDown = self.kkMid - self.atrValue * self.kkDev
    
        if self.highAfterEntry[-2]==0:
            self.highAfterEntry[-1]=self.kkUp
        else:
            self.highAfterEntry[-1]=self.highAfterEntry[-2]
            
        if self.lowAfterEntry[-2]==0:
            self.lowAfterEntry[-1]=self.kkDown
        else:
            self.lowAfterEntry[-1]=self.lowAfterEntry[-2]
            
        
        print bar.datetime,self.highAfterEntry[-1],self.lowAfterEntry[-1],self.kkUp,self.kkDown,self.atrValue
        
        # 判断是否要进行交易
    
        # 当前无仓位，发送OCO开仓委托
        if self.pos == 0:
            #self.intraTradeHigh = bar.high
            #self.intraTradeLow = bar.low            
            self.sendOcoOrder(self.highAfterEntry[-1], self.lowAfterEntry[-1], self.fixedSize)
    
        # 持有多头仓位
        elif self.pos > 0:
            self.highAfterEntry[-1] = max(self.highAfterEntry[-1], bar.high)
            self.lowAfterEntry[-1] = min(self.lowAfterEntry[-1],bar.low)

            #Helk added
            _stopLine=self.entryPrice-self.atrValue*self.initStopATRs;
            if (self.highAfterEntry[-1]  > self.entryPrice + self.atrValue * self.breakEvenStopATRs):
                _stopLine=self.entryPrice
            if (_stopLine < self.highAfterEntry[-1]  - self.atrValue*self.trailingStopATRs):
                _stopLine=self.highAfterEntry[-1]  - self.atrValue*self.trailingStopATRs


            orderID = self.sell(_stopLine, abs(self.pos), True)
            self.orderList.append(orderID)
            
            
            #orderID = self.sell(self.intraTradeHigh*(1-self.trailingPrcnt/100),abs(self.pos), True)
            #self.orderList.append(orderID)
    
        # 持有空头仓位
        elif self.pos < 0:
            self.highAfterEntry[-1]  = max(self.highAfterEntry[-1] , bar.high)
            self.lowAfterEntry[-1]  = min(self.lowAfterEntry[-1] ,bar.low)
            
            #self.intraTradeHigh = bar.high
            #self.intraTradeLow = min(self.intraTradeLow, bar.low)
            
            _stopLine=self.entryPrice+self.atrValue*self.initStopATRs;
            if (self.lowAfterEntry[-1]  < self.entryPrice - self.atrValue * self.breakEvenStopATRs):
                _stopLine=self.entryPrice
            if (_stopLine > self.lowAfterEntry[-1]  + self.atrValue*self.trailingStopATRs):
                _stopLine=self.lowAfterEntry[-1]  + self.atrValue*self.trailingStopATRs

            orderID = self.cover(_stopLine, abs(self.pos), True)
            self.orderList.append(orderID)

            #orderID = self.cover(self.intraTradeLow*(1+self.trailingPrcnt/100),abs(self.pos), True)
            #self.orderList.append(orderID)
    
        # 发出状态更新事件
        self.putEvent()        

    #----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    #----------------------------------------------------------------------
    def onTrade(self, trade):
        # 多头开仓成交后，撤消空头委托
        if self.pos > 0:
            self.lowAfterEntry[-1] =self.highAfterEntry[-1] 
            
            self.cancelOrder(self.shortOrderID)
            if self.buyOrderID in self.orderList:
                self.orderList.remove(self.buyOrderID)
            if self.shortOrderID in self.orderList:
                self.orderList.remove(self.shortOrderID)
        # 反之同样
        elif self.pos < 0:
            self.highAfterEntry[-1] =self.lowAfterEntry[-1] 
            
            self.cancelOrder(self.buyOrderID)
            if self.buyOrderID in self.orderList:
                self.orderList.remove(self.buyOrderID)
            if self.shortOrderID in self.orderList:
                self.orderList.remove(self.shortOrderID)
        
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
    engine.setStartDate('20130101')
    
    # 设置产品相关参数
    engine.setSlippage(0.2)     # 股指1跳
    engine.setRate(1.0/10000)   # 万0.3
    engine.setSize(300)         # 股指合约大小 
    #engine.setPriceTick(0.2)    # 股指最小价格变动       
    
    # 设置使用的历史数据库
    engine.setDatabase(MINUTE_DB_NAME, 'IF0000')
    
    # 在引擎中创建策略对象
    d = {"timeFrame":30, "currentBucket": None}
    engine.initStrategy(YYFKkStrategy, d)
    
    # 开始跑回测
    engine.runBacktesting()
    
    # 显示回测结果
    engine.showBacktestingResult()