# encoding: UTF-8

"""
仅在知乎Live《vn.py的过去、现在和将来》中分享，请勿外传。

基于布林通道通道的交易策略，适合用在股指上5分钟线上。
"""

from __future__ import division

import talib
import numpy as np

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import EMPTY_STRING
from vnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate


########################################################################
class BollingerBotStrategy(CtaTemplate):
    """基于布林通道的交易策略"""
    className = 'BollingerBotStrategy'
    author = u'用Python的交易员'

    # 策略参数
    bollLength = 28         # 通道窗口数
    entryDev = 3.2          # 开仓偏差
    exitDev = 1.2           # 平仓偏差
    trailingPrcnt = 0.4     # 移动止损百分比
    maLength = 10           # 过滤用均线窗口
    initDays = 10           # 初始化数据所用的天数
    fixedSize = 1           # 每次交易的数量

    # 策略变量
    bar = None                  # 1分钟K线对象
    barMinute = EMPTY_STRING    # K线当前的分钟
    fiveBar = None              # 1分钟K线对象

    bufferSize = 40                     # 需要缓存的数据的大小
    bufferCount = 0                     # 目前已经缓存了的数据的计数
    highArray = np.zeros(bufferSize)    # K线最高价的数组
    lowArray = np.zeros(bufferSize)     # K线最低价的数组
    closeArray = np.zeros(bufferSize)   # K线收盘价的数组
    
    bollMid = 0                         # 布林带中轨
    bollStd = 0                         # 布林带宽度
    entryUp = 0                         # 开仓上轨
    exitUp = 0                          # 平仓上轨    
    
    maFilter = 0                        # 均线过滤
    maFilter1 = 0                       # 上一期均线                   
    
    intraTradeHigh = 0                  # 持仓期内的最高点  
    longEntry = 0                       # 多头开仓
    longExit = 0                        # 多头平仓    
    
    orderList = []                      # 保存委托代码的列表

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'bollLength',
                 'entryDev',
                 'exitDev',
                 'trailingPrcnt',
                 'maLength',
                 'initDays',
                 'fixedSize']    

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'bollMid',
               'bollStd',
               'entryUp',
               'exitUp',
               'intraTradeHigh',
               'longEntry',
               'shortEntry']  

    #----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(BollingerBotStrategy, self).__init__(ctaEngine, setting)
        
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
        if (bar.datetime.minute + 1) % 5 == 0:
            # 如果已经有聚合5分钟K线
            if self.fiveBar:
                # 将最新分钟的数据更新到目前5分钟线中
                fiveBar = self.fiveBar
                fiveBar.high = max(fiveBar.high, bar.high)
                fiveBar.low = min(fiveBar.low, bar.low)
                fiveBar.close = bar.close
                
                # 推送5分钟线数据
                self.onFiveBar(fiveBar)
                
                # 清空5分钟线数据缓存
                self.fiveBar = None
        else:
            # 如果没有缓存则新建
            if not self.fiveBar:
                fiveBar = VtBarData()
                
                fiveBar.vtSymbol = bar.vtSymbol
                fiveBar.symbol = bar.symbol
                fiveBar.exchange = bar.exchange
            
                fiveBar.open = bar.open
                fiveBar.high = bar.high
                fiveBar.low = bar.low
                fiveBar.close = bar.close
            
                fiveBar.date = bar.date
                fiveBar.time = bar.time
                fiveBar.datetime = bar.datetime 
                
                self.fiveBar = fiveBar
            else:
                fiveBar = self.fiveBar
                fiveBar.high = max(fiveBar.high, bar.high)
                fiveBar.low = min(fiveBar.low, bar.low)
                fiveBar.close = bar.close
    
    #----------------------------------------------------------------------
    def onFiveBar(self, bar):
        """收到5分钟K线"""
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
        self.bollMid = talib.MA(self.closeArray, self.bollLength)[-1]
        self.bollStd = talib.STDDEV(self.closeArray, self.bollLength)[-1]
        self.entryUp = self.bollMid + self.bollStd * self.entryDev
        self.exitUp = self.bollMid + self.bollStd * self.exitDev
        
        maArray = talib.MA(self.closeArray, self.maLength)
        self.maFilter = maArray[-1]
        self.maFilter1 = maArray[-2]
        
        # 判断是否要进行交易
    
        # 当前无仓位，发送OCO开仓委托
        if self.pos == 0:
            self.intraTradeHigh = bar.high
            
            if bar.close > self.maFilter and self.maFilter > self.maFilter1:
                self.longEntry = self.entryUp
                
                orderID = self.buy(self.longEntry, self.fixedSize, True)
                self.orderList.append(orderID)
    
        # 持有多头仓位
        elif self.pos > 0:
            self.intraTradeHigh = max(self.intraTradeHigh, bar.high)
            
            self.longExit = self.intraTradeHigh * (1 - self.trailingPrcnt/100)
            self.longExit = min(self.longExit, self.exitUp)
            
            orderID = self.sell(self.longExit, abs(self.pos), True)
            self.orderList.append(orderID)
    
        # 发出状态更新事件
        self.putEvent()        

    #----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    #----------------------------------------------------------------------
    def onTrade(self, trade):
        # 发出状态更新事件
        self.putEvent()

    #----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass