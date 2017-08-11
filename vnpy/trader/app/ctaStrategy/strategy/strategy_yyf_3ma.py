# encoding: UTF-8

"""
这里的Demo是一个最简单的策略实现，并未考虑太多实盘中的交易细节，如：
1. 委托价格超出涨跌停价导致的委托失败
2. 委托未成交，需要撤单后重新委托
3. 断网后恢复交易状态
4. 等等
这些点是作者选择特意忽略不去实现，因此想实盘的朋友请自己多多研究CTA交易的一些细节，
做到了然于胸后再去交易，对自己的money和时间负责。
也希望社区能做出一个解决了以上潜在风险的Demo出来。
"""


import talib
import numpy as np

import math
import datetime
import vnpy.trader.tools as tools
import copy

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import EMPTY_STRING
from vnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate



########################################################################
class ThreeEmaStrategy(CtaTemplate):
    """双指数均线策略Demo"""
    className = 'ThreeEmaStrategy'
    author = u'yfyang'
    
    timeFrame = 30
    currentBucket = None 
    NBar = None              # N分钟K线对象
    
    # 策略参数
    fastK = 5     # 快速EMA参数
    slowK = 50     # 慢速EMA参数
    longK = 200
    initDays = 10   # 初始化数据所用的天数
    
    # 策略变量
    bar = None                  # 1分钟K线对象
    barMinute = EMPTY_STRING    # K线当前的分钟

    bufferSize = 201                    # 需要缓存的数据的大小
    bufferCount = 0                     # 目前已经缓存了的数据的计数
    closeArray = np.zeros(bufferSize)   # K线收盘价的数组  
    highArray = np.zeros(bufferSize)
    lowArray = np.zeros(bufferSize)
    
    fastMa = []             # 快速EMA均线数组
    slowMa = []             # 与上面相同
    longMa = []             # 与上面相同
    
    initCapital = 1000000
    riskPercent=0.01
    stopAtrs=2
    atrLength=20
    atr=None
    
    orderList=[]
    tradeList=[]
    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'fastK',
                 'slowK',
                 'initCapital','timeFrame',
                 'riskPercent','stopAtrs','atrLength']
    
    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos']  

    #----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(ThreeEmaStrategy, self).__init__(ctaEngine, setting)
        
        # 注意策略类中的可变对象属性（通常是list和dict等），在策略初始化时需要重新创建，
        # 否则会出现多个策略实例之间数据共享的情况，有可能导致潜在的策略逻辑错误风险，
        # 策略类中的这些可变对象属性可以选择不写，全都放在__init__下面，写主要是为了阅读
        # 策略时方便（更多是个编程习惯的选择）
        self.fastMa = []
        self.slowMa = []
        self.longMa = []        
        
        self.totalEquity = self.initCapital
        
    #----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'双EMA演示策略初始化')
        
        initData = self.loadBar(self.initDays)
        for bar in initData:
            self.onBar(bar)
        
        self.putEvent()
        
    #----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeCtaLog(u'双EMA演示策略启动')
        self.putEvent()
    
    #----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.writeCtaLog(u'双EMA演示策略停止')
        self.putEvent()
        
    #----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        # 计算K线
        tickMinute = tick.datetime.minute
        
        if tickMinute != self.barMinute:    
            if self.bar:
                self.onBar(self.bar)
            
            bar = CtaBarData()              
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
            
            # 实盘中用不到的数据可以选择不算，从而加快速度
            #bar.volume = tick.volume
            #bar.openInterest = tick.openInterest
            
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
        """收到5分钟K线"""
        # 撤销之前发出的尚未成交的委托（包括限价单和停止单）
        d=self.calculateTradeResult()
        if d.has_key('capital'):
            self.totalEquity=self.totalEquity+d['capital']
            print self.initCapital,self.totalEquity,d['capital']
        
        if self.totalEquity<=0:
            return
        
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

        self.fastMa = talib.SMA(self.closeArray, self.fastK)
        self.slowMa = talib.SMA(self.closeArray, self.slowK)
        self.longMa = talib.SMA(self.closeArray, self.longK)
        
        self.atr=tools.SATR(self.highArray, self.lowArray, self.closeArray, self.atrLength)
        
        _lots=math.floor(self.totalEquity*self.riskPercent/(self.ctaEngine.size*self.stopAtrs*self.atr[-1]))
        if _lots==0:
            _lots=1        
        
        
        # 判断买卖
        crossOver = self.fastMa[-1]>self.slowMa[-1] and self.fastMa[-2]<self.slowMa[-2]      # 金叉上穿
        crossBelow = self.fastMa[-1]<self.slowMa[-1] and self.fastMa[-2]>self.slowMa[-2] # 死叉下穿
        
        # 金叉和死叉的条件是互斥
        # 所有的委托均以K线收盘价委托（这里有一个实盘中无法成交的风险，考虑添加对模拟市价单类型的支持）
        if crossOver:
            # 如果金叉时手头没有持仓，则直接做多
            if bar.close>self.longMa[-1]:
                # 如果有空头持仓，则先平空，再做多
                if self.pos < 0:
                    self.cover(bar.close, abs(self.pos))
                    print bar.datetime , "\t cover:fastMa[-1]:" , self.fastMa[-1]," fastMa[-2]:",self.fastMa[-2]," slowMa[-1]:",self.slowMa[-1]," slowMa[-2]:",self.slowMa[-2]," longMa[-1]:",self.longMa[-1]," close[-1]:",bar.close
                
                print bar.datetime , "\t long :fastMa[-1]:" , self.fastMa[-1]," fastMa[-2]:",self.fastMa[-2]," slowMa[-1]:",self.slowMa[-1]," slowMa[-2]:",self.slowMa[-2]," longMa[-1]:",self.longMa[-1]," close[-1]:",bar.close
                self.buy(bar.close, _lots)
                
            elif self.pos < 0:
                self.cover(bar.close, abs(self.pos))
                print bar.datetime , "\t cover:fastMa[-1]:" , self.fastMa[-1]," fastMa[-2]:",self.fastMa[-2]," slowMa[-1]:",self.slowMa[-1]," slowMa[-2]:",self.slowMa[-2]," longMa[-1]:",self.longMa[-1]," close[-1]:",bar.close


        # 死叉和金叉相反
        elif crossBelow:
            if bar.close<self.longMa[-1]:
                if self.pos > 0:
                    self.sell(bar.close, abs(self.pos))
                    print bar.datetime, "\t sell :fastMa[-1]:" , self.fastMa[-1]," fastMa[-2]:",self.fastMa[-2]," slowMa[-1]:",self.slowMa[-1]," slowMa[-2]:",self.slowMa[-2]," longMa[-1]:",self.longMa[-1]," close[-1]:",bar.close
                
                self.short(bar.close, _lots)
                print bar.datetime, "\t short:fastMa[-1]:" , self.fastMa[-1]," fastMa[-2]:",self.fastMa[-2]," slowMa[-1]:",self.slowMa[-1]," slowMa[-2]:",self.slowMa[-2]," longMa[-1]:",self.longMa[-1]," close[-1]:",bar.close
            
            elif self.pos > 0:
                self.sell(bar.close, abs(self.pos))
                print bar.datetime, "\t sell :fastMa[-1]:" , self.fastMa[-1]," fastMa[-2]:",self.fastMa[-2]," slowMa[-1]:",self.slowMa[-1]," slowMa[-2]:",self.slowMa[-2]," longMa[-1]:",self.longMa[-1]," close[-1]:",bar.close

                
        # 发出状态更新事件
        self.putEvent()
        
        
    #----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        # 对于无需做细粒度委托控制的策略，可以忽略onOrder
        pass
    
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
    
########################################################################################
class OrderManagementDemoStrategy(CtaTemplate):
    """基于tick级别细粒度撤单追单测试demo"""
    
    className = 'OrderManagementDemoStrategy'
    author = u'用Python的交易员'
    
    # 策略参数
    initDays = 10   # 初始化数据所用的天数
    
    # 策略变量
    bar = None
    barMinute = EMPTY_STRING
    
    
    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol']
    
    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos']

    #----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(OrderManagementDemoStrategy, self).__init__(ctaEngine, setting)
                
        self.lastOrder = None
        self.orderType = ''
        
    #----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'双EMA演示策略初始化')
        
        initData = self.loadBar(self.initDays)
        for bar in initData:
            self.onBar(bar)
        
        self.putEvent()
        
    #----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeCtaLog(u'双EMA演示策略启动')
        self.putEvent()
    
    #----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.writeCtaLog(u'双EMA演示策略停止')
        self.putEvent()
        
    #----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""

        # 建立不成交买单测试单        
        if self.lastOrder == None:
            self.buy(tick.lastprice - 10.0, 1)

        # CTA委托类型映射
        if self.lastOrder != None and self.lastOrder.direction == u'多' and self.lastOrder.offset == u'开仓':
            self.orderType = u'买开'

        elif self.lastOrder != None and self.lastOrder.direction == u'多' and self.lastOrder.offset == u'平仓':
            self.orderType = u'买平'

        elif self.lastOrder != None and self.lastOrder.direction == u'空' and self.lastOrder.offset == u'开仓':
            self.orderType = u'卖开'

        elif self.lastOrder != None and self.lastOrder.direction == u'空' and self.lastOrder.offset == u'平仓':
            self.orderType = u'卖平'
                
        # 不成交，即撤单，并追单
        if self.lastOrder != None and self.lastOrder.status == u'未成交':

            self.cancelOrder(self.lastOrder.vtOrderID)
            self.lastOrder = None
        elif self.lastOrder != None and self.lastOrder.status == u'已撤销':
        # 追单并设置为不能成交
            
            self.sendOrder(self.orderType, self.tick.lastprice - 10, 1)
            self.lastOrder = None
            
    #----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        pass
    
    #----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        # 对于无需做细粒度委托控制的策略，可以忽略onOrder
        self.lastOrder = order
    
    #----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交推送（必须由用户继承实现）"""
        # 对于无需做细粒度委托控制的策略，可以忽略onOrder
        
        pass
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
    engine.setStartDate('20170101')
    
    # 设置产品相关参数
    engine.setSlippage(1)     # 股指1跳
    engine.setRate(0.3/10000)   # 万0.3
    engine.setSize(10)         # 股指合约大小 
    #engine.setPriceTick(1)    # 股指最小价格变动       
    
    # 设置使用的历史数据库
    engine.setDatabase(MINUTE_DB_NAME, 'rb000')
    
    # 在引擎中创建策略对象
    d = {}
    engine.initStrategy(ThreeEmaStrategy, d)
    
    # 开始跑回测
    engine.runBacktesting()
    
    # 显示回测结果
    engine.showBacktestingResult()