# encoding: UTF-8

'''
本文件包含了CTA引擎中的策略开发用模板，开发策略时需要继承CtaTemplate类。
'''

from ctaBase import *
from vtConstant import *
import json

from PyQt4 import QtCore, QtGui
import pyqtgraph as pg
from eventEngine import *

from vtFunction import *
########################################################################
class CtaTemplateAll(object):
    """CTA策略模板"""
    
    # 策略类的名称和作者
    className = 'CtaTemplate'
    author = EMPTY_UNICODE
    
    # MongoDB数据库的名称，K线数据库默认为1分钟
    tickDbName = TICK_DB_NAME
    barDbName = MINUTE_DB_NAME
    
    # 策略的基本参数
    name = EMPTY_UNICODE           # 策略实例名称
    vtSymbol = EMPTY_STRING        # 交易的合约vt系统代码
    tradeparam={}
    productClass = EMPTY_STRING    # 产品类型（只有IB接口需要）
    currency = EMPTY_STRING        # 货币（只有IB接口需要）

    tick = {}                       # tick 列表
    bar = {}                        # bar 列表
    barlist = {}
    mode = EMPTY_STRING             #回测类型，tick，5m，15m等
    barperiod=['5m','15m']         # 本次回测的bar周期
    # 策略的基本变量，由引擎管理
    inited = False                 # 是否进行了初始化
    trading = False                # 是否启动交易，由引擎管理
    pos = {}                        # 持仓情况
    tradestate={}                   # 交易状态

    longTrade={}                    # 开多交易列表
    shortTrade={}                   # 开空交易列表
    tradeResultDict={}              # 交易计算结果字典

    rate=0.00005
    slippage={}                 #滑点
    size={}                     #点值
    tickunit={}

    #新参数配置

    var_dict={}     #以合约为key的变量列表
    period = []     #k线周期列表
    codes = []      #证券代码列表
    codes=['rb']
    period=[5,15]
    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol']
    
    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos']

    #是否回测
    isBacktest=False

    #----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        self.ctaEngine = ctaEngine

        self.eventEngine=EventEngine()
        self.eventEngine.start()

        # 然后基于每笔交易的结果，我们可以计算具体的盈亏曲线和最大回撤等
        capital = 0             # 资金
        maxCapital = 0          # 资金最高净值
        drawdown = 0            # 回撤

        totalResult = 0         # 总成交数量
        totalTurnover = 0       # 总成交金额（合约面值）
        totalCommission = 0     # 总手续费
        totalSlippage = 0       # 总滑点

        timeList = []           # 时间序列
        pnlList = []            # 每笔盈亏序列
        capitalList = []        # 盈亏汇总的时间序列
        drawdownList = []       # 回撤的时间序列

        winningResult = 0       # 盈利次数
        losingResult = 0        # 亏损次数
        totalWinning = 0        # 总盈利金额
        totalLosing = 0         # 总亏损金额

        winningRate = 0.0         # 胜率
        averageWinning = 0                                  # 这里把数据都初始化为0
        averageLosing = 0
        profitLossRatio = 0

        self.tradeResultDict['capital'] = capital
        self.tradeResultDict['maxCapital'] = maxCapital
        self.tradeResultDict['drawdown'] = drawdown
        self.tradeResultDict['totalResult'] = totalResult
        self.tradeResultDict['totalTurnover'] = totalTurnover
        self.tradeResultDict['totalCommission'] = totalCommission
        self.tradeResultDict['totalSlippage'] = totalSlippage
        self.tradeResultDict['timeList'] = timeList
        self.tradeResultDict['pnlList'] = pnlList
        self.tradeResultDict['capitalList'] = capitalList
        self.tradeResultDict['drawdownList'] = drawdownList

        self.tradeResultDict['winningResult'] = winningResult
        self.tradeResultDict['losingResult'] = losingResult
        self.tradeResultDict['totalWinning'] = totalWinning
        self.tradeResultDict['totalLosing'] = totalLosing

        self.tradeResultDict['winningRate'] = winningRate
        self.tradeResultDict['averageWinning'] = averageWinning
        self.tradeResultDict['averageLosing'] = averageLosing
        self.tradeResultDict['profitLossRatio'] = profitLossRatio

        self.ContractTick=getContractSize()
        # 设置策略的参数
        if setting:
            self.barperiod=setting['period']
            self.name = setting['name']
            for symbol in setting['vtSymbol'].split(','):
                n=getSimpleName(symbol).upper()
                self.slippage[symbol]=self.ContractTick[n][1]*1
                self.size[symbol]=self.ContractTick[n][0]
                self.tickunit[symbol]=self.ContractTick[n][1]
        print 'slippage',self.slippage
        print 'size',self.size
    #----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        raise NotImplementedError
    
    #----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        raise NotImplementedError
    
    #----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        raise NotImplementedError

    #----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        raise NotImplementedError

    #----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        raise NotImplementedError
    
    #----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交推送（必须由用户继承实现）"""
        symbol=trade.vtSymbol
        if self.tradestate[symbol] != 0 :
            print 'ontrade',trade.tradeTime,trade.vtSymbol,trade.direction,trade.offset,trade.price
            self.tradestate[symbol] = 0
            if trade.direction==DIRECTION_LONG :
                self.pos[symbol] += trade.volume
            else :
                self.pos[symbol] -= trade.volume
            d=self.calculateTradeResult(trade)
            print d['capital'],d['drawdown'],len(d['timeList'])
            self.saveTrade(trade)
            event = Event(type_=EVENT_PLOT_BTS)
            data=d
            event.dict_['data'] =data
            self.eventEngine.put(event)
    #----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        raise NotImplementedError

    
    #----------------------------------------------------------------------
    def buy(self, price, volume,vtsymbol, stop=False):
        """买开"""
        self.tradestate[vtsymbol]=1
        return self.sendOrder(CTAORDER_BUY, price, volume,vtsymbol, stop)
    
    #----------------------------------------------------------------------
    def sell(self, price, volume, vtsymbol,stop=False):
        """卖平"""
        self.tradestate[vtsymbol]=1
        return self.sendOrder(CTAORDER_SELL, price, volume, vtsymbol,stop)

    #----------------------------------------------------------------------
    def short(self, price, volume,vtsymbol, stop=False):
        """卖开"""
        self.tradestate[vtsymbol]=1
        return self.sendOrder(CTAORDER_SHORT, price, volume, vtsymbol,stop)
 
    #----------------------------------------------------------------------
    def cover(self, price, volume, vtsymbol,stop=False):
        """买平"""
        self.tradestate[vtsymbol]=1
        return self.sendOrder(CTAORDER_COVER, price, volume,vtsymbol, stop)
        
    #----------------------------------------------------------------------
    def sendOrder(self, orderType, price, volume, vtSymbol,stop=False):
        """发送委托"""
        #print 'send order ',vtSymbol, orderType, price, volume
        if self.trading:
            self.tradeparam['price']=price
            self.tradeparam['volume']=volume
            self.tradeparam['vtsymbol']=vtSymbol
            self.tradeparam['orderType']=orderType
            # 如果stop为True，则意味着发本地停止单
            if stop:
                vtOrderID = self.ctaEngine.sendStopOrder(vtSymbol, orderType, price, volume, self)
            else:
                vtOrderID = self.ctaEngine.sendOrder(vtSymbol, orderType, price, volume, self)
            return vtOrderID
        else:
            return None        
        
    #----------------------------------------------------------------------
    def cancelOrder(self, vtOrderID):
        """撤单"""
        if STOPORDERPREFIX in vtOrderID:
            self.ctaEngine.cancelStopOrder(vtOrderID)
        else:
            self.ctaEngine.cancelOrder(vtOrderID)
        print 'cancel order '
    
    #----------------------------------------------------------------------
    def insertTick(self,vtsymbol, tick):
        """向数据库中插入tick数据"""
        self.ctaEngine.insertData(self.tickDbName, vtsymbol, tick)
    
    #----------------------------------------------------------------------
    def insertBar(self, vtsymbol,bar):
        """向数据库中插入bar数据"""
        self.ctaEngine.insertData(self.barDbName, vtsymbol, bar)
        
    #----------------------------------------------------------------------
    def loadTick(self, days,vtsymbol):
        """读取tick数据"""
        return self.ctaEngine.loadTick(self.tickDbName, vtsymbol, days)
    
    #----------------------------------------------------------------------
    def loadBar(self, days,vtsymbol):
        """读取bar数据"""
        return self.ctaEngine.loadBar(self.barDbName, vtsymbol, days)

    #----------------------------------------------------------------------
    def ticktoBar(self, tick,barperiod):
        """tick转成bar"""
        rflag=False
        rbar=None
        symbol=tick.symbol
        if symbol in self.bar.keys():
            if barperiod in self.bar[symbol].keys():
                bar=self.bar[symbol][barperiod]
            else :
                bar=self.bar[symbol][barperiod]=self.newBar(tick)
        else :
            self.bar[symbol]={}
            bar=self.bar[symbol][barperiod]=self.newBar(tick)

        tickMinute = tick.datetime.minute   #by hw
        barMinute = bar.datetime.minute
        p=barperiod[-1]
        pt=int(barperiod[0:-1])
        #if bar.datetime.minute >= pt:
        if (tickMinute%pt==0 and  abs(tickMinute-barMinute) >=pt   )   :

            self.onBar(bar)
            '''l=self.getVars(symbol,barperiod,'bar')
            l.append(bar)
            self.setVars(symbol,barperiod,'bar',l)
            l=self.getVars(symbol,barperiod,'close')
            l.append(bar.close)
            self.setVars(symbol,barperiod,'close',l)
            l=self.getVars(symbol,barperiod,'open')
            l.append(bar.open)
            self.setVars(symbol,barperiod,'open',l)
            l=self.getVars(symbol,barperiod,'high')
            l.append(bar.high)
            self.setVars(symbol,barperiod,'high',l)
            l=self.getVars(symbol,barperiod,'low')
            l.append(bar.low)
            self.setVars(symbol,barperiod,'low',l)
            '''
            bar=self.newBar(tick)
            rflag=True
            # 实盘中用不到的数据可以选择不算，从而加快速度
            #bar.volume = tick.volume
            #bar.openInterest = tick.openInterest

            self.bar[symbol][barperiod]=bar                  # 这种写法为了减少一层访问，加快速度 by hw

        else:                               # 否则继续累加新的K线
            bar = self.bar[symbol][barperiod]                 # 写法同样为了加快速度
            bar.high = max(bar.high, tick.lastPrice)
            bar.low = min(bar.low, tick.lastPrice)
            bar.close = tick.lastPrice

            self.bar[symbol][barperiod]=bar
        return rflag
    #----------------------------------------------------------------------
    def bartoBar(self,inbar,barperiod):
        """tick转成bar"""
        rflag=False
        rbar=None
        symbol=inbar.symbol
        if symbol in self.bar.keys():
            if barperiod in self.bar[symbol].keys():
                bar=self.bar[symbol][barperiod]
            else :
                bar=self.bar[symbol][barperiod]=inbar
        else :
            self.bar[symbol]={}
            bar=self.bar[symbol][barperiod]=inbar

        inbarMinute = inbar.datetime.minute   #by hw
        barMinute = bar.datetime.minute
        p=barperiod[-1]
        pt=int(barperiod[0:-1])
        #if pt==30:   #调试用途
        #    print 'into make bar' ,pt,inbarMinute,barMinute
        #print 'p',p,'pt',pt
        #if bar.datetime.minute >= pt:
        #if ((inbarMinute%pt==0 and  abs(inbarMinute-barMinute) >=pt   ) )    :
        if (inbarMinute%pt==0  )    :
            #print 'into make bar' ,pt
            l=self.getVars(symbol,barperiod,'bar')
            l.append(bar)
            self.setVars(symbol,barperiod,'bar',l)
            l=self.getVars(symbol,barperiod,'close')
            l.append(bar.close)
            self.setVars(symbol,barperiod,'close',l)
            l=self.getVars(symbol,barperiod,'open')
            l.append(bar.open)
            self.setVars(symbol,barperiod,'open',l)
            l=self.getVars(symbol,barperiod,'high')
            l.append(bar.high)
            self.setVars(symbol,barperiod,'high',l)
            l=self.getVars(symbol,barperiod,'low')
            l.append(bar.low)
            self.setVars(symbol,barperiod,'low',l)
            bar=inbar
            rflag=True
            # 实盘中用不到的数据可以选择不算，从而加快速度
            #bar.volume = tick.volume
            #bar.openInterest = tick.openInterest

            self.bar[symbol][barperiod]=bar                  # 这种写法为了减少一层访问，加快速度 by hw

        else:                               # 否则继续累加新的K线
            bar = self.bar[symbol][barperiod]                 # 写法同样为了加快速度
            bar.high = max(bar.high, inbar.high)
            bar.low = min(bar.low, inbar.low)
            bar.close = inbar.close

            self.bar[symbol][barperiod]=bar
        return rflag
    #----------------------------------------------------------------------
    def newBar(self,tick):
        bar=CtaBarData()
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
        return bar


    #----------------------------------------------------------------------
    def writeCtaLog(self, content):
        """记录CTA日志"""
        content = self.name + ':' + content
        self.ctaEngine.writeCtaLog(content)
        
    #----------------------------------------------------------------------
    def putEvent(self):
        """发出策略状态变化事件"""
        self.ctaEngine.putStrategyEvent(self.name)

    #----------------------------------------------------------------------
    def onTradeTime(self, ticktime):
        #if  ticktime.hour ==10 or ticktime.hour ==13  or ticktime.hour ==21 or ticktime.hour ==22 \
        raise NotImplementedError

    #----------------------------------------------------------------------
    def saveTrade(self,trade):
        reload(sys)
        sys.setdefaultencoding( "utf-8" )
        fname= self.ctaEngine.fpath + self.name + '_trade.txt'
        f= open(fname,'a')
        #with open(fname, 'w') as f:
        s=[]
        s.append(str(trade.dt))
        s.append(str(trade.tradeTime))
        s.append(trade.vtSymbol)
        s.append(trade.direction)
        s.append(trade.offset)
        s.append(str(trade.price))
        l=' '.join(s)
        f.write(l)
        f.write('\n')
        f.close()

        #发送邮件
        title=self.name+'策略交易信息'
        msg='本次交易内容：'+l
        #self.ctaEngine.sendmail(title,msg)

    #----------------------------------------------------------------------
    def setVars(self,code,period,key,value):
        #设置全局变量
        self.var_dict[code][period][key] = value

    #----------------------------------------------------------------------
    def getVars(self,code,period,key):
        #读取全局变量
        if code not in self.var_dict.keys():
            self.var_dict[code]={}
        if period not in  self.var_dict[code].keys():
            self.var_dict[code][period]={}
        if key not in  self.var_dict[code][period].keys():
            self.var_dict[code][period][key]=[]
        value = self.var_dict[code][period][key]
        return value

    #----------------------------------------------------------------------
    def readVars(self):
        print 'name',self.name,self.className
        fname= self.ctaEngine.fpath + self.name + '.json'
        #f=open()
        try :
            with open(fname) as f:
                l = json.load(f)
                for setting in l:
                    for varname in setting.keys():
                        self.__setattr__(varname,setting[varname])
            f.close()
        except :
            f= open(fname,'w')
            f.close()
    #----------------------------------------------------------------------
    def saveVars(self):
        fname= self.ctaEngine.fpath + self.name + '.json'
        with open(fname, 'w') as f:
            l = []
            setting = {}
            for param in self.varList:
                setting[param] = self.__getattribute__(param)
            l.append(setting)

            jsonL = json.dumps(l, indent=4)
            f.write(jsonL)
    #----------------------------------------------------------------------
    def setIsBacktest(self,flag):
    #设置当前是否为回测
        self.isBacktest=flag


    #----------------------------------------------------------------------
    def calculateTradeResult(self,trade):
        """
        计算回测结果
        """
        #self.output(u'计算回测结果')

        # 首先基于回测后的成交记录，计算每笔交易的盈亏
        resultList = []             # 交易结果列表

        #longTrade = []              # 未平仓的多头交易
        #shortTrade = []             # 未平仓的空头交易
        symbol=trade.vtSymbol
        if symbol not in self.shortTrade.keys():
            self.shortTrade[symbol]=[]
        if symbol not in self.longTrade.keys():
            self.longTrade[symbol]=[]

        # 多头交易
        if trade.direction == DIRECTION_LONG:
            # 如果尚无空头交易
            if not self.shortTrade[symbol]:
                self.longTrade[symbol].append(trade)
            # 当前多头交易为平空
            else:
                while True:
                    entryTrade = self.shortTrade[symbol][0]
                    exitTrade = trade

                    # 清算开平仓交易
                    closedVolume = min(exitTrade.volume, entryTrade.volume)
                    result = TradingResult(entryTrade.price, entryTrade.dt,
                                           exitTrade.price, exitTrade.dt,
                                           -closedVolume, self.rate, self.slippage[symbol], self.size[symbol])
                    resultList.append(result)

                    # 计算未清算部分
                    entryTrade.volume -= closedVolume
                    exitTrade.volume -= closedVolume

                    # 如果开仓交易已经全部清算，则从列表中移除
                    if not entryTrade.volume:
                        self.shortTrade[symbol].pop(0)

                    # 如果平仓交易已经全部清算，则退出循环
                    if not exitTrade.volume:
                        break

                    # 如果平仓交易未全部清算，
                    if exitTrade.volume:
                        # 且开仓交易已经全部清算完，则平仓交易剩余的部分
                        # 等于新的反向开仓交易，添加到队列中
                        if not self.shortTrade[symbol]:
                            self.longTrade[symbol].append(exitTrade)
                            break
                        # 如果开仓交易还有剩余，则进入下一轮循环
                        else:
                            pass

        # 空头交易
        else:
            # 如果尚无多头交易
            if not self.longTrade[symbol]:
                self.shortTrade[symbol].append(trade)
            # 当前空头交易为平多
            else:
                while True:
                    entryTrade = self.longTrade[symbol][0]
                    exitTrade = trade

                    # 清算开平仓交易
                    closedVolume = min(exitTrade.volume, entryTrade.volume)
                    result = TradingResult(entryTrade.price, entryTrade.dt,
                                           exitTrade.price, exitTrade.dt,
                                           closedVolume, self.rate, self.slippage[symbol], self.size[symbol])
                    resultList.append(result)

                    # 计算未清算部分
                    entryTrade.volume -= closedVolume
                    exitTrade.volume -= closedVolume

                    # 如果开仓交易已经全部清算，则从列表中移除
                    if not entryTrade.volume:
                        self.longTrade[symbol].pop(0)

                    # 如果平仓交易已经全部清算，则退出循环
                    if not exitTrade.volume:
                        break

                    # 如果平仓交易未全部清算，
                    if exitTrade.volume:
                        # 且开仓交易已经全部清算完，则平仓交易剩余的部分
                        # 等于新的反向开仓交易，添加到队列中
                        if not self.longTrade[symbol]:
                            self.shortTrade[symbol].append(exitTrade)
                            break
                        # 如果开仓交易还有剩余，则进入下一轮循环
                        else:
                            pass

        d=self.tradeResultDict
        # 检查是否有交易
        if not resultList:
            self.writeCtaLog(u'无交易结果')
            return d

        # 然后基于每笔交易的结果，我们可以计算具体的盈亏曲线和最大回撤等
        capital = d['capital']             # 资金
        maxCapital = d['maxCapital']          # 资金最高净值
        drawdown = d['drawdown']            # 回撤

        totalResult = d['totalResult']         # 总成交数量
        totalTurnover = d['totalTurnover']       # 总成交金额（合约面值）
        totalCommission = d['totalCommission']     # 总手续费
        totalSlippage = d['totalSlippage']       # 总滑点

        timeList = d['timeList']           # 时间序列
        pnlList = d['pnlList']            # 每笔盈亏序列
        capitalList = d['capitalList']       # 盈亏汇总的时间序列
        drawdownList = d['drawdownList']      # 回撤的时间序列

        winningResult = d['winningResult']       # 盈利次数
        losingResult = d['losingResult']        # 亏损次数
        totalWinning = d['totalWinning']        # 总盈利金额
        totalLosing = d['totalLosing']         # 总亏损金额

        for result in resultList:
            capital += result.pnl
            maxCapital = max(capital, maxCapital)
            drawdown = capital - maxCapital

            pnlList.append(result.pnl)
            timeList.append(result.exitDt)      # 交易的时间戳使用平仓时间
            capitalList.append(capital)
            drawdownList.append(drawdown)

            totalResult += 1
            totalTurnover += result.turnover
            totalCommission += result.commission
            totalSlippage += result.slippage

            if result.pnl >= 0:
                winningResult += 1
                totalWinning += result.pnl
            else:
                losingResult += 1
                totalLosing += result.pnl

        # 计算盈亏相关数据
        winningRate = winningResult/totalResult*100         # 胜率

        averageWinning = d['averageWinning']
        averageLosing = d['averageLosing']
        profitLossRatio = d['profitLossRatio']

        if winningResult:
            averageWinning = totalWinning/winningResult     # 平均每笔盈利
        if losingResult:
            averageLosing = totalLosing/losingResult        # 平均每笔亏损
        if averageLosing:
            profitLossRatio = -averageWinning/averageLosing # 盈亏比

        # 返回回测结果

        d['capital'] = capital
        d['maxCapital'] = maxCapital
        d['drawdown'] = drawdown
        d['totalResult'] = totalResult
        d['totalTurnover'] = totalTurnover
        d['totalCommission'] = totalCommission
        d['totalSlippage'] = totalSlippage
        d['timeList'] = timeList
        d['pnlList'] = pnlList
        d['capitalList'] = capitalList
        d['drawdownList'] = drawdownList
        d['winningRate'] = winningRate
        d['averageWinning'] = averageWinning
        d['averageLosing'] = averageLosing
        d['profitLossRatio'] = profitLossRatio

        return d


########################################################################
class DataRecorder(CtaTemplateAll):
    """
    纯粹用来记录历史数据的工具（基于CTA策略），
    建议运行在实际交易程序外的一个vn.trader实例中，
    本工具会记录Tick和1分钟K线数据。
    """
    className = 'DataRecorder'
    author = u'用Python的交易员'
    
    # 策略的基本参数
    name = EMPTY_UNICODE            # 策略实例名称
    vtSymbol = EMPTY_STRING         # 交易的合约vt系统代码    
    
    # 策略的变量
    bar = None                      # K线数据对象
    barMinute = EMPTY_STRING        # 当前的分钟，初始化设为-1  
    
    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'barMinute']    

    #----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(DataRecorder, self).__init__(ctaEngine, setting)

    #----------------------------------------------------------------------
    def onInit(self):
        """初始化"""
        self.writeCtaLog(u'数据记录工具初始化')
        
    #----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeCtaLog(u'数据记录工具启动')
        self.putEvent()
    
    #----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.writeCtaLog(u'数据记录工具停止')
        self.putEvent()
        
    #----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送"""
        # 收到Tick后，首先插入到数据库里
        self.insertTick(tick)
        
        # 计算K线
        tickMinute = tick.datetime.minute
        
        if tickMinute != self.barMinute:    # 如果分钟变了，则把旧的K线插入数据库，并生成新的K线
            if self.bar:
                self.onBar(self.bar)
            
            bar = CtaBarData()              # 创建新的K线，目的在于防止之前K线对象在插入Mongo中被再次修改，导致出错
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
            
            bar.volume = tick.volume
            bar.openInterest = tick.openInterest
            
            self.bar = bar                  # 这种写法为了减少一层访问，加快速度
            self.barMinute = tickMinute     # 更新当前的分钟
            
        else:                               # 否则继续累加新的K线
            bar = self.bar                  # 写法同样为了加快速度
            
            bar.high = max(bar.high, tick.lastPrice)
            bar.low = min(bar.low, tick.lastPrice)
            bar.close = tick.lastPrice
            
            bar.volume = bar.volume + tick.volume   # 成交量是累加的
            bar.openInterest = tick.openInterest    # 持仓量直接更新
        
    #----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送"""
        pass
    
    #----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交推送"""
        pass
    
    #----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送"""
        self.insertBar(bar)   

    
    
########################################################################
class TradingResult(object):
    """每笔交易的结果"""

    #----------------------------------------------------------------------
    def __init__(self, entryPrice, entryDt, exitPrice,
                 exitDt, volume, rate, slippage, size):
        """Constructor"""
        self.entryPrice = entryPrice    # 开仓价格
        self.exitPrice = exitPrice      # 平仓价格

        self.entryDt = entryDt          # 开仓时间datetime
        self.exitDt = exitDt            # 平仓时间

        self.volume = volume    # 交易数量（+/-代表方向）

        self.turnover = (self.entryPrice+self.exitPrice)*size*abs(volume)   # 成交金额
        self.commission = self.turnover*rate                                # 手续费成本
        self.slippage = slippage*2*size*abs(volume)                         # 滑点成本
        self.pnl = ((self.exitPrice - self.entryPrice) * volume * size
                    - self.commission - self.slippage)                      # 净盈亏



########################################################################
class resultWidget(QtGui.QWidget):
    signal = QtCore.pyqtSignal(type(Event()))
    signal1 = QtCore.pyqtSignal(type(Event()))
    signal2 = QtCore.pyqtSignal(type(Event()))

    listBar=[]
    barnum=0
    pwDict={}
    curveDict={}        #曲线与data的对照关系
    cuvData={}          #曲线数据
    setting={}
    class CandlestickItem(pg.GraphicsObject):
        def __init__(self, data):
            pg.GraphicsObject.__init__(self)
            self.data = data  ## data must have fields: time, open, close, min, max
            self.generatePicture()

        def generatePicture(self):
            ## pre-computing a QPicture object allows paint() to run much more quickly,
            ## rather than re-drawing the shapes every time.
            self.picture = QtGui.QPicture()
            p = QtGui.QPainter(self.picture)
            p.setPen(pg.mkPen(color='w', width=0.4))  # 0.4 means w*2
            # w = (self.data[1][0] - self.data[0][0]) / 3.
            w = 0.1
            for (t, open, close, min, max) in self.data:
                p.drawLine(QtCore.QPointF(t, min), QtCore.QPointF(t, max))
                if open > close:
                    p.setBrush(pg.mkBrush('g'))
                else:
                    p.setBrush(pg.mkBrush('r'))
                p.drawRect(QtCore.QRectF(t-w, open, w*2, close-open))
            p.end()

        def paint(self, p, *args):
            p.drawPicture(0, 0, self.picture)

        def boundingRect(self):
            ## boundingRect _must_ indicate the entire area that will be drawn on
            ## or else we will get artifacts and possibly crashing.
            ## (in this case, QPicture does all the work of computing the bouning rect for us)
            return QtCore.QRectF(self.picture.boundingRect())


    def __init__(self,eventEngine,setting,parent=None):
        super(resultWidget, self).__init__(parent)
        self.eventEngine=eventEngine
        self.initUi(setting)
        self.registerEvent()

    #----------------------------------------------------------------------
    def initUi(self, setting):
        """初始化界面"""
        self.setWindowTitle(u'view')
        self.setting=setting
        #self.vbl_1 = QtGui.QVBoxLayout()
        #self.initplotTick()  # plotTick初始化
        self.vbl_2 = QtGui.QVBoxLayout()    #竖排 放行情
        self.vbl_3 = QtGui.QVBoxLayout()    #竖排 放指标
        self.vbl_4 = QtGui.QVBoxLayout()    #竖排 放回测绩效
        self.hbl = QtGui.QHBoxLayout()    #横排
        #kl=''
        #dl=''
        #bt=''
        screenL=0
        screenW=768
        if 'kline' in setting.keys():
            kl=setting['kline']
            self.initplotKline(self.vbl_3)  # plotKline初始化
            self.hbl.addLayout(self.vbl_2)
            screenL+=400
        if 'dline' in setting.keys():
            dl=setting['dline']
            #l=['abc','bbb']
            for l in dl :
                na=l[0]
                self.initplotVline(self.vbl_3,na,l)
            self.hbl.addLayout(self.vbl_3)
            screenL+=400
        if 'backtest' in setting.keys():
            bt=setting['backtest']
            self.initbacktestResult(self.vbl_4)
            self.hbl.addLayout(self.vbl_4)
            screenL+=400

        #for k,v in self.pwDict.items():

        #self.initplotTendency()  # plot分时图的初始化

        # 整体布局
        if screenL >0:
            #self.hbl.addLayout(self.vbl_1)
           # self.hb1.  .resize(800,600)
            self.setLayout(self.hbl)
        else :
            screenW=0

        self.resize(screenL,screenW)

    #----------------------------------------------------------------------
    def initplotKline(self,box):
        """Kline"""
        pg.BarGraphItem()
        self.pw2 = pg.PlotWidget(name='Kline', pen='r')  # K线图
        self.pw2.addLegend()
        box.addWidget(self.pw2)
        self.pw2.setDownsampling(mode='peak')
        self.pw2.setClipToView(True)
        self.pw2.setLabel('bottom','5m')
        self.pw2.showGrid(x=True, y=True)

        #self.curve5 = self.pw2.plot()
        #self.curve6 = self.pw2.plot()

        self.candle = self.CandlestickItem(self.listBar)
        self.pw2.addItem(self.candle)
        self.pwDict['pw2']=self.pw2
        ## Draw an arrowhead next to the text box
        # self.arrow = pg.ArrowItem()
        # self.pw2.addItem(self.arrow)

    #----------------------------------------------------------------------
    def initplotVline(self,box,name,d):

        if name in self.pwDict.keys():
            pw=self.pwDict[name]
        else :
            pw=pg.PlotWidget(name=name)
            pw.addLegend()
            for i in range(len(d)) :
                c=d[i]
                self.curveDict[c]=[pw.plot(name=c,pen=i*10),pw]
                #c=pw.plot([1,3,2,4], pen=40,    name='red plot')
                #b=pg.BarGraphItem(x=[1,2,3],width=0.5,height=[1,2,3],pen='b')  #画柱状图
                #pw.addItem(b)
            pw.setLabel('bottom',name)
            pw.showGrid(x=True, y=True)
            self.pwDict[name]=pw
            box.addWidget(pw)
            if 'pw2' in self.pwDict.keys():
                pw2=self.pwDict['pw2']
                pw.setXLink('Kline')


    #----------------------------------------------------------------------
    def initbacktestResult(self,box):
        pCapital=pg.PlotWidget(name='pCapital')
        box.addWidget(pCapital)
        pCapital.setLabel('bottom','Capital')
        pCapital.showGrid(x=True, y=True)
        self.pwDict['pCapital']=pCapital
        pDD=pg.PlotWidget(name='pDD')
        pDD.setLabel('bottom','pDD')
        pDD.showGrid(x=True, y=True)
        box.addWidget(pDD)
        self.pwDict['pDD']=pDD
        pnl=pg.PlotWidget(name='pnl')
        pnl.setLabel('bottom','pnl')
        pnl.showGrid(x=True, y=True)
        box.addWidget(pnl)
        self.pwDict['pnl']=pnl
        #pnl=pg.BarGraphItem()

    #----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.signal.connect(self.setBar)
        #self.eventEngine.register(EVENT_PLOT_BAR, self.signal.emit)
        self.signal1.connect(self.setCuv)
        self.signal2.connect(self.setBacktest)
        self.eventEngine.register(EVENT_PLOT_BAR, self.signal.emit)
        self.eventEngine.register(EVENT_PLOT_CUV, self.signal1.emit)
        self.eventEngine.register(EVENT_PLOT_BTS, self.signal2.emit)
        print 'finish regedit event'



    #----------------------------------------------------------------------
    def plotKline(self):
        """K线图"""
        #if self.initCompleted:
        # 均线
        #self.curve5.setData(self.listfastEMA, pen=(255, 0, 0), name="Red curve")
        #self.curve6.setData(self.listslowEMA, pen=(0, 255, 0), name="Green curve")

        # 画K线
        #self.pw2.removeItem(self.candle)
        #print self.listBar[-1]
        print self.listBar[-1]
        self.candle = self.CandlestickItem([self.listBar[-1]])
        self.pw2.addItem(self.candle)
        xr_max=max(50,len(self.listBar))
        xr_min=xr_max-50
        xr=(xr_min,xr_max)
        if len(self.listBar) > 0 :
            yr_max=self.listBar[-1][2]*1.05
            yr_min=self.listBar[-1][2]*0.95
        else :
            yr_max=100
            yr_min=0
        yr=(yr_min,yr_max)
        #re=self.pw2.rect()

        self.pw2.setRange(xRange=xr,yRange=yr)
        #self.plotText()   # 显示开仓信号位置


    #----------------------------------------------------------------------
    def plotVline(self,data):
        cuv=data['name']
        pp=self.curveDict[cuv][0]
        pw=self.curveDict[cuv][1]
        if cuv in self.cuvData.keys():
            l=self.cuvData[cuv]
        else :
            l=[]
            self.cuvData[cuv]=l
        l.append(data['data'])
        pp.setData(l)
        xr_max=max(50,len(l))
        xr_min=xr_max-50
        xr=(xr_min,xr_max)
        pw.setRange(xRange=xr)
    #----------------------------------------------------------------------
    def plotBacktestline(self,data):
        capitalList=data['capitalList']
        pnlList=data['pnlList']
        drawdownList=data['drawdownList']
        #print capitalList,pnlList,drawdownList
        pCapital=self.pwDict['pCapital']
        pCapital.plot().setData(capitalList)

        pDD=self.pwDict['pDD']
        pDD.plot(fillLevel=-0.3, brush=(50,50,200,100)).setData(drawdownList)

        #b=pg.BarGraphItem(x=[1,2,3],width=0.5,height=[1,2,3],pen='b')  #画柱状图
        #pw.addItem(b)
        pnl=self.pwDict['pnl']
        ba=pg.BarGraphItem(x=range(len(pnlList)),width=0.5,height=pnlList,pen='b')
        #pnl.removeItem(ba)
        pnl.addItem(ba)

        '''
        d['capital'] = capital
        d['maxCapital'] = maxCapital
        d['drawdown'] = drawdown
        d['totalResult'] = totalResult
        d['totalTurnover'] = totalTurnover
        d['totalCommission'] = totalCommission
        d['totalSlippage'] = totalSlippage
        d['timeList'] = timeList
        d['pnlList'] = pnlList
        d['capitalList'] = capitalList
        d['drawdownList'] = drawdownList
        d['winningRate'] = winningRate
        d['averageWinning'] = averageWinning
        d['averageLosing'] = averageLosing
        d['profitLossRatio'] = profitLossRatio
        '''
    #----------------------------------------------------------------------
    def setBar(self,event):
        bar = event.dict_['data']
        #print bar.__dict__
        print bar
        self.setBarbase(bar)

    #----------------------------------------------------------------------
    def setCuv(self,event):
        data = event.dict_['data']
        self.plotVline(data)
    #----------------------------------------------------------------------
    def setBacktest(self,event):
        data = event.dict_['data']
        self.plotBacktestline(data)

    #----------------------------------------------------------------------
    def setBarbase(self,bar):
        self.barnum=self.barnum +1
        #o=bar.open
        #h=bar.high
        #l=bar.low
        #c=bar.close
        o=bar[1]
        c=bar[2]
        l=bar[3]
        h=bar[4]
        t=(self.barnum)
        #print o,c,l,h
        self.listBar.append((t,o, c, l, h))
        self.plotKline()

if __name__ == '__main__':
    import sys
    from  eventEngine import *
    app=QtGui.QApplication(sys.argv)
    graphsetting={}
    graphsetting['kline']=['15m']
    graphsetting['dline']=[['macd_short','macd_long'],['pos'],['dif_short','dif_long']]
    graphsetting['backtest']=[]
    eventEngine=EventEngine()
    rw=resultWidget(eventEngine,graphsetting)
    rw.show()
    if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
        QtGui.QApplication.instance().exec_()