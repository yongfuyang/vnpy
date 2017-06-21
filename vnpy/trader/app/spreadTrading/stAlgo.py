# encoding: UTF-8

from math import floor

from vnpy.trader.vtConstant import (EMPTY_INT, EMPTY_FLOAT, 
                                    EMPTY_STRING, EMPTY_UNICODE,
                                    DIRECTION_LONG, DIRECTION_SHORT,
                                    STATUS_ALLTRADED, STATUS_CANCELLED, STATUS_REJECTED)



########################################################################
class StAlgoTemplate(object):
    """价差算法交易模板"""
    MODE_LONGSHORT = 'longshort'
    MODE_LONGONLY = 'long'
    MODE_SHORTONLY = 'short'

    #----------------------------------------------------------------------
    def __init__(self, algoEngine, spread):
        """Constructor"""
        self.algoEngine = algoEngine        # 算法引擎        
        self.spreadName = spread.name       # 价差名称
        self.spread = spread                # 价差对象      

        self.algoName = EMPTY_STRING        # 算法名称
        
        self.active = False                 # 工作状态
        self.mode = self.MODE_LONGSHORT     # 工作模式
        
        self.buyPrice = EMPTY_FLOAT         # 开平仓价格
        self.sellPrice = EMPTY_FLOAT
        self.shortPrice = EMPTY_FLOAT
        self.coverPrice = EMPTY_FLOAT
        
        self.maxPosSize = EMPTY_INT         # 最大单边持仓量
        self.maxOrderSize = EMPTY_INT       # 最大单笔委托量
    
    #----------------------------------------------------------------------
    def updateSpreadTick(self, spread):
        """"""
        raise NotImplementedError
    
    #----------------------------------------------------------------------
    def updateSpreadPos(self, spread):
        """"""
        raise NotImplementedError
    
    #----------------------------------------------------------------------
    def updateTrade(self, trade):
        """"""
        raise NotImplementedError
    
    #----------------------------------------------------------------------
    def updateOrder(self, order):
        """"""
        raise NotImplementedError
    
    #----------------------------------------------------------------------
    def updateTimer(self):
        """"""
        raise NotImplementedError
        
    #----------------------------------------------------------------------
    def start(self):
        """"""
        raise NotImplementedError
    
    #----------------------------------------------------------------------
    def stop(self):
        """"""
        raise NotImplementedError



########################################################################
class SniperAlgo(StAlgoTemplate):
    """狙击算法（市价委托）"""
    FINISHED_STATUS = [STATUS_ALLTRADED, STATUS_CANCELLED, STATUS_REJECTED]

    #----------------------------------------------------------------------
    def __init__(self, algoEngine):
        """Constructor"""
        super(SniperAlgo, self).__init__(algoEngine)
        
        self.algoName = u'Sniper'
        self.hedgeInterval = 2      # 对冲腿撤单再发前的等待时间
        self.hedgeCount = 0         # 对冲计数
        
        self.activeVtSymbol = spread.activeLeg.vtSymbol                         # 主动腿代码
        self.passiveVtSymbols = [leg.vtSymbol for leg in spread.passiveLegs]    # 被动腿代码列表
        
        # 缓存每条腿对象的字典
        self.legDict = {}
        self.legDict[spread.activeLeg.vtSymbol] = spread.activeLeg
        for leg in spread.passiveLegs:
            self.legDict[leg.vtSymbol] = leg
        
        self.hedgingTaskDict = {}           # 被动腿需要对冲的数量字典  vtSymbol:volume
        self.legOrderDict = {}              # vtSymbol: list of vtOrderID        
        
    #----------------------------------------------------------------------
    def updateSpreadTick(self, spread):
        """价差行情更新"""
        self.spread = spread
    
    #----------------------------------------------------------------------
    def updateSpreadPos(self, spread):
        """价差持仓更新"""
        self.spread = spread
    
    #----------------------------------------------------------------------
    def updateTrade(self, trade):
        """成交更新"""
        if not self.active:
            return
        
        if trade.vtSymbol == self.activeVtSymbol:
            self.newActiveLegTrade(trade)
        else:
            self.newPassiveLegTrade(trade)
    
    #----------------------------------------------------------------------
    def updateOrder(self, order):
        """委托更新"""
        if not self.active:
            return
        
        # 只处理完成委托
        if order.status in self.FINISHED_STATUS:
            vtOrderID = order.vtOrderID
            vtSymbol = order.vtSymbol
            
            # 从委托列表中移除该委托
            orderList = self.legOrderDict[vtSymbol]
            
            if vtOrderID in orderList:
                orderList.remove(vtOrderID)
                
            # 检查若是被动腿，且已经没有未完成委托，则执行对冲
            if not orderList and vtSymbol in self.passiveVtSymbols:
                self.hedgePassiveLeg(vtSymbol)
    
    #----------------------------------------------------------------------
    def updateTimer(self):
        """计时更新"""
        self.hedgeCount += 1
        
        # 计时到达对冲间隔后，则对尚未成交的全部被动腿委托全部撤单
        # 收到撤单回报后，会自动发送新的对冲委托
        if self.hedgeCount > self.hedgeInterval:
            self.cancelAllPassiveLegOrders()
            self.hedgeCount = 0
        
    #----------------------------------------------------------------------
    def start(self):
        """启动"""
        raise NotImplementedError
    
    #----------------------------------------------------------------------
    def stop(self):
        """停止"""
        self.active = False
        
        self.hedgingTaskDict.clear()
        self.cancelAllOrders()
    
    #----------------------------------------------------------------------
    def sendLegOrder(self, leg, legVolume):
        """发送每条腿的委托"""
        vtSymbol = leg.vtSymbol
        volume = abs(legVolume)
        payup = leg.payup
        
        # 发送委托
        if legVolume > 0:
            price = leg.askPrice
            
            if leg.shortPos > 0:
                orderList = self.algoEngine.cover(vtSymbol, price, volume, payup)
            else:
                orderList = self.algoEngine.buy(vtSymbol, price, volume, payup)
                
        elif legVolume < 0:
            price = leg.bidPrice
            
            if leg.longPos > 0:
                orderList = self.algoEngine.sell(vtSymbol, price, volume, payup)
            else:
                orderList = self.algoEngine.short(vtSymbol, price, volume, payup)
                
        # 保存到字典中
        if vtSymbol not in self.legOrderDict:
            self.legOrderDict[vtSymbol] = orderList
        else:
            self.legOrderDict[vtSymbol].extend(orderList)        
    
    #----------------------------------------------------------------------
    def quoteActiveLeg(self, direction):
        """发出主动腿"""        
        spread = self.spread
        
        if direction == DIRECTION_LONG:
            spreadVolume = min(spread.askVolume,
                               self.maxPosSize - spread.netPos,
                               self.maxOrderSize)
        else:
            spreadVolume = min(spread.bidVolume,
                               self.maxPosSize + spread.netPos,
                               self.maxOrderSize)
            
        if spreadVolume <= 0:
            return
        
        leg = self.legDict[self.activeVtSymbol]
        legVolume = spreadVolume * leg.ratio
        
        self.sendLegOrder(leg, legVolume)

    #----------------------------------------------------------------------
    def hedgePassiveLeg(self, vtSymbol):
        """被动腿对冲"""
        if vtSymbol not in self.hedgingTaskDict:
            return
        legVolume = self.hedgingTaskDict[vtSymbol]
        
        leg = self.legDict[vtSymbol]
        
        self.sendLegOrder(leg, legVolume)
        
    #----------------------------------------------------------------------
    def hedgeAllPassiveLegs(self):
        """执行所有被动腿对冲"""
        for vtSymbol in self.hedgingTaskDict.keys():
            self.hedgePassiveLeg(vtSymbol)
        
    #----------------------------------------------------------------------
    def newActiveLegTrade(self, trade):
        """新的主动腿成交"""
        spread = self.spread
        
        # 计算主动腿成交后，对应的价差仓位
        activeRatio = spread.activeLeg.ratio
        spreadVolume = round(trade.volume / activeRatio)  # 四舍五入求主动腿成交量对应的价差份数
        
        # 计算价差新仓位，对应的被动腿需要对冲部分
        for leg in self.spread.passiveLegs:
            newHedgingTask = leg.ratio * spreadVolume
            
            if leg.vtSymbol not in self.hedgingTaskDict:
                self.hedgingTaskDict[leg.vtSymbol] = newHedgingTask
            else:
                self.hedgingTaskDict[leg.vtSymbol] += newHedgingTask
    
    #----------------------------------------------------------------------
    def newPassiveLegTrade(self, trade):
        """新的被动腿成交"""
        if trade.vtSymbol in self.hedgingTaskDict:
            # 计算完成的对冲数量
            if trade.direction == DIRECTION_LONG:
                hedgedVolume = trade.volume
            else:
                hedgedVolume = -trade.volume
                
            # 计算剩余尚未完成的数量
            self.hedgingTaskDict[trade.vtSymbol] -= hedgedVolume
            
            # 如果已全部完成，则从字典中移除
            if not self.hedgingTaskDict[trade.vtSymbol]:
                del self.hedgingTaskDict[trade.vtSymbol]
                
    #----------------------------------------------------------------------
    def cancelLegOrder(self, vtSymbol):
        """撤销某条腿的委托"""
        if vtSymbol not in self.legOrderDict:
            return
        
        orderList = self.legOrderDict[vtSymbol]
        
        for vtOrderID in orderList:
            self.algoEngine.cancelOrder(vtOrderID)
            
    #----------------------------------------------------------------------
    def cancelAllOrders(self):
        """撤销全部委托"""
        for orderList in self.legOrderDict.values():
            for vtOrderID in orderList:
                self.algoEngine.cancelOrder(vtOrderID)
    
    #----------------------------------------------------------------------
    def cancelAllPassiveLegOrders(self):
        """撤销全部被动腿委托"""
        for vtSymbol in self.passiveVtSymbols:
            self.cancelLegOrder(vtSymbol)