# encoding: UTF-8

'''
本文件中包含了CTA模块中用到的一些基础设置、类和常量等。
'''

from __future__ import division


# 把vn.trader根目录添加到python环境变量中
import sys
sys.path.append('..')


# 常量定义
# CTA引擎中涉及到的交易方向类型
CTAORDER_BUY = u'买开'
CTAORDER_SELL = u'卖平'
CTAORDER_SELLTODAY = u'卖平今'
CTAORDER_SELLYESTERDAY = u'卖平昨'
CTAORDER_SHORT = u'卖开'
CTAORDER_COVER = u'买平'
CTAORDER_COVERTODAY = u'买今平'
CTAORDER_COVERYESTERDAY = u'买平昨'
DIRECTION_LONG = u'多'
DIRECTION_SHORT = u'空'

# 本地停止单状态
STOPORDER_WAITING = u'等待中'
STOPORDER_CANCELLED = u'已撤销'
STOPORDER_TRIGGERED = u'已触发'

# 本地停止单前缀
STOPORDERPREFIX = 'CtaStopOrder.'

# 数据库名称
SETTING_DB_NAME = 'VnTrader_Setting_Db'
POSITION_DB_NAME = 'VnTrader_Position_Db'

BARSIZE_DICT = {}
BARSIZE_DICT = {
    0 : 'tick',
    1 : '1 secs',
    2 : '5 secs',
    3 : '15 secs',
    4 : '30 secs',
    5 : '1 min',
    6 : '2 mins',
    7 : '3 min',
    8 : '5 mins',
    9 : '15 mins',
    10 : '30 mins',
    11 : '1 hour',
    12 : '1 day'
}


BARSIZE_TICK=0
BARSIZE_S1=1
BARSIZE_S5=2
BARSIZE_S15=3
BARSIZE_S30=4
BARSIZE_M1=5
BARSIZE_M2=6
BARSIZE_M3=7
BARSIZE_M5=8
BARSIZE_M15=9
BARSIZE_M30=10
BARSIZE_H1=11
BARSIZE_D=12

# 使用的缓存表
# 临时变量使用 barSize
BARSIZE_DFNAME_DICT = {}
BARSIZE_DFNAME_DICT = {
    BARSIZE_TICK : 'df_tick',
    BARSIZE_S1 : 'df_S_Bar',
    BARSIZE_S5 : 'df_S5_Bar',
    BARSIZE_S15 : 'df_S15_Bar',
    BARSIZE_S30 : 'df_S30_Bar',
    BARSIZE_M1 : 'df_M1_Bar',
    BARSIZE_M2 : 'df_M2_Bar',
    BARSIZE_M3 : 'df_M3_Bar',
    BARSIZE_M5 : 'df_M5_Bar',
    BARSIZE_M15 : 'df_M15_Bar',
    BARSIZE_M30 : 'df_M30_Bar',
    BARSIZE_H1 : 'df_H_Bar',
    BARSIZE_D : 'df_D_Bar'
}

# BARSIZE 跟本地数据库名的对应关系
# 库名要同 ctaBase 一致
BARSIZE_DBNAME_DICT = {}
BARSIZE_DBNAME_DICT = {
    BARSIZE_TICK:'VnTrader_Tick_Db',
    BARSIZE_M1:'VnTrader_1Min_Db',
    BARSIZE_M5:'VnTrader_5Min_Db',
    BARSIZE_M15: 'VnTrader_15Min_Db',
    BARSIZE_M30: 'VnTrader_30Min_Db',
    BARSIZE_H1: 'VnTrader_Hour_Db',
    BARSIZE_D: 'VnTrader_Daily_Db'
}


# 数据库名称
SETTING_DB_NAME = 'VnTrader_Setting_Db'
TICK_DB_NAME = 'VnTrader_Tick_Db'
DAILY_DB_NAME = 'VnTrader_Daily_Db'
MINUTE_DB_NAME = 'VnTrader_1Min_Db'         # 分钟 数据库名称 原名是 ： 'VnTrader_1Min_Db'

# 自己加上
HOUR_DB_NAME =  'VnTrader_Hour_Db'
MINUTE5_DB_NAME = 'VnTrader_5Min_Db'
MINUTE15_DB_NAME = 'VnTrader_15Min_Db'
MINUTE30_DB_NAME = 'VnTrader_30Min_Db'

# 引擎类型，用于区分当前策略的运行环境
ENGINETYPE_BACKTESTING = 'backtesting'  # 回测
ENGINETYPE_TRADING = 'trading'          # 实盘

# CTA引擎中涉及的数据类定义
from vtConstant import EMPTY_UNICODE, EMPTY_STRING, EMPTY_FLOAT, EMPTY_INT


########################################################################
class StopOrder(object):
    """本地停止单"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.vtSymbol = EMPTY_STRING
        self.orderType = EMPTY_UNICODE
        self.direction = EMPTY_UNICODE
        self.offset = EMPTY_UNICODE
        self.price = EMPTY_FLOAT
        self.volume = EMPTY_INT
        
        self.strategy = None             # 下停止单的策略对象
        self.stopOrderID = EMPTY_STRING  # 停止单的本地编号 
        self.status = EMPTY_STRING       # 停止单状态


########################################################################
class CtaBarData(object):
    """K线数据"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.vtSymbol = EMPTY_STRING        # vt系统代码
        self.symbol = EMPTY_STRING          # 代码
        self.exchange = EMPTY_STRING        # 交易所
        self.barsize = EMPTY_INT             # K线周期   自己增加
    
        self.open = EMPTY_FLOAT             # OHLC
        self.high = EMPTY_FLOAT
        self.low = EMPTY_FLOAT
        self.close = EMPTY_FLOAT
        
        self.date = EMPTY_STRING            # bar开始的时间，日期
        self.time = EMPTY_STRING            # 时间
        self.datetime = None                # python的datetime时间对象
        
        self.volume = EMPTY_INT             # 成交量
        self.openInterest = EMPTY_INT       # 持仓量


########################################################################
class CtaTickData(object):
    """Tick数据"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""       
        self.vtSymbol = EMPTY_STRING            # vt系统代码
        self.symbol = EMPTY_STRING              # 合约代码
        self.exchange = EMPTY_STRING            # 交易所代码

        # 成交数据
        self.lastPrice = EMPTY_FLOAT            # 最新成交价
        self.volume = EMPTY_INT                 # 最新成交量
        self.openInterest = EMPTY_INT           # 持仓量
        
        self.upperLimit = EMPTY_FLOAT           # 涨停价
        self.lowerLimit = EMPTY_FLOAT           # 跌停价
        
        # tick的时间
        self.date = EMPTY_STRING            # 日期
        self.time = EMPTY_STRING            # 时间
        self.datetime = None                # python的datetime时间对象
        
        # 五档行情
        self.bidPrice1 = EMPTY_FLOAT
        self.bidPrice2 = EMPTY_FLOAT
        self.bidPrice3 = EMPTY_FLOAT
        self.bidPrice4 = EMPTY_FLOAT
        self.bidPrice5 = EMPTY_FLOAT
        
        self.askPrice1 = EMPTY_FLOAT
        self.askPrice2 = EMPTY_FLOAT
        self.askPrice3 = EMPTY_FLOAT
        self.askPrice4 = EMPTY_FLOAT
        self.askPrice5 = EMPTY_FLOAT        
        
        self.bidVolume1 = EMPTY_INT
        self.bidVolume2 = EMPTY_INT
        self.bidVolume3 = EMPTY_INT
        self.bidVolume4 = EMPTY_INT
        self.bidVolume5 = EMPTY_INT
        
        self.askVolume1 = EMPTY_INT
        self.askVolume2 = EMPTY_INT
        self.askVolume3 = EMPTY_INT
        self.askVolume4 = EMPTY_INT
        self.askVolume5 = EMPTY_INT    