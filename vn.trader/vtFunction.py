# encoding: UTF-8

"""
包含一些开发中常用的函数
"""

import os
import decimal
import json
from datetime import *

MAX_NUMBER = 10000000000000
MAX_DECIMAL = 4

#----------------------------------------------------------------------
def safeUnicode(value):
    """检查接口数据潜在的错误，保证转化为的字符串正确"""
    # 检查是数字接近0时会出现的浮点数上限
    if type(value) is int or type(value) is float:
        if value > MAX_NUMBER:
            value = 0
    
    # 检查防止小数点位过多
    if type(value) is float:
        d = decimal.Decimal(str(value))
        if abs(d.as_tuple().exponent) > MAX_DECIMAL:
            value = round(value, ndigits=MAX_DECIMAL)
    
    return unicode(value)

#----------------------------------------------------------------------
def loadMongoSetting():
    """载入MongoDB数据库的配置"""
    fileName = 'VT_setting.json'
    path = os.path.abspath(os.path.dirname(__file__)) 
    fileName = os.path.join(path, fileName)  
    
    try:
        f = file(fileName)
        setting = json.load(f)
        host = setting['mongoHost']
        port = setting['mongoPort']
        logging = setting['mongoLogging']
    except:
        host = 'localhost'
        port = 27017
        logging = False
        
    return host, port, logging

#----------------------------------------------------------------------
def todayDate():
    """获取当前本机电脑时间的日期"""
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)    

def toESTtime(localtime=None, estdatetime=None, timestamp=None, returnStr=False):
    """
        如果没有传入任何参数。则把当前时间转为 EST 时间格式
        localtime: 本地时间转为 EST 时间， 不传来代表本地时间转为 EST 时间
        datetime:  传入一个 EST 时间
        timestampe:  把传入的 timestamp 转为 EST 时间
        returnStr:  是否返回 STR 格式
        存在问题： 对于比较的时间，发现相差一个小时。
    """
#   先判断 timestamp, 再判断 datetime，没有则默认 localtime

    if timestamp:   # 如果传入的是 timestamp
        if isinstance(timestamp, (str)) and len(timestamp) == 8:    # IB 返回的日线 timestamp
            estDateTime = datetime.strptime(timestamp, '%Y%m%d')
        else:
            # 日内来的 timeStamp  要减 4个小时
            estDateTime = datetime.utcfromtimestamp(float(timestamp))
            estDateTime = estDateTime - timedelta(hours=4)  # EST  GMT-4:00

        if returnStr == True:
            estDateTimeStr = estDateTime.strftime('%Y%m%d %H:%M:%S EST')
            return estDateTimeStr
        return estDateTime

    elif estdatetime:        # 如果传入的是一个 EST 时间
        if isinstance(estdatetime, (str)):                                   #是否字符串
            if len(estdatetime)==8:
                estDateTime = datetime.strptime(estdatetime, '%Y%m%d')
            else:
                estDateTime =  datetime.strptime(estdatetime, '%Y%m%d %H:%M:%S')

        elif isinstance(estdatetime,(datetime)):                             #传入的是日期格式
            estDateTime = estdatetime

        if returnStr == True:
            estDateTimeStr = estDateTime.strftime('%Y%m%d %H:%M:%S EST')
            return estDateTimeStr
        return estDateTime

    else:
        estDateTime = datetime.utcnow() - timedelta(hours=4)  # EST = GMT -4:00
        if returnStr == True:
            estDateTimeStr = estDateTime.strftime('%Y%m%d %H:%M:%S EST')
            return estDateTimeStr
        return estDateTime