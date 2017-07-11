# encoding: UTF-8

"""
包含一些开发中常用的函数
"""

import talib

def SATR(highArray,lowArray,closeArray,atrLength):
	TR = talib.TRANGE(highArray,lowArray,closeArray)	
	return talib.SMA(TR,atrLength)	


    
    
    
