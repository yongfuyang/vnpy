# encoding: UTF-8

"""
包含一些开发中常用的函数
"""

import talib

def SATR(highArray,lowArray,closeArray,atrLength):
	TR = talib.TRANGE(highArray,lowArray,closeArray)	
	return talib.SMA(TR,atrLength)	


def KDJ(highArray,lowArray,closeArray,fastk_period=13,slowk_period=3,slowd_period=3):
	HighestValue = talib.MAX(highArray, fastk_period);
	LowestValue = talib.MIN(lowArray, fastk_period); 
	SumHLValue = talib.SUM(talib.SUB(HighestValue,LowestValue),slowk_period);
	SumCLValue = talib.SUM(talib.SUB(closeArray,LowestValue),slowk_period);
	
	KValue=talib.DIV(SumCLValue,SumHLValue)
	DValue = talib.SMA(KValue,slowd_period);
	return KValue,DValue
    
    
