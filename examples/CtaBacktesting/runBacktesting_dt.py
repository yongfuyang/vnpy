# encoding: UTF-8

"""
展示如何执行策略回测。
"""

from __future__ import division

from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting, MINUTE_DB_NAME
from vnpy.trader.app.ctaStrategy.strategy.strategy_yyf_dt import YYFDualThrustStrategy
#from vnpy.trader.app.ctaStrategy.strategy.strategyDualThrust import DualThrustStrategy
#from vnpy.trader.app.ctaStrategy.strategy.strategyKingKeltner import KkStrategy
#from vnpy.trader.app.ctaStrategy.strategy.strategyAtrRsi import AtrRsiStrategy

if __name__ == '__main__':
	

	# 创建回测引擎
	engine = BacktestingEngine()

	# 设置引擎的回测模式为K线
	engine.setBacktestingMode(engine.BAR_MODE)

	# 设置回测用的数据起始日期
	engine.setStartDate('20110501')
	engine.setEndDate('20110720')

	# 设置产品相关参数
	engine.setSlippage(1)     # 1跳
	engine.setRate(1.0/10000)   # 万1
	engine.setSize(10)         # 合约大小 
	engine.setPriceTick(1)    # 最小价格变动

	# 设置使用的历史数据库
	engine.setDatabase(MINUTE_DB_NAME, 'rb8888')

	# 在引擎中创建策略对象
	d = {'k1':0.5,'k2':0.5}
	engine.initStrategy(YYFDualThrustStrategy, d)

	# 开始跑回测
	engine.runBacktesting()

	# 显示回测结果
	engine.showBacktestingResult()