# encoding: UTF-8

"""
展示如何执行策略回测。
"""

from __future__ import division

from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting, MINUTE_DB_NAME
from vnpy.trader.app.ctaStrategy.strategy.strategy_yyf_turtle_tb import YYFTurtleStrategy

if __name__ == '__main__':
	

	# 创建回测引擎
	engine = BacktestingEngine()

	# 设置引擎的回测模式为K线
	engine.setBacktestingMode(engine.BAR_MODE)

	# 设置回测用的数据起始日期
	engine.setStartDate('20120101')
	#engine.setEndDate('20120201')

	# 设置产品相关参数
	engine.setSlippage(0.2)     # 股指1跳
	engine.setRate(1.0/10000)   # 万0.3
	engine.setSize(300)         # 股指合约大小 
	engine.setPriceTick(0.2)    # 股指最小价格变动

	# 设置使用的历史数据库
	engine.setDatabase(MINUTE_DB_NAME, 'IF0000')

	# 在引擎中创建策略对象
	d = {"timeFrame":5}
	engine.initStrategy(YYFTurtleStrategy, d)

	# 开始跑回测
	engine.runBacktesting()

	# 显示回测结果
	engine.showBacktestingResult()