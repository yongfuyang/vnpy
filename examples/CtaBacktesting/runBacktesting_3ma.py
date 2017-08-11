# encoding: UTF-8

"""
展示如何执行策略回测。
"""

from __future__ import division

from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting, MINUTE_DB_NAME
from vnpy.trader.app.ctaStrategy.strategy.strategy_yyf_3ma import ThreeEmaStrategy
from vnpy.trader.vtObject import VtTickData, VtBarData

import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui
import random
import datetime

## Create a subclass of GraphicsObject.
## The only required methods are paint() and boundingRect() 
## (see QGraphicsItem documentation)
class CandlestickItem(pg.GraphicsObject):
	def __init__(self):
		pg.GraphicsObject.__init__(self)
		self.flagHasData = False

	def set_data(self, data):
		self.data = data  ## data must have fields: time, open, close, min, max
		self.flagHasData = True
		self.generatePicture()
		self.informViewBoundsChanged()

	def generatePicture(self):
		## pre-computing a QPicture object allows paint() to run much more quickly, 
		## rather than re-drawing the shapes every time.
		self.picture = QtGui.QPicture()
		p = QtGui.QPainter(self.picture)
		p.setPen(pg.mkPen('w'))
		w = (self.data[1][0] - self.data[0][0]) / 3.
		for (t, open, close, min, max) in self.data:
			p.drawLine(QtCore.QPointF(t, min), QtCore.QPointF(t, max))
			if open > close:
				p.setBrush(pg.mkBrush('r'))
			else:
				p.setBrush(pg.mkBrush('g'))
			p.drawRect(QtCore.QRectF(t-w, open, w*2, close-open))
		p.end()

	def paint(self, p, *args):
		if self.flagHasData:
			p.drawPicture(0, 0, self.picture)

	def boundingRect(self):
		## boundingRect _must_ indicate the entire area that will be drawn on
		## or else we will get artifacts and possibly crashing.
		## (in this case, QPicture does all the work of computing the bouning rect for us)
		return QtCore.QRectF(self.picture.boundingRect())




def update():
	global item, data, count
	d = engine.dbCursor[count]
	#engine.output(u'count=%s' %(count))
	#engine.output(u'total=%s' %(engine.dbCursor.count()))
	mdata = dataClass()
	mdata.__dict__ = d
	func(mdata)  # 策略执行

	new_bar = [count,mdata.open,mdata.close,mdata.low,mdata.high]
	data.append(new_bar)
	if len(data) > BAR_COUNT:  # 显示的BAR数目，满屏左移
		data.pop(0)      
	item.set_data(data)
	app.processEvents()  ## force complete redraw for every plot    
	count = count+1  # 下一个BAR
	if count >= engine.dbCursor.count():
		engine.output(u'结束')
		# 显示回测结果
		# spyder或者ipython notebook中运行时，会弹出盈亏曲线图
		# 直接在cmd中回测则只会打印一些回测数值
		engine.showBacktestingResult() 
		
if __name__ == '__main__':
	

	# 创建回测引擎
	engine = BacktestingEngine()

	# 设置引擎的回测模式为K线
	engine.setBacktestingMode(engine.BAR_MODE)

	# 设置回测用的数据起始日期
	engine.setStartDate('20120101')
	#engine.setEndDate('20170201')

	# 设置产品相关参数
	engine.setSlippage(1)     # 股指1跳
	engine.setRate(1.0/10000)   # 万0.3
	engine.setSize(10)         # 股指合约大小 
	engine.setPriceTick(1.0)    # 股指最小价格变动

	# 设置使用的历史数据库
	engine.setDatabase(MINUTE_DB_NAME, 'rb8888')

	# 在引擎中创建策略对象
	d = {"timeFrame":5}
	engine.initStrategy(ThreeEmaStrategy, d)
	
	# 开始跑回测
	engine.runBacktesting()

	# 显示回测结果
	engine.showBacktestingResult()	