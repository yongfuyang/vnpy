# encoding: UTF-8

"""
展示如何执行策略回测。
"""

from __future__ import division

from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting, MINUTE_DB_NAME
from vnpy.trader.app.ctaStrategy.strategy.strategy_yyf_turtle_tb import YYFTurtleStrategy
from vnpy.trader.vtObject import VtTickData, VtBarData

import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui
import random

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
	#engine.runBacktesting()

	# 载入历史数据
	engine.loadHistoryData()	
	
	if engine.mode == engine.BAR_MODE:
		dataClass = VtTickData
		func = engine.newBar
	else:
		dataClass = CtaTickData
		func = engine.newTick

	engine.output(u'开始回测')

	engine.strategy.inited = True
	engine.strategy.onInit()
	engine.output(u'策略初始化完成')

	engine.strategy.trading = True
	engine.strategy.onStart()
	engine.output(u'策略启动完成')

	engine.output(u'开始回放数据')	

	# 开始跑回测
	app = QtGui.QApplication([])
	item = CandlestickItem()

	# 初始值
	count = 0
	data = []
	BAR_COUNT = 60
	for i in range(0,BAR_COUNT):
		d = engine.dbCursor[count]
		mdata = dataClass()
		mdata.__dict__ = d
		func(mdata)  # 策略执行        
		new_bar = [count,mdata.open,mdata.close,mdata.low,mdata.high]
		data.append(new_bar)  
		count = count+1

	item.set_data(data)
	plt = pg.plot()
	plt.addItem(item)
	plt.setWindowTitle('pyqtgraph example: customGraphicsItem')

	timer = QtCore.QTimer()
	timer.timeout.connect(update)
	timer.start(200)  # 定时间隔

	import sys
	if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
		QtGui.QApplication.instance().exec_()	