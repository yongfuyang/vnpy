# encoding: UTF-8

from collections import OrderedDict

from vnpy.event import Event
from vnpy.trader.uiQt import QtWidgets, QtCore
from vnpy.trader.uiBasicWidget import (BasicMonitor, BasicCell, PnlCell,
                                       AskCell, BidCell, BASIC_FONT)

from .stBase import (EVENT_SPREADTRADING_TICK, EVENT_SPREADTRADING_POS,
                     EVENT_SPREADTRADING_LOG, EVENT_SPREADTRADING_ALGO,
                     EVENT_SPREADTRADING_ALGOLOG)
from .stAlgo import MfAlgoTemplate


STYLESHEET_START = 'background-color: rgb(111,255,244); color: black'
STYLESHEET_STOP = 'background-color: rgb(255,201,111); color: black'


########################################################################
class MfTickMonitor(BasicMonitor):
    """价差行情监控"""
    
    #----------------------------------------------------------------------
    def __init__(self, mainEngine, eventEngine, parent=None):
        """Constructor"""
        super(MfTickMonitor, self).__init__(mainEngine, eventEngine, parent)
        
        d = OrderedDict()
        d['name'] = {'chinese':u'价差名称', 'cellType':BasicCell}
        d['bidPrice'] = {'chinese':u'买价', 'cellType':BidCell}
        d['bidVolume'] = {'chinese':u'买量', 'cellType':BidCell}
        d['askPrice'] = {'chinese':u'卖价', 'cellType':AskCell}
        d['askVolume'] = {'chinese':u'卖量', 'cellType':AskCell}
        d['time'] = {'chinese':u'时间', 'cellType':BasicCell}
        d['symbol'] = {'chinese':u'价差公式', 'cellType':BasicCell}
        self.setHeaderDict(d)
    
        self.setDataKey('name')
        self.setEventType(EVENT_SPREADTRADING_TICK)
        self.setFont(BASIC_FONT)
    
        self.initTable()
        self.registerEvent()        


########################################################################
class MfPosMonitor(BasicMonitor):
    """价差持仓监控"""
    
    #----------------------------------------------------------------------
    def __init__(self, mainEngine, eventEngine, parent=None):
        """Constructor"""
        super(MfPosMonitor, self).__init__(mainEngine, eventEngine, parent)
        
        d = OrderedDict()
        d['name'] = {'chinese':u'价差名称', 'cellType':BasicCell}
        d['netPos'] = {'chinese':u'净仓', 'cellType':PnlCell}
        d['longPos'] = {'chinese':u'多仓', 'cellType':BasicCell}
        d['shortPos'] = {'chinese':u'空仓', 'cellType':BasicCell}
        d['symbol'] = {'chinese':u'代码', 'cellType':BasicCell}
        self.setHeaderDict(d)
    
        self.setDataKey('name')
        self.setEventType(EVENT_SPREADTRADING_POS)
        self.setFont(BASIC_FONT)
    
        self.initTable()
        self.registerEvent()        


########################################################################
class MfLogMonitor(QtWidgets.QTextEdit):
    """价差日志监控"""
    signal = QtCore.Signal(type(Event()))
    
    #----------------------------------------------------------------------
    def __init__(self, mainEngine, eventEngine, parent=None):
        """Constructor"""
        super(MfLogMonitor, self).__init__(parent)
        
        self.eventEngine = eventEngine
        
        self.registerEvent()
        
    #----------------------------------------------------------------------
    def processLogEvent(self, event):
        """处理日志事件"""
        log = event.dict_['data']
        content = '%s:%s' %(log.logTime, log.logContent)
        self.append(content)
    
    #----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.signal.connect(self.processLogEvent)
        
        self.eventEngine.register(EVENT_SPREADTRADING_LOG, self.signal.emit)


########################################################################
class MfAlgoLogMonitor(BasicMonitor):
    """价差日志监控"""
    
    #----------------------------------------------------------------------
    def __init__(self, mainEngine, eventEngine, parent=None):
        """Constructor"""
        super(MfAlgoLogMonitor, self).__init__(mainEngine, eventEngine, parent)
        
        d = OrderedDict()
        d['logTime'] = {'chinese':u'时间', 'cellType':BasicCell}
        d['logContent'] = {'chinese':u'信息', 'cellType':BasicCell}
        self.setHeaderDict(d)
    
        self.setEventType(EVENT_SPREADTRADING_ALGOLOG)
        self.setFont(BASIC_FONT)
    
        self.initTable()
        self.registerEvent()
        

########################################################################
class MfBuyPriceSpinBox(QtWidgets.QDoubleSpinBox):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, algoEngine, spreadName, price, parent=None):
        """Constructor"""
        super(MfBuyPriceSpinBox, self).__init__(parent)
        
        self.algoEngine = algoEngine
        self.spreadName = spreadName
        
        self.setDecimals(4)
        self.setRange(-10000, 10000)
        self.setValue(price)
        
        self.valueChanged.connect(self.setPrice)
        
    #----------------------------------------------------------------------
    def setPrice(self, value):
        """设置价格"""
        self.algoEngine.setAlgoBuyPrice(self.spreadName, value)
    

########################################################################
class MfSellPriceSpinBox(QtWidgets.QDoubleSpinBox):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, algoEngine, spreadName, price, parent=None):
        """Constructor"""
        super(MfSellPriceSpinBox, self).__init__(parent)
        
        self.algoEngine = algoEngine
        self.spreadName = spreadName
        
        self.setDecimals(4)
        self.setRange(-10000, 10000)
        self.setValue(price)
        
        self.valueChanged.connect(self.setPrice)
        
    #----------------------------------------------------------------------
    def setPrice(self, value):
        """设置价格"""
        self.algoEngine.setAlgoSellPrice(self.spreadName, value)


########################################################################
class MfShortPriceSpinBox(QtWidgets.QDoubleSpinBox):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, algoEngine, spreadName, price, parent=None):
        """Constructor"""
        super(MfShortPriceSpinBox, self).__init__(parent)
        
        self.algoEngine = algoEngine
        self.spreadName = spreadName
        
        self.setDecimals(4)
        self.setRange(-10000, 10000)
        self.setValue(price)
        
        self.valueChanged.connect(self.setPrice)
        
    #----------------------------------------------------------------------
    def setPrice(self, value):
        """设置价格"""
        self.algoEngine.setAlgoShortPrice(self.spreadName, value)


########################################################################
class MfCoverPriceSpinBox(QtWidgets.QDoubleSpinBox):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, algoEngine, spreadName, price, parent=None):
        """Constructor"""
        super(MfCoverPriceSpinBox, self).__init__(parent)
        
        self.algoEngine = algoEngine
        self.spreadName = spreadName
        
        self.setDecimals(4)
        self.setRange(-10000, 10000)
        self.setValue(price)
        
        self.valueChanged.connect(self.setPrice)
        
    #----------------------------------------------------------------------
    def setPrice(self, value):
        """设置价格"""
        self.algoEngine.setAlgoCoverPrice(self.spreadName, value)
    

########################################################################
class MfMaxPosSizeSpinBox(QtWidgets.QSpinBox):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, algoEngine, spreadName, size, parent=None):
        """Constructor"""
        super(MfMaxPosSizeSpinBox, self).__init__(parent)
        
        self.algoEngine = algoEngine
        self.spreadName = spreadName
        
        self.setRange(-10000, 10000)
        self.setValue(size)
        
        self.valueChanged.connect(self.setSize)
        
    #----------------------------------------------------------------------
    def setSize(self, size):
        """设置价格"""
        self.algoEngine.setAlgoMaxPosSize(self.spreadName, size)


########################################################################
class MfMaxOrderSizeSpinBox(QtWidgets.QSpinBox):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, algoEngine, spreadName, size, parent=None):
        """Constructor"""
        super(MfMaxOrderSizeSpinBox, self).__init__(parent)
        
        self.algoEngine = algoEngine
        self.spreadName = spreadName
        
        self.setRange(-10000, 10000)
        self.setValue(size)
        
        self.valueChanged.connect(self.setSize)
        
    #----------------------------------------------------------------------
    def setSize(self, size):
        """设置价格"""
        self.algoEngine.setAlgoMaxOrderSize(self.spreadName, size)    



    

########################################################################
class MfAlgoManager(QtWidgets.QTableWidget):
    """价差算法管理组件"""

    #----------------------------------------------------------------------
    def __init__(self, stEngine, parent=None):
        """Constructor"""
        super(MfAlgoManager, self).__init__(parent)
        
        self.algoEngine = stEngine.algoEngine
        
        self.buttonActiveDict = {}       # spreadName: buttonActive
        
        self.initUi()
        
    #----------------------------------------------------------------------
    def initUi(self):
        """初始化表格"""
        headers = [u'价差',
                   u'算法',
                   'BuyPrice',
                   'SellPrice',
                   'CoverPrice',
                   'ShortPrice',
                   u'委托上限',
                   u'持仓上限',
                   u'模式',
                   u'状态']
        self.setColumnCount(len(headers))
        self.setHorizontalHeaderLabels(headers)
        self.horizontalHeader().setResizeMode(QtWidgets.QHeaderView.Stretch)
        
        self.verticalHeader().setVisible(False)
        self.setEditTriggers(self.NoEditTriggers)
        
    #----------------------------------------------------------------------
    def initCells(self):
        """初始化单元格"""
        algoEngine = self.algoEngine
        
        l = self.algoEngine.getAllAlgoParams()
        self.setRowCount(len(l))
        
        for row, d in enumerate(l):            
            cellSpreadName = QtWidgets.QTableWidgetItem(d['spreadName'])
            cellAlgoName = QtWidgets.QTableWidgetItem(d['algoName'])
            spinBuyPrice = StBuyPriceSpinBox(algoEngine, d['spreadName'], d['buyPrice'])
            spinSellPrice = StSellPriceSpinBox(algoEngine, d['spreadName'], d['sellPrice'])
            spinShortPrice = StShortPriceSpinBox(algoEngine, d['spreadName'], d['shortPrice'])
            spinCoverPrice = StCoverPriceSpinBox(algoEngine, d['spreadName'], d['coverPrice'])
            spinMaxOrderSize = StMaxOrderSizeSpinBox(algoEngine, d['spreadName'], d['maxOrderSize'])
            spinMaxPosSize = StMaxPosSizeSpinBox(algoEngine, d['spreadName'], d['maxPosSize'])
            comboMode = StModeComboBox(algoEngine, d['spreadName'], d['mode'])
            buttonActive = StActiveButton(algoEngine, d['spreadName'])
            
            self.setItem(row, 0, cellSpreadName)
            self.setItem(row, 1, cellAlgoName)
            self.setCellWidget(row, 2, spinBuyPrice)
            self.setCellWidget(row, 3, spinSellPrice)
            self.setCellWidget(row, 4, spinCoverPrice)
            self.setCellWidget(row, 5, spinShortPrice)
            self.setCellWidget(row, 6, spinMaxOrderSize)
            self.setCellWidget(row, 7, spinMaxPosSize)
            self.setCellWidget(row, 8, comboMode)
            self.setCellWidget(row, 9, buttonActive)
            
            buttonActive.signalActive.connect(comboMode.algoActiveChanged)
            
            self.buttonActiveDict[d['spreadName']] = buttonActive
            
    #----------------------------------------------------------------------
    def stopAll(self):
        """停止所有算法"""
        for button in self.buttonActiveDict.values():
            button.stop()     


########################################################################
class MfGroup(QtWidgets.QGroupBox):
    """集合显示"""

    #----------------------------------------------------------------------
    def __init__(self, widget, title, parent=None):
        """Constructor"""
        super(MfGroup, self).__init__(parent)
        
        self.setTitle(title)
        vbox = QtWidgets.QVBoxLayout()
        vbox.addWidget(widget)
        self.setLayout(vbox)
        

########################################################################
class MfManager(QtWidgets.QWidget):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, mfEngine, eventEngine, parent=None):
        """Constructor"""
        super(MfManager, self).__init__(parent)
        
        self.mfEngine = mfEngine
        self.mainEngine = mfEngine.mainEngine
        self.eventEngine = eventEngine
        
        self.initUi()
        
    #----------------------------------------------------------------------
    def initUi(self):
        """初始化界面"""
        self.setWindowTitle(u'世纪潮股票终端')
        
        # 创建组件
        self.tickMonitor = MfTickMonitor(self.mainEngine, self.eventEngine)
        self.signalMonitor = MfPosMonitor(self.mainEngine, self.eventEngine)
        self.traceMonitor = StLogMonitor(self.mainEngine, self.eventEngine)        
        
        # 创建按钮
        self.tickButton = QtWidgets.QPushButton(u'版块行情')
        self.tickButton.clicked.connect(self.tickButtonClick)       
        
        self.daySignalButton = QtWidgets.QPushButton(u'今日信号')
        self.daySignalButton.clicked.connect(self.daySignalButtonClick)
        
        self.traceButton = QtWidgets.QPushButton(u'信号追踪')
        self.traceButton.clicked.connect(self.traceButtonClick)        
        
                
        # 设置布局
        self.vbox = QtWidgets.QVBoxLayout()
        self.vbox.addWidget(tickButton)
        self.vbox.addWidget(daySignalButton)
        self.vbox.addWidget(traceButton)
        
        self.right = QtWidgets.QGridLayout()
        self.right.addWidget(self.tickMonitor)
        
        grid = QtWidgets.QGridLayout()
        grid.addWidget(self.vbox, 0, 0)
        grid.addWidget(self.right, 0, 1)

        self.setLayout(grid)
        
        self.initStatusBar()
        
    #----------------------------------------------------------------------
    def tickButtonClick(self):
        self.tickMonitor.show()
    
    #----------------------------------------------------------------------
    def show(self):
        """重载显示"""
        self.showMaximized()
        
    #----------------------------------------------------------------------
    def init(self):
        """初始化"""
        self.stEngine.init()
        self.algoManager.initCells()
    
    
    #----------------------------------------------------------------------
    def initStatusBar(self):
        """初始化状态栏"""
        self.statusLabel = QtWidgets.QLabel()
        self.statusLabel.setAlignment(QtCore.Qt.AlignLeft)
        
        self.statusBar().addPermanentWidget(self.statusLabel)
        self.statusLabel.setText(self.getCpuMemory())
        
        self.sbCount = 0
        self.sbTrigger = 10     # 10秒刷新一次
        self.signalStatusBar.connect(self.updateStatusBar)
        self.eventEngine.register(EVENT_TIMER, self.signalStatusBar.emit)
        
    #----------------------------------------------------------------------
    def updateStatusBar(self, event):
        """在状态栏更新CPU和内存信息"""
        self.sbCount += 1
        
        if self.sbCount == self.sbTrigger:
            self.sbCount = 0
            self.statusLabel.setText(self.getCpuMemory())
    
    #----------------------------------------------------------------------
    def getCpuMemory(self):
        """获取CPU和内存状态信息"""
        cpuPercent = psutil.cpu_percent()
        memoryPercent = psutil.virtual_memory().percent
        return vtText.CPU_MEMORY_INFO.format(cpu=cpuPercent, memory=memoryPercent)    
    