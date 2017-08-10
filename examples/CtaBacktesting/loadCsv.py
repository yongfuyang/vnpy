# encoding: UTF-8

"""
导入MC导出的CSV历史数据到MongoDB中
"""

from vnpy.trader.app.ctaStrategy.ctaBase import MINUTE_DB_NAME
from vnpy.trader.app.ctaStrategy.ctaHistoryData import loadMcCsv
from vnpy.trader.app.ctaStrategy.ctaHistoryData import TBPLUSDataImport


if __name__ == '__main__':
    #loadMcCsv('IF0000_1min.csv', MINUTE_DB_NAME, 'IF0000')
    #loadMcCsv('rb888_1m.csv', MINUTE_DB_NAME, 'rb0000')
    TBPLUSDataImport('rb8888_tbplus.csv','.','./backup',to5m=False, to15m=False, to30m=False, toH1=False, toD1=False)

