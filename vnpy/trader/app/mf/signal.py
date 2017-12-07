# encoding: UTF-8
import MySQLdb
from mysql.connector import RefreshOption
from datetime import datetime, timedelta


#------------------------------------------------------------------------------------------------------------------
def plateSignal(dbName, Date, plateResonanceNum, stockInfoTableName):
	
	conn = MySQLdb.connect(host='1w8573f055.iok.la', port=35941, user='root',passwd='123456',db=dbName)
	cur = conn.cursor()	
	sql = "CALL Stock_Dasic_Data.getStockSignalPlateByDate_4('%s', %d,'%s')" %(Date,plateResonanceNum,stockInfoTableName)
	n=cur.execute(sql)
	rs=cur.fetchall()
	cur.close()
	conn.close()
	
	return rs,n

#------------------------------------------------------------------------------------------------------------------
def initStockInfoDate():
	conn = MySQLdb.connect(host='1w8573f055.iok.la', port=35941, user='root',passwd='123456',db='stock_basics_info_all_date')
	
	cur = conn.cursor()	
	sql = "SELECT t.calendarDate from Stock_Dasic_Data.stock_TradeCal t where t.calendarDate>'2017-10-01' "
	cur.execute(sql)	
	days=cur.fetchall()
	cur.close()
	
	cur = conn.cursor()	
	tablesql= "select table_name from information_schema.tables where table_schema='Stock_Basics_Info_All_New' and table_type='base table'"
	cur.execute(tablesql)
	tables=cur.fetchall()	
	cur.close()

	tableNameList=[]
	for d in days:
		tableNameList.append(str.replace(d[0], '-', '')+'stock_basics')

	for t in tableNameList:
		sql1="create table "+t
		sql2=""
		for t1 in tables:
			sql2=sql2+" select * from "+t1[0]+" where date='"+d[0]+"' union"

	sql3=sql1+sql2[:-5]
	#print sql3
	cur = conn.cursor()	
	cur.execute(sql3)		
	cur.close()
	conn.close()

	pass 	
	
#------------------------------------------------------------------------------------------------------------------
def initStockInfoDateBatch():
	conn = MySQLdb.connect(host='1w8573f055.iok.la', port=35941, user='root',passwd='123456',db='stock_basics_info_all_date')
	
	refresh = RefreshOption.LOG | RefreshOption.THREADS | RefreshOption.TABLES
	
	cur = conn.cursor()	
	sql = "SELECT t.calendarDate from Stock_Dasic_Data.stock_TradeCal t where t.calendarDate>'2011-01-01' "
	cur.execute(sql)	
	days=cur.fetchall()
	cur.close()
	
	cur = conn.cursor()	
	tablesql= "select table_name from information_schema.tables where table_schema='Stock_Basics_Info_All_New' and table_type='base table'"
	cur.execute(tablesql)
	tables=cur.fetchall()	
	cur.close()
	
	templateSql="""
	CREATE TABLE  IF NOT EXISTS  `TEMPLATETABLENAME` (
	`index` bigint(20) DEFAULT NULL COMMENT '序号-基础数据-tushare',
	`date` text COMMENT '日期和时间 低频数据时为：YYYY-MM-DD 高频数为：YYYY-MM-DD HH:MM',
	`open` double DEFAULT NULL COMMENT '开盘价-基础数据-tushare',
	`close` double DEFAULT NULL COMMENT '收盘价-基础数据-tushare',
	`high` double DEFAULT NULL COMMENT '最高价-基础数据-tushare',
	`low` double DEFAULT NULL COMMENT '最低价-基础数据-tushare',
	`volume` double DEFAULT NULL COMMENT '成交量-基础数据-tushare',
	`code` text COMMENT '证券代码-基础数据-tushare',
	`ma5` double DEFAULT NULL COMMENT '5日均价-公式计算数据-基础数据',
	`ma10` double DEFAULT NULL COMMENT '10日均价-公式计算数据-基础数据',
	`ma20` double DEFAULT NULL COMMENT '20日均价-公式计算数据-基础数据',
	`DailyFluctuation` double DEFAULT NULL COMMENT '每日波动率-公式计算数据-基础数据',
	`v_ma2` double DEFAULT NULL COMMENT '2日均量-公式计算数据-基础数据',
	`v_ma5` double DEFAULT NULL COMMENT '5日均量-公式计算数据-基础数据',
	`v_ma10` double DEFAULT NULL COMMENT '10日均量-公式计算数据-基础数据',
	`v_ma20` double DEFAULT NULL COMMENT '20日均量-公式计算数据-基础数据',
	`d_fa2` double DEFAULT NULL COMMENT '2日涨幅-公式计算数据-基础数据',
	`d_fa5` double DEFAULT NULL COMMENT '5日涨幅-公式计算数据-基础数据',
	`d_fa10` double DEFAULT NULL COMMENT '10日涨幅-公式计算数据-基础数据',
	`d_fa20` double DEFAULT NULL COMMENT '20日涨幅-公式计算数据-基础数据',
	`d_fa30` double DEFAULT NULL COMMENT '30日涨幅-公式计算数据-基础数据',
	`PB` double DEFAULT NULL COMMENT '市净率-补充数据-通联数据',
	`PE` double DEFAULT NULL COMMENT '市盈率-补充数据-通联数据',
	`PETTM` double DEFAULT NULL COMMENT '滚动市盈率-补充数据-通联数据',
	`infoSource` text COMMENT '发布机构代码-补充数据-通联数据',
	`sacIndustryCD` text COMMENT '交易市场-补充数据-通联数据',
	`secShortName` text COMMENT '证券简称-补充数据-通联数据',
	`amount` double DEFAULT NULL COMMENT '成交量-基础数据-wind',
	`BuyingVolume` double DEFAULT NULL COMMENT '机构买入量-基础数据-wind',
	`SellingVolume` double DEFAULT NULL COMMENT '机构卖出量-基础数据-wind',
	`OrgVolume` double DEFAULT NULL COMMENT '机构当日成交额-公式计算数据-基础数据',
	KEY `ix_000001stock_basics_index` (`index`)
  ) ENGINE=MyISAM DEFAULT CHARSET=utf8;
  """

	tableNameList=[]
	for d in days:
		tableNameList.append(str.replace(d[0], '-', '')+'stock_basics')

	for t in tableNameList:
		
		while len(tables)>0:
			sql1=templateSql.replace("TEMPLATETABLENAME", t)+" insert into "+t
			
			if len(tables)>=100:
				
				sql2=""
				for i in range(99) :
					t1=tables[i]			
					sql2=sql2+" select * from Stock_Basics_Info_All_New."+t1[0]+" where date='"+d[0]+"' union"
		
				sql3=sql1+sql2[:-5]
				#print sql3
				cur = conn.cursor()	
				cur.execute(sql3)
				tables=tables[100:]				
				cur.close()
				#conn.cmd_refresh(refresh)
				conn.commit()
			else:

				sql2=""
				for i in range(len(tables)) :
					t1=tables[i]			
					sql2=sql2+" select * from Stock_Basics_Info_All_New."+t1[0]+" where date='"+d[0]+"' union"
		
				sql3=sql1+sql2[:-5]
				#print sql3
				cur = conn.cursor()	
				cur.execute(sql3)
				tables=[]
				cur.close()
				#conn.cmd_refresh(refresh)
				conn.commit()
	
	conn.close()

	pass 


class MfSignal(object):
	
	def init(self):
		self.turnoverRate=5 #换手率高于，0为不计算
		self.fluctuation=1	#涨跌幅，0为不设置
		self.resonanceNum=4	#版块共振的信号数目		
		self.pe=50			#pe，0为不设置
		self.flu50=20		#前50日涨幅小于20%
		self.orgBuySellRate=2	#机构当日净买入额是机构当日净卖出额的2倍以上
		
	def stockSignal(self):
		pass
	
	
		

if __name__ == '__main__':
	'''
	startDate = datetime.strptime('20151201', '%Y%m%d').date()
	endDate   = datetime.strptime('20170101', '%Y%m%d').date()
	
	d = startDate
	delta = timedelta(days=1) 
	
	while d <= endDate:  
		print d.strftime("%Y-%m-%d")  
		rs,n=plateSignal('Stock_Dasic_Data', d, 4, 'Stock_Signal_Info.Stock_Signal_Info_18')
		print n,rs
			
		d += delta  
	'''
	
	initStockInfoDateBatch()
	
	