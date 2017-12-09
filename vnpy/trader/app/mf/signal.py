# encoding: UTF-8
import MySQLdb
import mysql.connector
from mysql.connector import RefreshOption
from datetime import datetime, timedelta
import csv

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
	conn = MySQLdb.connect(host='1w8573f055.iok.la', port=35941, user='root',passwd='123456',db='stock_basics_info_all_date',connect_timeout=36000)
	#conn = mysql.connector.connect(host='1w8573f055.iok.la', port=35941, user='root',password='123456',database='stock_basics_info_all_date',use_unicode=True,charset="utf8")
	
	refresh = RefreshOption.LOG | RefreshOption.THREADS | RefreshOption.TABLES
	
	cur = conn.cursor()	
	sql = "SELECT t.calendarDate from Stock_Dasic_Data.stock_TradeCal t where t.calendarDate>'2012-12-07' "
	cur.execute(sql)	
	days=cur.fetchall()
	cur.close()
	
	cur = conn.cursor()	
	tablesql= "select table_name from information_schema.tables where table_schema='test' and table_type='base table' and table_name like '%stock_basics' "
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
	dateTableNames=[]
	for d in days:
		tn=str.replace(d[0], '-', '')+'stock_basics'
		tableNameList.append(tn)
		dateTableNames.append((d[0],tn))

	sql2=""
	for d,t in dateTableNames:
		cur = conn.cursor()	
		sql1=templateSql.replace("TEMPLATETABLENAME", t)
		cur.execute(sql1)
		cur.close()		
		
		i=0
		while i<len(tables):
			sql1=" insert into "+t
			t1=tables[i]			
			sql2=sql2+" select * from test."+t1[0]+" where date='"+d+"' union"
			i=i+1				
			
			if i%100 == 0 or i==len(tables) :
				sql3=sql1+sql2[:-5]
				
				cur = conn.cursor()	
				n=cur.execute(sql3)
				cur.close()
				print 'insert %d rows!'%n
				#conn.cmd_refresh(refresh)
				conn.commit()				
				sql2=""
				
			
	conn.close()

	pass 

#------------------------------------------------------------------------------------------------------------------
def initStockInfoDateBatchYear(yearEnd):
	conn = MySQLdb.connect(host='1w8573f055.iok.la', port=35941, user='root',passwd='123456',db='stock_basics_info_all_year',connect_timeout=36000)
	#conn = mysql.connector.connect(host='1w8573f055.iok.la', port=35941, user='root',password='123456',database='stock_basics_info_all_date',use_unicode=True,charset="utf8")
	
	refresh = RefreshOption.LOG | RefreshOption.THREADS | RefreshOption.TABLES
	
	year=2011
	
	cur = conn.cursor()	
	tablesql= "select table_name from information_schema.tables where table_schema='test' and table_type='base table' and table_name like '%stock_basics' "
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
	dateTableNames=[]
	while year<yearEnd:
		tn=str(year)+'stock_basics'
		tableNameList.append(tn)
		dateTableNames.append((year,tn))
		year=year+1

	sql2=""
	for y,t in dateTableNames:
		cur = conn.cursor()	
		sql1=templateSql.replace("TEMPLATETABLENAME", t)
		cur.execute(sql1)
		cur.close()		
		
		i=0
		while i<len(tables):
			sql1=" insert into "+t
			t1=tables[i]			
			sql2=sql2+" select * from test."+t1[0]+" where date like '"+str(y)+"%' union"
			i=i+1				
			
			if i%100 == 0 or i==len(tables) :
				sql3=sql1+sql2[:-5]
				
				cur = conn.cursor()	
				n=cur.execute(sql3)
				cur.close()
				print 'insert %d rows!'%n
				#conn.cmd_refresh(refresh)
				conn.commit()				
				sql2=""
				
			
	conn.close()

	pass 

#------------------------------------------------------------------------------------------------------------------
def initStockOneByOne(yearEnd):
	conn = MySQLdb.connect(host='1w8573f055.iok.la', port=35941, user='root',passwd='123456',db='stock_one_by_one',connect_timeout=36000)
	#conn = mysql.connector.connect(host='1w8573f055.iok.la', port=35941, user='root',password='123456',database='stock_basics_info_all_date',use_unicode=True,charset="utf8")
	
	refresh = RefreshOption.LOG | RefreshOption.THREADS | RefreshOption.TABLES
	
	year=2011
	
	cur = conn.cursor()	
	tablesql= "select table_name from information_schema.tables where table_schema='test' and table_type='base table' and table_name like '%stock_basics' "
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
	`org_v_ma2` double DEFAULT NULL COMMENT '2日均量-公式计算数据-基础数据',
	`org_v_ma5` double DEFAULT NULL COMMENT '5日均量-公式计算数据-基础数据',
	`org_v_ma10` double DEFAULT NULL COMMENT '10日均量-公式计算数据-基础数据',
	`org_v_ma20` double DEFAULT NULL COMMENT '20日均量-公式计算数据-基础数据', 
	`ma20_before50` double DEFAULT NULL COMMENT '50日前的20日均价-公式计算数据-基础数据'
	KEY `ix_stock_basics_index` (`index`)
  ) ENGINE=MyISAM DEFAULT CHARSET=utf8;
  """

	for tt in tables:
		t=tt[0]
		cur = conn.cursor()	
		sql1=templateSql.replace("TEMPLATETABLENAME", t)
		cur.execute(sql1)
		cur.close()
		
		cur = conn.cursor()	
		sql2=" insert into "+t+" select * from test."+t+" order by date"
		cur.execute(sql2)
		cur.close()
		
		cur = conn.cursor()	
		sql2=" update "+t+" t1 set t1.org_v_ma2= (select avg(OrgVolume) from test."+t+" t2 where t2.index>=t1.index-2 and t2.index<t1.index)"
		cur.execute(sql2)
		cur.close()
		
		cur = conn.cursor()	
		sql2=" update "+t+" t1 set t1.org_v_ma5= (select avg(OrgVolume) from test."+t+" t2 where t2.index>=t1.index-5 and t2.index<t1.index)"
		cur.execute(sql2)
		cur.close()
		
		cur = conn.cursor()	
		sql2=" update "+t+" t1 set t1.org_v_ma10= (select avg(OrgVolume) from test."+t+" t2 where t2.index>=t1.index-10 and t2.index<t1.index)"
		cur.execute(sql2)
		cur.close()
		
		cur = conn.cursor()	
		sql2=" update "+t+" t1 set t1.org_v_ma20= (select avg(OrgVolume) from test."+t+" t2 where t2.index>=t1.index-20 and t2.index<t1.index)"
		cur.execute(sql2)
		cur.close()	
		
		cur = conn.cursor()	
		sql2=" update "+t+" t1 set t1.ma20_before50= (select avg(close) from test."+t+" t2 where t2.index>=t1.index-70 and t2.index<t1.index-50)"
		cur.execute(sql2)
		cur.close()			

		
		conn.commit()				
		
		
				
			
	conn.close()

	pass 

#-----------------------------------------------------------------------------------------------------------------------------------------
class MfSignal(object):
	
	#--------------------------------------------------------
	def __init__(self):
		self.turnoverRate=0.05 #换手率高于，0为不计算
		self.fluctuation=0.01	#涨跌幅，0为不设置
		self.resonanceNum=4	#版块共振的信号数目		
		self.pe=50			#pe，0为不设置
		self.fluN=0.20		#前N日涨幅小于20%
		self.orgBuySellRate=2	#机构当日净买入额是机构当日净卖出额的2倍以上
		self.orgVolRation=1.5	#当日机构成交量是前两日成交量均值的1.5倍
	
	#--------------------------------------------------------	
	def stockSignal(self):
		conn = MySQLdb.connect(host='1w8573f055.iok.la', port=35941, user='root',passwd='123456',db='stock_basics_info_all_date')
		
		cur = conn.cursor()	
		sql = "SELECT t.calendarDate from Stock_Dasic_Data.stock_TradeCal t where t.calendarDate>'2011-01-01' "
		cur.execute(sql)	
		days=cur.fetchall()
		cur.close()	
		
		file = open('./signal.csv','w')
		tableNameList=[]
		for d in days:
			tableNameList=(str.replace(d[0], '-', '')+'stock_basics')	
			
			cur = conn.cursor()	
			sql = """
			select * from %s where date ='%s'
			and DailyFluctuation>%s
			and volume/v_ma2>%s
			and close>ma5
			and ma5>ma10
			and ma10>ma20
			and d_fa30<%s
			and BuyingVolume/SellingVolume>%s
			""" %(tableNameList, d[0], self.fluctuation, self.orgVolRation, self.fluN, self.orgBuySellRate)
			n=cur.execute(sql)
			rs=cur.fetchall()
			#print '------------'+d[0]+"----------------"
			#print rs
			cur.close()
			
			with open('stockSignal.csv', 'ab') as f:
				writer = csv.writer(f, dialect='excel')
				writer.writerows(rs)
				
			
			
		conn.close()		
		pass
	
	
	#--------------------------------------------------------	
	def stockSignalFast(self,yearEnd):
		conn = MySQLdb.connect(host='1w8573f055.iok.la', port=35941, user='root',passwd='123456',db='stock_basics_info_all_year')
		
		year=2011
		
		tableNameList=[]
		while year<yearEnd:
			tableNameList=(str(year)+'stock_basics')	
			
			cur = conn.cursor()	
			sql = """
		    select * from %s where date like '%s%%'
		    and DailyFluctuation>%s
		    and volume/v_ma2>%s
		    and close>ma5
		    and ma5>ma10
		    and ma10>ma20
			and d_fa30<%s
		    and BuyingVolume/SellingVolume>%s
		    """ %(tableNameList, year, self.fluctuation, self.orgVolRation, self.fluN,  self.orgBuySellRate)
			n=cur.execute(sql)
			rs=cur.fetchall()
			#print '------------'+str(year)+"----------------"
			#print rs
			cur.close()
			year=year+1
			
			with open('stockSignalFast.csv', 'ab') as f:
				writer = csv.writer(f, dialect='excel')
				writer.writerows(rs)			

		conn.close()		
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
	
	#initStockInfoDateBatch()
	#initStockInfoDateBatchYear(2018)
	
	
	import time
	import datetime
	
	s=MfSignal()
	t1 = time.time()
	s.stockSignalFast(2018)
	t2= time.time()
	print u'---stockSignalFast耗时：%s' %(t2-t1)
	s.stockSignal()
	t3 = time.time()
	print u'---stockSignal: %s' %(t3-t2)