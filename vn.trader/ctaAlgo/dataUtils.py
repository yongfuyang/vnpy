#summarize.py
import datetime
import pymongo
from ctaBase import *

def writeToMongoDB(currentBarket, barketData, collection, symbol):
	bar = CtaBarData()
	bar.vtSymbol = symbol
	bar.symbol = symbol
	bar.open = barketData['Open']
	bar.high = barketData['High']
	bar.low = barketData['Low']
	bar.close = barketData['Close']
	bar.date = currentBarket.strftime('%Y%m%d')
	bar.time = currentBarket.strftime('%H:%M:%S')
	bar.datetime = currentBarket
	bar.volume = barketData['Vol']
	bar.openInterest = barketData['Val']

	flt = {'datetime': bar.datetime}
	collection.update_one(flt, {'$set':bar.__dict__}, upsert=True)  
	print bar.date, bar.time


def writeToBarket(outputFile, currentBarket, barketData, collection=None, symbol='Default'):
	if (currentBarket == None):
		return
	#DateTime,Open,High,Low,Close,Vol,Val
	#
	vDateTime = currentBarket.strftime('%Y%m%d %H:%M:%S')
	#
	line = vDateTime + ","
	line += str(barketData['Open']) + ","
	line += str(barketData['High']) + ","
	line += str(barketData['Low']) + ","
	line += str(barketData['Close']) + ","
	line += str(barketData['Vol']) + ","
	line += str(barketData['Val']) + "\n"
	#
	outputFile.write(line)
	if collection != None:
		writeToMongoDB(currentBarket, barketData, collection, symbol)



# if span is 0, that means 1 day span
def resample(span, iFile, oFile, collection=None, symbol='Default'):
	#DateTime,Open,High,Low,Close,Vol,Val
	with open(iFile, 'r') as inputFile:
		line = inputFile.readline() # ignore the first line
		#
		currentBarket = None
		barketData = {}
		#
		with open(oFile, 'w') as outputFile:
			outputFile.write("DateTime,Open,High,Low,Close,Vol,Val\n")
			#outputFile.write("DateTime,Val,Vol,High,Low,Close,Open\n")
			#
			while line:  
				line = inputFile.readline()
				fields = line.split(',')
				if len(fields) < 2:
					break
				
				DateTime = fields[0]
				Open = fields[1]
				High = fields[2]
				Low = fields[3]
				Close = fields[4]
				Vol = fields[5]
				Val = fields[6]
				time = datetime.datetime.strptime(DateTime, '%Y%m%d %H:%M:%S')
				#
				year = time.year
				month = time.month
				day = time.day
				hour = time.hour
				minute = time.minute
				#
				vOpen = float(Open)
				vHigh = float(High)
				vLow = float(Low)
				vClose = float(Close)
				vVol = float(Vol)
				vVal = float(Val)
				#
				if span == 0: # 1day
					barket =  datetime.datetime(year, month, day)
				else :
					barket =  datetime.datetime(year, month, day, hour, minute - minute % span)
				#to5m=True, to15m=True, to30m=True, toH1=True, toD1=True
				if currentBarket != barket:
					# write currentBarket to new csv
					writeToBarket(outputFile, currentBarket, barketData, collection, symbol)
					currentBarket = barket
					barketData = {'Open':vOpen, 'High':vHigh, 'Low':vLow, 'Close':vClose, 'Vol': vVol, 'Val':vVal}
					#
				else:
					# Apply to barket
					barketData['Close'] = vClose
					barketData['Vol'] += vVol
					barketData['Val'] += vVal
					if barketData['High'] < vHigh:
						barketData['High'] = vHigh
					if barketData['Low'] > vLow:
						barketData['Low'] = vLow
				#print barketData
			writeToBarket(outputFile, currentBarket, barketData, collection, symbol)




if __name__ == '__main__':
	

	host, port= "localhost", 27017
	client = pymongo.MongoClient(host, port)    
	backupDir = 'historicalData/TBbackup'

	for span in [0, 5,15,30,60]: #0 代表1天
		print "Span: " + str(span)
		dbName = 'myTestDBName' + str(span)
		symbol = 'hs300'
		collection = client[dbName][symbol]
		collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)   
		
		iFile = backupDir + '/' + 'rb1705_1Min.csv_1M.csv'
		oFile = backupDir + '/' + 'rb1705_1Min.csv_' + str(span) + 'M.csv'
		resample(span, iFile, oFile, collection, symbol)
		



