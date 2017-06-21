#encoding: utf-8


'''
Form
"Date","Time","Open","High","Low","Close","TotalVolume"
2010-04-16,09:16:00,3450.0,3488.0,3450.0,3468.0,489
'''

'''
To
20100416,0.0916,3450.0,3488.0,3450.0,3468.0,489,30000
'''
import csv

with open('IF0000_1min.csv') as file:
	with open('IF0000_1min_tb.csv', 'w') as ofile:
		writer = csv.writer(ofile)

		file.readline()   #读掉第一行,下次再引用file的时候,将file的文件指针指向第二行开始的文件.
		reader = csv.reader(file)
		for Date , Time , Open , High , Low , Close , TotalVolume in reader:
			writer.writerow([Date.replace('-','') , "0." + Time[:-3].replace(":", "") , Open , High , Low , Close , TotalVolume, TotalVolume])

