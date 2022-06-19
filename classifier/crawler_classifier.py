import re
import json
import csv
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

def getKeywords():
	excludeWords = 'the is of for and new health'.split()
	excludePattern =r'\b('+'|'.join(excludeWords)+r')\b'
	keywords ='government department revenue tax(es)? HM foreign affairs trade world agreement procurement policies organization organisation international patents trademarks registry bond trading culture program relation business british african american astralian grants scheme intervention intervene regulations endorsement enforcement govern freedon franchise capital civil citizen citizenship control aides anthem ammendment law leader legacy parliament independence judgement justice judicial legislate liberty military militia nation national parties political politics security social ministry arms veteran vote voter war welfare'.split()
	with open('countries.txt','r') as fl:
		keywords+=fl.read().split()
	result = set([x.strip() for x in keywords if x.strip() and not re.match(excludePattern,x,re.I)])
	return list(result)


def isGovernmentData(text,threshold):
	keywords = getKeywords()
	pattern = r'\b'+('|'.join(keywords))+r'\b'
	total_matched = [x.group().strip() for x in re.finditer(pattern,text,re.I|re.M) if x.group().strip()]
	result= len(total_matched)> threshold
	return result

def addCategory(item, category):
	item['category']=category
	return item

def convertToCSV(inputfile,outputfile):
	header =['link','title','description','category']
	allData=[]
	with open(inputfile,'r') as fl:
		jsonData = json.load(fl)
		for category in jsonData:
			data = [addCategory(x,category) for x in jsonData[category]]
			allData+=data
	with open(outputfile,'w') as fl:
		writer = csv.DictWriter(fl, fieldnames=header)
		writer.writeheader()
		writer.writerows(allData)

def splitTestTrain(filename):
	data = read_csv(filename)
	train, test = train_test_split(data,test_size=0.2,random_state=30)
	name = filename.split('.',maxsplit=2)
	name = name[0]
	testname=name+'_test.csv'
	trainname = name+'_train.csv'
	train_pd = pd.DataFrame(train)
	train_pd.to_csv(trainname)
	test_pd = pd.DataFrame(test)
	test_pd.to_csv(testname)
	print('data splitted')
	exit()

def read_csv(filename):
	result=[]
	with open(filename,'r') as fl:
		reader= csv.reader(fl)
		for row in reader:
			result.append(list(row))
	return result

# jsonFile ='dmoz_links.json'
outputfile ='dmoz_links.csv'
splitTestTrain(outputfile)
# convertToCSV(jsonFile,outputfile)	
# data = pd.read_csv(outputfile)
# data.heads()