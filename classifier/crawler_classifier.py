import re
import json
import csv
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle
import os
from sklearn.metrics.pairwise import cosine_similarity

category_vectors = {}
category_models ={}

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
	header = data.pop(0)
	train, test = train_test_split(data,test_size=0.2,random_state=30)
	name = filename.split('.',maxsplit=2)
	name = name[0]
	testname=name+'_test.csv'
	trainname = name+'_train.csv'
	train_pd = pd.DataFrame(train)
	train_pd.to_csv(trainname,header=header,index=False)
	test_pd = pd.DataFrame(test)
	test_pd.to_csv(testname,header=header,index=False)
	print('data splitted')
	exit()

def read_csv(filename):
	result=[]
	with open(filename,'r') as fl:
		reader= csv.reader(fl)
		for row in reader:
			result.append(list(row))
	return result

def buildVectors():
	filePath = 'dmoz_links_train.csv'
	all_data = pd.read_csv(filePath)
	all_data['all_text']=all_data['title'].str.cat(all_data['description'],sep=' ')
	categories= all_data['category'].unique()
	for category in categories:
		model,text = getVectors(all_data,category)
		# now save the vector object for classification later
		saveVector(model,category,text)

def saveVector(model,category,text):
	category = category.replace('/','_')
	with open('models/'+category+'_model.bat','wb') as fl:
		pickle.dump(model, fl)
	with open('models/vectors/'+category+'_model.vec','wb') as fl:
		temp = model.transform(text)
		pickle.dump(temp,fl)
	print(category,' saved')

def getVectors(all_data,category):
	data = all_data[all_data['category']==category]
	doc = [x for x in data['all_text'].values.tolist() if x and isinstance(x,str) and x.strip()]
	model = TfidfVectorizer()
	result = model.fit(doc)
	return result,doc

def getScore(text,classify=False,threshold=False):
	global category_vectors
	global category_models

	if len(category_vectors)==0:
		loadAllCategoryVector();

	if len(category_models) == 0:
		loadAllModels()

	maxScore = 0
	folder='models/'
	files = os.listdir(folder)
	result=[]
	for category in category_models:
		try:
			vector = item.transform([text])
			other = loadCategoryVector(category)
			score = getSimilarityScore(vector,other)
			if classify and threshold and score >= threshold:
				return True
			if score > maxScore:
				maxScore = score
		except Exception as e:
				continue
	if classify:
		return False
	return maxScore

def loadAllModels():
	global category_models
	directory = "models/"
	for f in os.listdir(directory):
		if os.path.isdir(directory+f):
			continue
		key = f.replace("_model.bat",'')
		with open(directory+f,'rb') as fl:
			temp = pickle.load(fl)
		category_models[key]=temp


def loadAllCategoryVector():
	global category_vectors
	directory = "models/vectors/"
	for f in os.listdir(directory):
		key = f.replace("_model.vec",'')
		with open(directory+f,'rb') as fl:
			temp = pickle.load(fl)
		category_vectors[key]=temp

def loadCategoryVector(category):
	global category_vectors
	return category_vectors[category]
	# folder = 'models/vectors/'+category+"_model.vec"
	# with open(folder,'rb') as fl:
	# 	result = pickle.load(fl)
	# 	return result

def getSimilarityScore(first, second):
	result = cosine_similarity(first,second)
	result = result[0]
	return max(result)
	# avg = sum(result)/len(result)
	# return avg

def classify(text):
	'''
		This will return true or false based on the threshold used
	'''
	threshold=0.5
	if isinstance(text,str):
		return getScore(text,classify=True, threshold=threshold)
		# return result >= threshold
	result=[]
	for t in text:
		temp = getScore(t,classify=True, threshold=threshold)
		result.append(temp)
	return result

def getTestScores():
	filePath = 'dmoz_links_test.csv'
	all_data = pd.read_csv(filePath)
	all_data['all_text']=all_data['title'].str.cat(all_data['description'],sep=' ')
	data = all_data['all_text'].values.tolist()
	data = data[:10]
	result = classify(data)
	return result

# jsonFile ='dmoz_links.json'
# outputfile ='dmoz_links.csv'
# splitTestTrain(outputfile)
# convertToCSV(jsonFile,outputfile)	
# data = pd.read_csv(outputfile)
# data.heads()
# print(getTestScores())