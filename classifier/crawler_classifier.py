import re


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



		