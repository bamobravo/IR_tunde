from crawler import Crawler
from cache import Cache
import time
import sys
sys.path.append('classifier')
import crawler_classifier as clf

# List of government pages that contains articles that could be used to classify the pages
# https://www.usatoday.com/news/politics/

#ckan.org is a data portal software
sites =['https://open.canada.ca/en/open-data','https://catalog.data.gov/dataset','https://data.gov.uk']

# sites=['https://open.canada.ca/data/en/dataset/0313f880-492c-4f4e-95ef-f53e4216576d']
# sites =['https://data.gov.uk/search?filters%5Btopic%5D=Mapping&page=12']
visitedData = Cache()
classifier =  clf.Classifier()
method ='bfs'
def startCrawler():
	# create a parralel crawler for each of the websites
	# get information about pages already visited
	# for site in sites:
	# use three threads to make things a bit faster
	for index,x in enumerate(sites):

		Crawler(classifier, visitedData,[x],'site',False, method,"_"+method+'_'+str(index+1)).start()


startCrawler()