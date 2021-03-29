from crawler import Crawler
from cache import Cache

#ckan.org is a data portal software
sites =['https://data.gov','https://data.gov.uk','https://data.gov.uk/search?filters%5Btopic%5D=Business+and+economy','https://catalog.data.gov/dataset']

visitedData = Cache()
def startCrawler():
	# create a parralel crawler for each of the websites
	# get information about pages already visited
	for site in sites:
		Crawler(visitedData,site,site).start()


startCrawler()