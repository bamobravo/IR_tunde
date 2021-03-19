from crawler import Crawler
from cache import Cache


sites =['https://ckan.org','https://data.gov','https://data.gov.uk']

visitedData = Cache()
def startCrawler():
	# create a parralel crawler for each of the websites
	# get information about pages already visited
	for site in sites:
		Crawler(visitedData,site,site).start()


startCrawler()