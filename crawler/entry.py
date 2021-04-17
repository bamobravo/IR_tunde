from crawler import Crawler
from cache import Cache

#ckan.org is a data portal software
sites =['https://catalog.data.gov/dataset','https://data.gov.uk']
# sites =['https://data.gov.uk/search?filters%5Btopic%5D=Mapping&page=12']
visitedData = Cache()
def startCrawler():
	# create a parralel crawler for each of the websites
	# get information about pages already visited
	for site in sites:
		Crawler(visitedData,site,site).start()


startCrawler()