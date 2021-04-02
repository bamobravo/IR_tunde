from crawler import Crawler
from cache import Cache

#ckan.org is a data portal software
sites =['https://data.gov.uk/dataset/0c7e33f1-9fbe-453b-81b9-00245557f97d/gift-aid-repayments-to-charities','https://catalog.data.gov/dataset/department-for-the-aging-dfta-geriatric-mental-health-contracted-providers','https://data.gov','https://data.gov.uk','https://catalog.data.gov/dataset']

visitedData = Cache()
def startCrawler():
	# create a parralel crawler for each of the websites
	# get information about pages already visited
	for site in sites:
		Crawler(visitedData,site,site).start()


startCrawler()