from crawler import Crawler


sites =['ckan','data.gov','data.gov.uk']


def startCrawler():
	# create a parralel crawler for each of the websites
	for site in sites:
		Crawler(site,site).start()


startCrawler()