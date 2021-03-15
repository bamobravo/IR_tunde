import threading
import requests
from bs4 import BeautifulSoup as bs

class Crawler(threading.Thread):
	"""docstring for Crawler"""
	def __init__(self, site,site_type):
		threading.Thread.__init__(self)
		self.site = site
		self.site_type=site_type

		#create a cache for check the page that has already been visited

	def start_crawling(self):
		print('I am crawling now')

	def run(self):
		self.start_crawling()