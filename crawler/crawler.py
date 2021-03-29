import threading
import requests
from bs4 import BeautifulSoup as bs
import time
import re

class Crawler(threading.Thread):
	"""docstring for Crawler"""
	def __init__(self,cache, site,site_type):
		threading.Thread.__init__(self)
		self.links = [site]
		self.site_type=site_type
		self.cache = cache

		#create a cache for check the page that has already been visited

	def start_crawling(self):
		# put this in a loop and pause by few seconds not to overload the server
		current_link = self.links.pop(0)
		content = requests.get(current_link).content
		htmlContent = bs(content,'html.parser')
		#append link that have next here also
		list_items= htmlContent.select('li.dataset-item a,ul.govuk-list.dgu-topics__list > li a')
		all_links= [self.wrapLink(current_link,x.get('href')) for x in list_items]
		self.processPage(htmlContent,current_link,all_links)
		self.cache.addVisited(current_link)

	def processPage(self,content, link,all_links):
		#tthis will basically decide if to save the page or not based on the content of the page
		# check if the page has a new page, then add the new page to the list of pages to be visited
		# convert to text and save with the link as the first thing on the page
		next_link = self.getNextLink(content)
		if (not next_link) and self.isRelevantDataPage(content):
			self.savePage(content)
			return all_links
		return all_links+next_link


	def isRelevantDataPage(self,content):
		#convert content to text first
		# search for meta information and use the classifier module to determine how relavant the page is
		text = content.get_text()

	def function(self,content):
		text = content.get_text()
		
	def getNextLink(self,content):
		temp = content.select('.pagination li:last-child a,.dgu-pagination__numbers > a')
		if not temp:
			return False
		return self.temp.get('href')

	def wrapLink(self,current_link,link):
		pattern =r'(https?://|www\.)[a-z0-9.\-_]+'
		if re.match(pattern,link,re.I):
			return link
		base = re.match(pattern,current_link,re.I)
		base =base.group()
		if not base:
			return link
		result = (base+link) if link[0]=='/' else (base+"/"+link)
		return result

	def run(self):
		self.start_crawling()