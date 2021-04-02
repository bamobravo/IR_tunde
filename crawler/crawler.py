import threading
import requests
from bs4 import BeautifulSoup as bs
import time
import re
import sys
sys.path.append('classifier')
import crawler_classifier as classifier
import sqlite3

class Crawler(threading.Thread):
	"""docstring for Crawler"""
	def __init__(self,cache, site,site_type):
		threading.Thread.__init__(self)
		self.links = [site]
		self.site_type=site_type
		self.cache = cache
		self.database_path = "./data/data.db"

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
		return all_links.append(next_link)


	def isRelevantDataPage(self,content):
		#convert content to text first
		# search for meta information and use the classifier module to determine how relavant the page is
		text = self.extractMainText(content)
		metaPattern=r'\b(metadata|data\.json|publisher|licen(s|c)e|xls|csv|xlsx|(\w+ )+:$\b)'
		total_matched = len(list(re.finditer(metaPattern,text,re.I|re.M)))
		# check if the page has a download page
		download_links = content.find_all('a',href=re.compile(r'\.*(download)\.*',flags=re.I|re.M))
		if total_matched:
			return total_matched > 10 and download_links and classifier.isGovernmentData(text)
		return False
	
	def tablesExists(self):
		table_check="SELECT name FROM sqlite_master WHERE type='table'"
		cur = self.connection.cursor()
		cur.execute(table_check)
		result = cur.fetchone()
		return result

	def createTables(self):
		try:
			if self.tablesExists():
				return True
			# document should one of the tables
			table_creation="""
				CREATE TABLE IF NOT EXISTS document (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,title TEXT NOT NULL,metadata text,date_created TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP ,URL TEXT NOT NULL,content TEXT NOT NULL)
			"""
			cur = self.connection.cursor()
			result = cur.execute(table_creation)
			return True
		except Exception as e:
			return False

	def savePage(self,content):
		# get the the title of the document first
		try:
			if not self.connection:
				self.connection = sqlite3.connect(self.database_path)
		except Exception as e:
			self.connection = sqlite3.connect(self.database_path)
		if self.createTables():
			heading=''
			title_element = content.select('.module-content > h1,.column-two-thirds h1')
			if title_element:
				print(title_element)
				heading = title_element[0].get_text()
				print(heading)
				exit()

	def extractMainText(self,content):
		body = content.select('main,article.prose')
		if not body:
			return ''
		text = body[0].get_text()
		return text
		
	def getNextLink(self,content):
		temp = content.select('.pagination li:last-child a,.dgu-pagination__numbers > a')
		if not temp:
			return False
		return temp[0].get('href')

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