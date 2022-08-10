import threading
import requests
from bs4 import BeautifulSoup as bs
import time
import re
import sys
import json
sys.path.append('classifier')
import crawler_classifier as classifier
import sqlite3
import time
import pymongo
import pickle
from itertools import cycle
from lxml.html import fromstring


# harvest ratio: rate where relevant webpages were acquired and irrelevant web page discarded
class Crawler(threading.Thread):
	"""docstring for Crawler"""
	def __init__(self,cache, sites,site_type,proxy=False):
		threading.Thread.__init__(self)
		visiteds = cache.loadVisited()
		try:
			with open('all_links.data','rb') as fl:
				# load the visited links too and remove them once and for all, so you can focus on the links that are yet to be visited
				visiteds = set(visiteds)
				temp = set(sites+pickle.load(fl)).difference(visiteds)
				self.links = list(temp)
		except Exception as e:
			print(e)
			self.links = sites
		self.site_type=site_type
		self.cache = cache
		self.database_path = "./data.db"
		self.DBType='mongo'
		proxies =self.get_proxies()
		self.proxy_pool = cycle(proxies)
		self.use_proxy =proxy

		#create a cache for check the page that has already been visited

	def get_proxies(self):
	  url = 'https://free-proxy-list.net/'
	  response = requests.get(url)
	  parser = fromstring(response.text)
	  proxies = set()
	  for i in parser.xpath('//tbody/tr')[:100]:
	    if i.xpath('.//td[7][contains(text(),"yes")]'):
	      #Grabbing IP and corresponding PORT
	      proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
	      proxies.add(proxy)
	  return proxies

	def get_request(self,url):
		try:
			proxy =next(self.proxy_pool)
			result = requests.get(url,proxies={'http':proxy,'https':proxy},timeout=10,verify=False)
			return result

		except Exception as e:
			print(e)
			return False
		

	def make_request(self,url,rotate=True):
		result = self.get_request(url)
		while not result:
			print('retrying : '+url)
			result = self.get_request(url)

		result= result.content
		return result

	def getBlockScore(self,block_text):
		score = classifier.getScore(block_text)
		return score

	def getRankedLinks(self,content_blocks,current_link):
		if not content_blocks:
			return []
		links =[]
		result =[]
		for block in content_blocks:
			text = block.get_text()
			score = self.getBlockScore(text)
			list_items= list(set(block.select('li.dataset-item .dataset-heading a,ul.govuk-list.dgu-topics__list > li a,dgu-results__result a.govuk-link,.subjects a,.panel-body a')))
			all_links= [self.wrapLink(current_link,x.get('href')) for x in list_items if x]
			if not all_links:
				continue
			links.append((all_links,score))
			
		return self.cleanLinks(links)

	def cleanLinks(self, links):
		minVisited =[]
		result=[]
		links = sorted(links,key=lambda x: x[1],reverse=True)
		for lk,score in links:
			for link in lk:
				if link in minVisited:
					continue
				result.append((link,score))
				minVisited.append(link)

		return result

	def start_crawling(self):
		# put this in a loop and pause by few seconds not to overload the server
		while len(self.links) > 0:
			next_score =0.5
			try:
				current_link = self.links.pop(0)
				print('visiting: ',current_link)
				#check if teh page has been visited 
				if self.cache.isVisited(current_link):
					print('already visited', current_link,'\n')
					continue
				content = self.make_request(current_link) if self.use_proxy else requests.get(current_link).content

				htmlContent = bs(content,'html.parser')
				#append link that have next here also
				next_link =self.processPage(htmlContent,current_link)
				# need to find a way to score this part
				content_blocks = htmlContent.select('div,h1,h2,h3,h4,h5,h6,p,address,center,ul,dt,table,th,tr,td')
				to_add = self.getRankedLinks(content_blocks,current_link)
				if next_link:
					to_add.append((next_link,next_score))
				self.cache.addVisited(current_link)
				if to_add:
					self.links+=to_add
				# save the visited links
				#if ther are changes
				if to_add:
					with open('all_links.data','wb') as fl:
						pickle.dump(self.links,fl)
				# time.sleep(2)
			except Exception as e:
				print(e)
				# exit()
				continue

	def processPage(self,content, link):
		#this will basically decide if to save the page or not based on the content of the page
		# check if the page has a new page, then add the new page to the list of pages to be visited
		# convert to text and save with the link as the first thing on the page
		next_link = self.getNextLink(content,link)
		if next_link:
			next_link = self.wrapLink(link,next_link)
		if (not next_link) and self.isRelevantDataPage(content,link):
			self.savePage(link,content)
			# return all_links
		# if next_link:
		# 	all_links.append(next_link)
		# return all_links
		return next_link


	def hasDownloadAction(self,content,link):
		download_links = content.find_all('a',href=re.compile(r'(download)',flags=re.I|re.M))
		if download_links:
			return True
		download_links = content.find_all('span',string=re.compile(r'(download)*',flags=re.I|re.M))
		if download_links:
			return True
		download_links = content.find_all('a',string=re.compile(r'(download)',flags=re.I|re.M))
		if download_links:
			return True
		return False

	def isRelevantDataPage(self,content,link):
		#convert content to text first
		# search for meta information and use the classifier module to determine how relavant the page is
		return classifier.classify(content)
		# threshold =3
		# text = self.extractMainText(content)
		# metaPattern=r'\b(metadata|data\.json|publisher|licen(s|c)e|xls|csv|xlsx|pdf|(\w+ )+:$\b)'
		# total_matched = len(list(re.finditer(metaPattern,text,re.I|re.M)))
		# # check if the page has a download page
		# has_download = self.hasDownloadAction(content,link)
		# if total_matched:
		# 	result = total_matched > threshold and has_download and classifier.isGovernmentData(text,threshold)
		# 	return result
		# return False

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
				CREATE TABLE IF NOT EXISTS document (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,title TEXT NOT NULL,metadata text,page_source TEXT NOT NULL,date_created TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP ,URL TEXT NOT NULL,content TEXT NOT NULL)
			"""
			cur = self.connection.cursor()
			result = cur.execute(table_creation)
			return True
		except Exception as e:
			return False

	def insertPage(self,title,metadata,url,content,html):
		query="INSERT INTO document(title,metadata,url,content,page_source) VALUES(?,?,?,?,?)"
		cursor= self.connection.cursor()
		result = cursor.execute(query,[title,metadata,url,content,html])
		self.connection.commit()
		return result.rowcount


	def savePage(self,link,content):
		print('saving page')
		if self.DBType=='sqllite':
			return self.saveSqllite(link,content)
		elif self.DBType=='mongo':
			return self.saveMongo(link,content)


	def saveMongo(self,link,content):
		# get the the title of the document first
		conn_str ='mongodb://localhost'
		client = pymongo.MongoClient(conn_str,serverSelectionTimeoutMS=5000)
		try:
			db = client['IR_crawled_data']
			collection = db['pages']
			heading=''
			title_element = content.select("h1[itemprop='name'],.column-two-thirds h1,main.container h1")
			if title_element:
				heading = title_element[0].string.strip()
				# now get the meta data
				metadata = self.extractMetadata(link,content)
				text_content = content.get_text()
				html = str(content)
				document ={'title':heading,"metadata":metadata,'link':link,'text':text_content,'raw':html}
				result =collection.insert_one(document)
				print('document inserted')
				# exit()
				return result.inserted_id
		except Exception as e:
			# return 
			print(e)
			# exit()
			return False

	def saveSqllite(self, link, content):
		# get the the title of the document first
		try:
			if not self.connection:
				self.connection = sqlite3.connect(self.database_path)
		except Exception as e:
			self.connection = sqlite3.connect(self.database_path)
		if self.createTables():
			heading=''
			title_element = content.select("h1[itemprop='name'],.column-two-thirds h1")
			if title_element:
				heading = title_element[0].string.strip()
				# now get the meta data
				metadata = self.extractMetadata(link,content)
				text_content = content.get_text()
				html = str(content)
				self.insertPage(heading,metadata,link,text_content,html)
				return True
		return False


	def getMetaText(self,element):
		if not element.find('a'):
			return element.string.strip()
		return element.a.string.strip()

	def getMetadata(self,link,content):
		try:
			maxText=50
			keys = [self.getMetaText(x) for x in content.select('.metadata > dt') if x]
			values = [self.getMetaText(x) for x in content.select('.metadata > dd') if x]
			result = dict(zip(keys,values))
			#get the information abut the summary now
			summary = [x.string for x in content.select('.js-summary > p') if x]
			summary_text=''
			if summary:
				for text in summary:
					if ':' in text:
						temp = text.split(':',maxsplit=1)
						if '\n' in temp[0] or len(temp[0]) > maxText:
							summary_text+=("\n"+text)
							continue
						result[temp[0].strip()]=temp[1].strip()
					else:
						summary_text+=("\n"+text)
				result['summary']=summary_text
			return json.dumps(result)
		except Exception as e:
			return False

	def extractMetadata(self,base,content):
		try:
			metadata_link = content.find('a',string=re.compile(r'\.*(download +metadata)\.*',flags=re.I|re.M))
			if metadata_link:
				current_link = metadata_link.get('href')
				link = self.wrapLink(base,current_link)
				jsonContent = requests.get(link).json()
				result= json.dumps(jsonContent)
				return result
			# what if the metadata does not contain a link to a file
			return self.getMetadata(base,content)
		except Exception as e:
			print(e)
			return False

	def extractMainText(self,content):
		body = content.select('main,article.prose')
		if not body:
			return ''
		text = body[0].get_text()
		return text
		
	def linkFromJSNext(self,link,element):
		gotoText = element.get('onclick')
		temp = re.search(r'\d+',gotoText)
		if not temp:
			return False
		temp = temp.group()
		pat = r'page=\d+'	
		result = re.sub(pat,'page='+temp,link) if re.search(pat,link) else link+'&page='+temp
		return result

	def getNextLink(self,content,link):
		temp = content.select('.pagination li:last-child a')
		if not temp:
			temp = content.find('a',{'rel':'next'})
		if not temp:
			return False
		result = temp[0].get('href')
		if result.strip()=='#':
			return self.linkFromJSNext(link,temp[0])
		return result

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