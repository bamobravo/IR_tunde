import threading
import requests
from bs4 import BeautifulSoup as bs
import time
import re
import sys
import json
import sqlite3
import time
import pymongo
import pickle
from itertools import cycle
from lxml.html import fromstring
from logger import Log
from datetime import datetime




# harvest ratio: rate where relevant webpages were acquired and irrelevant web page discarded
class Crawler(threading.Thread):
	"""
		docstring for Crawler
		The method can either be bfs or block. Where block is the enhanced algorithm and bfs is breadth first search
	"""
	
	def __init__(self, classifier,cache, sites,site_type,proxy=False,method='bfs',suffix=''):
		self.crawled_count = 0
		self.saved_count = 0
		self.default_score = 0.01
		start = time.time()
		threading.Thread.__init__(self)
		visiteds = cache.loadVisited()
		self.class_selector ='li.dataset-item .dataset-heading a,ul.govuk-list.dgu-topics__list > li a,.dgu-results__result .govuk-link,.subjects a,.panel-body a, td.views-field a,.mrgn-bttm-xl a'
		# exit()
		self.method = method
		self.classifier = classifier
		self.all_link_path =f'all_links{suffix}.data'
		self.save_interval = 10
		try:
			with open(self.all_link_path,'rb') as fl:
				# load the visited links too and remove them once and for all, so you can focus on the links that are yet to be visited
				# for block the sites should be a tuple and not a list 
				to_add_sites = [ (x,self.default_score) for x in sites] if self.method else sites
				visiteds = set(visiteds)
				temp = set(to_add_sites+pickle.load(fl)).difference(visiteds)
				self.links = list(temp)
		except Exception as e:
			print(e)
			self.links = sites
		if self.method =='block':
			self.links = [x if isinstance(x, tuple) else (x,self.default_score) for x in self.links]
			self.links = sorted(self.links,key=lambda x: float(x[1]),reverse=True)

		self.site_type = site_type
		self.cache = cache
		self.database_path = "./data.db"
		self.DBType='mongo'
		proxies =self.get_proxies()
		self.proxy_pool = cycle(proxies)
		self.use_proxy =proxy

		self.log = Log(f'in_crawlinglog{suffix}.csv', 'url','timestamp')
		self.log2 = Log(f'in_processlog{suffix}.csv', 'url','category', 'timestamp')
		self.log3 = Log(f'metrics_log{suffix}.csv', 'crawled_count','saved_count', 'timestamp')
		self.crawled_count += self.log.get_saved_count()
		self.saved_count += self.log2.get_saved_count()
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
			# raise e
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
		block_text = re.sub(r'[\n \t]+',' ', block_text)
		score, category = self.classifier.getNewScore(block_text)
		return score

	def getRankedLinks(self,content_blocks,current_link):
		if not content_blocks:
			return []
		links =[]
		result =[]
		for block in content_blocks:
			text = block.get_text()
			# score_time = time.time()
			score = self.getBlockScore(text)
			# print('scoring time: ', time.time() - score_time)
			list_items= block.select(self.class_selector)
			# list_items= list(set(block.select('li.dataset-item .dataset-heading a,ul.govuk-list.dgu-topics__list > li a,.dgu-results__result .govuk-link,.subjects a,.panel-body a')))
			# wrap_time = time.time()
			all_links= [self.wrapLink(current_link,x.get('href')) for x in list_items if x]
			# print('time to perform wrapping: ', time.time() - wrap_time)
			if not all_links:
				continue
			links.append((all_links,score))
		# clean_time = time.time()
		result = self.cleanLinks(links)
		# print('cleaning time: ', time.time() - clean_time)
		return result

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

	def selector(self,tag):
		items =['div','h1','h2','h3','h4','h5','h6','p','address','center','ul','dt','table','tr','section']
		return tag.name in items and tag.find('a')

	def start_crawling(self):
		# put this in a loop and pause by few seconds not to overload the server
		while len(self.links) > 0:
			next_score = self.default_score
			try:
				self.current_link = self.links.pop(0)
				if isinstance(self.current_link,tuple):
					self.current_link= self.current_link[0]
				print('visiting: ',self.current_link)

				if self.cache.isVisited(self.current_link):
					print('already visited', self.current_link,'\n')
					continue
				# print('done checking visited: ',time.time() - visit_start)
				# request_start = time.time()
				content = self.make_request(self.current_link) if self.use_proxy else requests.get(self.current_link).content

				htmlContent = bs(content,'html.parser')
				# print('time between network request:', time.time() - start)
				to_add=[]
				
				# print('done making request: ', time.time() - request_start)

				if self.method=='bfs':
					list_items= htmlContent.select(self.class_selector)
					self.all_links = [self.wrapLink(self.current_link, x.get('href')) for x in list_items if x]
					# extract other links from current page also
					to_add = self.processPage(htmlContent,self.current_link,self.all_links)
				else:
					# process_start = time.time()
					self.next_link = self.processPage(htmlContent,self.current_link,[])
					content_blocks = htmlContent.find_all(self.selector)
					to_add = self.getRankedLinks(content_blocks,self.current_link)
					# print('time taken to rank: ', time.time() - rank_time)
					if self.next_link:
						# insert_index_time = time.time()
						insertIndex = self.getInsertIndex(next_score, to_add)
						# print('time taken to fetch insert index: ',time.time() -insert_index_time )
						items = self.next_link if isinstance(self.next_link,list) else [self.next_link]
						# insert_time = time.time()
						for itm in items:
							to_add.insert(insertIndex,(itm,next_score))
					# print('time taken to insert: ', time.time() - insert_time)

				if to_add:
					
					# the link should be sorted if it is not bfs

					if self.method=='block':
						# mer_start = time.time()
						self.links = self.mergeSorted(self.links,to_add)
						# print('time to merge sort: ', time.time() - mer_start)

					else:
						self.links+=to_add
				self.crawled_count+=1
				can_log =  self.crawled_count > 0 and  self.crawled_count % self.save_interval == 0
				if can_log:
					with open(self.all_link_path,'wb') as fl:
						pickle.dump(self.links,fl)
				# print('done saving : ',time.time() - save_time)
				# exit()
				print('adding visited')
				self.cache.addVisited(self.current_link)
				self.log.enter(self.current_link, str(time.time_ns()))
				
				if can_log:
					self.log3.enter(str(self.crawled_count), str(self.saved_count), str(time.time_ns()))
				# print('done saving visit and login')
				# time.sleep(2)
			except Exception as e:
				raise e
				print(e)
				# exit()
				continue
		print('process completed')

	def mergeSorted(self,first, second):
		# the sort should be done in the reverse order
		if len(first) == 0:
			return second

		if len(second)==0:
			return first

		if first[-1][1] >= second[0][1]:
			return first + second

		if second[-1][1] >= first[0][1]:
			return second + first

		current_index =0
		for item in second:
			# look first the best index to place the item in the first one
			# for i in range(current_index,len(first)):
			while current_index < len(first):
				if item[1] > first[current_index][1]:
					first.insert(current_index,item)
					current_index+=2
					continue
				current_index+=1
			first.append(item)
			current_index = len(first)

		return first


	def getInsertIndex(self,score, items):
		if not items:
			return -1

		if score > items[0][1]:
			return 0
		if score < items[-1][1]:
			return -1
		n= len(items)
		for i in range(n):
			index = n - (i +1)
			if score > items[index][1]:
				return index +1

	def processPage(self,content, link, all_links=[]):
		#this will basically decide if to save the page or not based on the content of the page
		# check if the page has a new page, then add the new page to the list of pages to be visited
		# convert to text and save with the link as the first thing on the page
		# link is the current_link

		next_link = self.getNextLink(content,link)
		if next_link:
			next_link = self.wrapLink(link,next_link)
			# if all_links:
			all_links.append(next_link)

		if not next_link:
			status, category = self.isRelevantDataPage(content,link)
			# print(status, category)

			if status:
				self.log2.enter(link,category, str(time.time_ns()))
				self.savePage(category,link, content)
				self.saved_count+=1

		# result = all_links 
		return all_links


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
		text = content.get_text()
		result = self.classifier.newClassify(text)
		return result
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


	def savePage(self,category,link,content):
		# print('saving page')
		if self.DBType=='sqllite':
			return self.saveSqllite(link,category,content)
		elif self.DBType=='mongo':
			return self.saveMongo(link,category,content)


	def saveMongo(self,link,category,content):
		# get the the title of the document first
		# conn_str ='mongodb://localhost'
		# conn_str = "mongodb+srv://tunlamania:oloriebi@cluster0.gipgzjs.mongodb.net/?retryWrites=true&w=majority"
		conn_str = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.10.6"
		client = pymongo.MongoClient(conn_str,serverSelectionTimeoutMS=5000)
		try:
			db = client['IR_crawled_data_block']
			collection = db['pages']
			heading=''
			title_element = content.select("h1[itemprop='name'],.column-two-thirds h1,main.container h1")
			if title_element:
				heading = title_element[0].string.strip()
				# now get the meta data
				metadata = self.extractMetadata(link,content)
				text_content = content.get_text()
				html = str(content)
				document ={'time':datetime.now().isoformat(), 'title':heading, "category":category, "metadata":metadata, 'link':link, 'text':text_content, 'raw':html}
				result =collection.insert_one(document)
				print('document inserted')
				# exit()
				return result.inserted_id
		except Exception as e:
			# raise e
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
			# raise e
			print(e)
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
			# raise e
			# print(e)
			# print('na meta')
			return False

	def extractMainText(self,content):
		body = content.select('main, article.prose')
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
		result = re.sub(pat,'page='+temp,link) if re.search(pat,link) else link+'?page='+temp
		return result

	def getNextLink(self,content,link):
		temp = content.find('a',{'rel':'next'})
		if not temp:
			temp = content.find('a', string='»')
		if not temp:
			temp = content.select('.pagination li:last-child a, li.next a')

		if not temp:
			return False
		result = (temp[0] if isinstance(temp, list) else temp).get('href')
		if result.strip()=='#':
			return self.linkFromJSNext(link,temp[0])
		return result

	def wrapLink(self,current_link,link):
		pattern =r'^((https?://(www\.)?)\w+(\.\w+){0,2}\.(com|org|gov|uk|ca|edu|net))'
		mtc = re.match(pattern,link,re.I)
		if mtc:
			return link
		basePattern = re.match(pattern,current_link,re.I)
		if not basePattern:
			print('there is a problem here, I will be stopping for inspection')
			exit()
		base = basePattern.group()

		# temp = current_link.rsplit('[^/]/[^/]',maxsplit=1)
		# base = temp[0]

		result = (base+link) if link[0]=='/' else (base+"/"+link)
		# print(f'the resulting link from {link} is {result} and current link is {current_link}')
		# exit()
		return result


	def run(self):
		self.start_crawling()
