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


# harvest ratio: rate where relevant webpages were acquired and irrelevant web page discarded
class Crawler(threading.Thread):
	"""docstring for Crawler"""
	def __init__(self,cache, site,site_type):
		threading.Thread.__init__(self)
		try:
			with open('all_links.data','r') as fl:
				self.links = pickle.load(fl)
		except Exception as e:
			self.links = [site]
		self.site_type=site_type
		self.cache = cache
		self.database_path = "./data.db"
		self.DBType='mongo'

		#create a cache for check the page that has already been visited

	def start_crawling(self):
		# put this in a loop and pause by few seconds not to overload the server
		while len(self.links)>0:
			try:
				current_link = self.links.pop(0)
				print('visiting: ',current_link)
				#check if teh page has been visited 
				if self.cache.isVisited(current_link):
					print('already visited', current_link,'\n')
					continue
				content = requests.get(current_link).content
				htmlContent = bs(content,'html.parser')
				#append link that have next here also
				list_items= htmlContent.select('li.dataset-item .dataset-heading a,ul.govuk-list.dgu-topics__list > li a,dgu-results__result a.govuk-link')
				all_links= [self.wrapLink(current_link,x.get('href')) for x in list_items if x]
				to_add =self.processPage(htmlContent,current_link,all_links)
				if to_add:
					self.cache.addVisited(current_link)
					self.links+=to_add
				# save the visited links
				if to_add:
					with open('all_links.data','wb') as fl:
						pickle.dump(self.links,fl)
				time.sleep(2)
			except Exception as e:
				continue

	def processPage(self,content, link,all_links):
		#this will basically decide if to save the page or not based on the content of the page
		# check if the page has a new page, then add the new page to the list of pages to be visited
		# convert to text and save with the link as the first thing on the page
		next_link = self.getNextLink(content)
		if next_link:
			next_link = self.wrapLink(link,next_link)
		if (not next_link) and self.isRelevantDataPage(content,link):
			self.savePage(link,content)
			return all_links
		if next_link:
			all_links.append(next_link)
		return all_links


	def hasDownloadAction(self,content,link):
		download_links = content.find_all('a',href=re.compile(r'\.*(download)\.*',flags=re.I|re.M))
		if download_links:
			return True
		download_links = content.find_all('span',string=re.compile(r'\.*(download)\.*',flags=re.I|re.M))
		if download_links:
			return True
		return False

	def isRelevantDataPage(self,content,link):
		#convert content to text first
		# search for meta information and use the classifier module to determine how relavant the page is

		threshold =3
		text = self.extractMainText(content)
		metaPattern=r'\b(metadata|data\.json|publisher|licen(s|c)e|xls|csv|xlsx|(\w+ )+:$\b)'
		total_matched = len(list(re.finditer(metaPattern,text,re.I|re.M)))
		# check if the page has a download page
		has_download = self.hasDownloadAction(content,link)
		if total_matched:
			result = total_matched > threshold and has_download and classifier.isGovernmentData(text,threshold)
			print(result)
			return result
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
			title_element = content.select("h1[itemprop='name'],.column-two-thirds h1")
			if title_element:
				heading = title_element[0].string.strip()
				# now get the meta data
				metadata = self.extractMetadata(link,content)
				text_content = content.get_text()
				html = str(content)
				document ={'title':heading,"metadata":metadata,'link':link,'text':text_content,'raw':html}
				result =collection.insert_one(document)
				print('document inserted')
				exit()
				return result.inserted_id
		except Exception as e:
			print(e)
			exit()
			raise e

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
		
	def getNextLink(self,content):
		temp = content.select('.pagination li:last-child a')
		if not temp:
			temp = content.find('a',{'rel':'next'})
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