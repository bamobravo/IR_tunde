import time
from datetime import datetime
import os

class Log:
	def __init__(self, filename:str, *headers:list[str], datemarkfilename=True):

		directory ='log/'
		self.filename = filename 
		self.filename = directory+filename
		self.headers = headers
		
		fileExists = os.path.exists(self.filename)
		existAndHasContent = False
		if fileExists:
			with open(self.filename,'r') as fl:
				txt = fl.read().strip()
				existAndHasContent = len(txt) > 10



		with open(self.filename, 'a') as logfile:
			content = '' if existAndHasContent else ",".join(self.headers) + "\n"
			logfile.write(content)


	def enter(self, *row:list[str], ):
		"""Add a new entry into log file"""
		line = ",".join(row) + '\n'
		with open(self.filename, 'a') as logfile:
			logfile.write(line)

	def enter_count(self, count, category_count, virtual_web_len,target_len,time):
		# line = ",".join(row) + '\n'
		lines = [','.join([str(count),str(category_count[x]),x, str(virtual_web_len),str(target_len), time]) for x in category_count]
		with open(self.filename, 'a') as logfile:
			logfile.write('\n'.join(lines)+'\n')

	def get_crawled_count(self):
		with open(self.filename,'r') as fl:
			text = fl.read().strip()
			if not text or len(text.split('\n')) < 2:
				return 0
			rows = text.split('\n')
			return int(rows[-1].split(',')[0])

	def get_saved_count(self, categories):
		result = {x:0 for x in categories}
		with open(self.filename,'r') as fl:
			texts = fl.read().strip()
			if not texts or len(texts.split('\n')) < 2:
				return result
			rows = texts.strip().split('\n')
			for row in rows[-len(categories):]:
				rl = row.split(',')
				cat = rl[2]
				if not cat in categories:
					continue
				result[cat]=int(rl[1])
		return result
	
	def get_last_target_len(self):
		with open(self.filename,'r') as fl:
			text = fl.read().strip()
			if not text or len(text.split('\n')) < 2:
				return 0
			rows = text.split('\n')
			return float(rows[-1].split(',')[-2])
