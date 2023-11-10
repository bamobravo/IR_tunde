import time
from datetime import datetime
import os

class Log:
	def __init__(self, filename:str, *headers:list[str], datemarkfilename=True):

		directory ='log/'
		self.filename = filename 
		self.filename = directory+filename
		self.headers = headers
		
		with open(self.filename, 'a') as logfile:
			content = ",".join(self.headers) + "\n" if os.path.exists(filename) and len(logfile.read().strip()) == 0 else ''
			logfile.write(content)


	def enter(self, *row:list[str], ):
		"""Add a new entry into log file"""
		line = ",".join(row) + '\n'
		with open(self.filename, 'a') as logfile:
			logfile.write(line)

	def get_saved_count(self):
		with open(self.filename,'r') as fl:
			texts = fl.read()
			return len(texts.strip().split('\n'))
