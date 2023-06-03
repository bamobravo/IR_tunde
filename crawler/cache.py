import json
import os

class Cache:
	"""docstring for Cache"""
	def __init__(self,suffix='',saveRate=1):
		self.path = f'visited{suffix}.visited'
		self.saveRate = saveRate
		self.visited = self.loadVisited()
		self.new_added = False
		self.dirty = False

	def loadVisited(self):
		try:
			if not os.path.exists(self.path):
				return []
			with open(self.path,'r') as fl:
				result = json.load(fl)
				return result
		except Exception as e:
			return []
			print(e)
		
	
	def saveVisited(self):
		if self.visited:
			with open(self.path,'w') as fl:
				json.dump(self.visited, fl)


	def addVisited(self,link):
		self.visited.append(link)
		if (len(self.visited) % self.saveRate==0):
			self.saveVisited()
		self.dirty= True

	def isVisited(self,link):
		return link in self.visited