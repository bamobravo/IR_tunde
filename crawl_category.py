from bs4 import BeautifulSoup as bs
import requests
import json

requests.packages.urllib3.disable_warnings()

categories={'finance':['https://dmoz-odp.org/Home/Personal_Finance/'],'education':['https://dmoz-odp.org/Reference/Education/'],'health':['https://dmoz-odp.org/Health/'],'Agriculture':['https://dmoz-odp.org/Science/Agriculture/','https://dmoz-odp.org/Business/Agriculture_and_Forestry/'],
	'transport':['https://dmoz-odp.org/Business/Automotive/','https://dmoz-odp.org/Business/Transportation_and_Logistics/'],
	'climate/environment':['https://dmoz-odp.org/Science/Environment/'],
	'technology':['https://dmoz-odp.org/Science/Technology/']
}

links=set()
data =[]
visited=[]
domain ='https://dmoz-odp.org'
def getLink(base,item):
	temp = item.get('href')
	return domain+temp;

def isMember(base,item):
	temp=base.replace(domain,'')
	link = item.get('href')
	result= temp in link
	return result

def getLinkTitleAndDescription(categories):
	links ={}
	for category in categories:
		categoryLists = categories[category]
		visited =[]
		counter=0;
		tempList=[]
		print('crawling category ',category,'\n\n\n')
		while counter < len(categoryLists):
			item = categoryLists[counter];
			temp = requests.get(item,verify=False);
			if item in visited:
				print(item,' already visited...skipping')
				counter+=1
				continue
			content = temp.content;
			html= bs(content,'html.parser')
			categoryLinks = html.select("#subcategories-div a")
			ctLink = [getLink(item,x) for x in categoryLinks if isMember(item,x)]
			categoryLists+=ctLink

			itemsLinks = html.select('.site-item')
			for lks in itemsLinks:
				url = lks.select('.title-and-desc > a')[0].get('href').strip()
				title = lks.select('.title-and-desc .site-title')[0].get_text().strip()
				desc = lks.select('.title-and-desc > .site-descr')[0].get_text().strip()
				tempList.append({'link':url,'title':title,'description':desc})
			counter+=1
			print(category,': completed ',counter,'/',len(categoryLists))
			visited.append(item)
		links[category]=tempList
		print('completed category ',category,'\n\n\n')
	return links
			
def downloadContent(links):
	for i in range(len(links)):
		print('processing ',i,'/',len(links))
		item=links[i]
		temp = requests.get(item,verify=False);

links = getLinkTitleAndDescription(categories)
with open('dmoz_links.json','w') as fl:
	json.dump(links,fl)
print('success!')
