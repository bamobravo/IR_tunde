from crawler import Crawler
from cache import Cache
import time
import sys
sys.path.append('classifier')
import crawler_classifier as clf
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

english_stopwords = stopwords.words('english')

def tokenize_and_stem(item):
    exclude =' .,-\t\n_#}{\/][;'
    item = item.lower().split()
    left = [x.lower() for x in item if x not in english_stopwords]
    left = [x.strip(exclude) for x in left if x and isinstance(x,str) and x.strip(exclude)]
    ps = PorterStemmer()
    result = [ps.stem(y) for y in left]
    return result
# List of government pages that contains articles that could be used to classify the pages
# https://www.usatoday.com/news/politics/

#ckan.org is a data portal software
sites =['https://search.open.canada.ca/opendata/','https://catalog.data.gov/dataset','https://www.data.gov.uk/search?filters%5Btopic%5D=Government']
# sites =['https://www.data.gov.uk/search?filters%5Btopic%5D=Government']

# sites=['https://open.canada.ca/data/en/dataset/0313f880-492c-4f4e-95ef-f53e4216576d']
# sites =['https://data.gov.uk/search?filters%5Btopic%5D=Mapping&page=12']
# sites = ['https://catalog.data.gov/dataset']
classifier =  clf.Classifier()
caches = []
method ='block'
# method ='bfs'
def startCrawler():
	# create a parralel crawler for each of the websites
	# get information about pages already visited
	# for site in sites:
	# use three threads to make things a bit faster
	for index,x in enumerate(sites):
		suffix = "_"+method+'_'+str(index+1)
		caches.append( Cache(suffix))
		tempThread = Crawler(classifier, caches[index],[x],'site',False, method,suffix)
		tempThread.start()
		# tempThread.join()
		print(f'starting {index}')


startCrawler()

