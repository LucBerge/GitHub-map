#!/usr/bin/python

from mrjob.job import *
from bs4 import BeautifulSoup
from selenium import webdriver
import requests, traceback, sqlite3, sys

class ReposScrapper(MRJob):

	file = "/media/lucas/DATA/Lucas/Etudes/ESISAR 2017-2020/Semestre 4 (Norway)/DAT500 - Data intensive systems/Project/sample/gitHubMap.db"
	driver = None
	db = None
	c = None

	def steps(self):
		return [
			MRStep(mapper=self.mapper1,
				combiner=self.combiner,
				reducer=self.reducer),
			MRStep(mapper_init=self.mapper_init,
				mapper=self.mapper,
				mapper_final=self.mapper_final)
		]

	def mapper1(self, key, repo_name):
		yield repo_name, 1

	def combiner(self, repo_name, number):
		yield repo_name, sum(number)

	def reducer(self, repo_name, number):
		print(repo_name)
		yield repo_name, sum(number)

	def mapper_init(self):
		self.driver = webdriver.Firefox()
		self.db = sqlite3.connect(self.file)
		self.c = self.db.cursor()

	def mapper(self, repo_name, number):
		url = 'https://github.com/' + repo_name
		dict = self.getDictFromRequests(url)

		if(dict != None):
			while('-1' in dict.values()):
				print(dict)
				dict = self.getDictFromSelenium(url)

			self.c.execute('''INSERT INTO repos (repo_name, commits, branches, releases, contributors, issues, pull_requests, watchs, starts, forks, age)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (repo_name, dict['commits'], dict['branches'], dict['releases'], dict['contributors'], dict['issues'], dict['pull_requests'], dict['watchs'], dict['starts'], dict['forks'], dict['age']))

	def mapper_final(self):
		self.driver.close()
		self.db.commit()
		self.db.close()

	def getDictFromRequests(self, url):
		result = requests.get(url)
		if(result.status_code == 200):
			dict = self.getDict(result.content)
			dict['age'] = getAge(url)
			return dict
		else:
			return None

	def getDictFromSelenium(self, url):
		self.driver.get(url)
		self.driver.execute_script("return document.readyState")
		html = self.driver.page_source
		dict = self.getDict(html)
		dict['age'] = getAge(url)
		return dict

	def getDict(self, html):
		html = BeautifulSoup(html, 'html.parser')
		dict = {}
		dict['commits'] = getCommits(html)				#0
		dict['branches'] = getBranches(html)			#1
		dict['releases'] = getReleases(html)			#2
		dict['contributors'] = getContributors(html)	#3
		dict['issues'] = getIssues(html)				#4
		dict['pull_requests'] = getPullRequests(html)	#5
		dict['watchs'] = getWatchs(html)				#6
		dict['starts'] = getStars(html)					#7
		dict['forks'] = getForks(html)					#8
		return dict

#SCRAPPING FUNCTIONS

def getCommits(html):
	try:
		li = html.find_all("li", class_="commits")
		return li[0].a.span.string.strip().replace(',','')
	except IndexError:
		return "0"
	except AttributeError:
		return "-1"

def getBranches(html):
	try:
		svg = html.find_all("svg", class_="octicon-git-branch")
		return svg[0].parent.span.string.strip().replace(',','')
	except IndexError:
		return "0"
	except AttributeError:
		return "-1"

def getReleases(html):
	try:
		svg = html.find_all("svg", class_="octicon-tag")
		return svg[0].parent.span.string.strip().replace(',','')
	except IndexError:
		return "0"
	except AttributeError:
		return "-1"

def getContributors(html):
	try:
		svg = html.find_all("svg", class_="octicon-organization")
		return svg[0].parent.span.string.strip().replace(',','')
	except IndexError:
		return "0"
	except AttributeError:
		return "-1"

def getIssues(html):
	try:
		svg = html.find_all("svg", class_="octicon-issue-opened")
		span = svg[0].parent.find_all("span", class_="Counter")
		return span[0].string.strip().replace(',','')
	except IndexError:
		return "0"
	except AttributeError, ValueError:
		return "-1"

def getPullRequests(html):
	try:
		svg = html.find_all("svg", class_="octicon-git-pull-request")
		span = svg[0].parent.find_all("span", class_="Counter")
		return span[0].string.strip().replace(',','')
	except IndexError:
		return "0"
	except AttributeError, ValueError:
		return "-1"

def getWatchs(html):
	try:
		ul = html.find_all("ul", class_="pagehead-actions")
		a = ul[0].find_all("a", class_="social-count")
		return a[0].string.strip().replace(',','')
	except IndexError:
		return "0"
	except AttributeError:
		return "-1"

def getStars(html):
	try:
		ul = html.find_all("ul", class_="pagehead-actions")
		a = ul[0].find_all("a", class_="social-count")
		return a[1].string.strip().replace(',','')
	except IndexError:
		return "0"
	except AttributeError:
		return "-1"
	
def getForks(html):
	try:
		ul = html.find_all("ul", class_="pagehead-actions")
		a = ul[0].find_all("a", class_="social-count")
		return a[2].string.strip().replace(',','')
	except IndexError:
		return "0"
	except AttributeError:
		return "-1"

def getAge(url):
	try:
		return "0"
	except IndexError:
		return "0"
	except AttributeError:
		return "-1"

if __name__ == '__main__':
	if(len(sys.argv) == 2):
		try:
			ReposScrapper.run()
		except KeyboardInterrupt:
			pass
	else:
		print("Usage 'python " + sys.argv[0].replace('./','') + " repos.txt'")
