#!/usr/bin/python

from mrjob.job import *
from bs4 import BeautifulSoup
from selenium import webdriver

import requests, traceback, sqlite3, sys


driver = None

def getDriver(visible=False):
	global driver
	if driver == None:
		if visible:
			return webdriver.Firefox()
		else:
			options = webdriver.firefox.options.Options()
			options.headless = True
			return webdriver.Firefox(options=options)
	else:
		return driver

class ReposScrapper(MRJob):

	#############
	# ATTRIBUTS #
	#############

	repo_name = None
	commits = None
	branches = None
	releases = None
	contributors = None
	issues = None
	pull_requests = None
	watchs = None
	stars = None
	forks = None
	age = None

	###########
	# BUILDER #
	###########

	def __init__(self, repo_name):
		self.repo_name = repo_name

	###########
	# GETTERS #
	###########

	def getRepoName(self):
		return self.repo_name

	def getCommits(self):
		return self.commits

	def getBranches(self):
		return self.branches

	def getReleases(self):
		return self.releases

	def getContributors(self):
		return self.contributors

	def getIssues(self):
		return self.issues

	def getPullRequests(self):
		return self.pull_requests

	def getWatchs(self):
		return self.watchs

	def getStars(self):
		return self.stars

	def getForks(self):
		return self.forks

	def getAge(self):
		return self.age

	###########
	# METHODS #
	###########

	def scrap(self, visible=False):
		self.driver = getDriver(visible)
		url = 'https://github.com/' + repo_name
		dict = self.getDictFromRequests(url)

		if(dict != None):
			while('-1' in dict.values()):
				print(dict)
				dict = self.getDictFromSelenium(url)

			self.c.execute('''INSERT INTO repos (repo_name, commits, branches, releases, contributors, issues, pull_requests, watchs, stars, forks, age)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (repo_name, dict['commits'], dict['branches'], dict['releases'], dict['contributors'], dict['issues'], dict['pull_requests'], dict['watchs'], dict['starts'], dict['forks'], dict['age']))

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
		dict['stars'] = getStars(html)					#7
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
