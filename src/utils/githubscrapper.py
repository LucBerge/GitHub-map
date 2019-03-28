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

class CannotScrapRepoException(Exception):
	pass

class PageNotLoadedException(Exception):
	pass

class RepoScrapper():

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
		url = 'https://github.com/' + self.repo_name

		try:
			self.getValuesFromRequests(self.repo_name, url)
		except PageNotLoadedException:
			exit = False
			driver = getDriver(visible)
			while(not exit):
				try:
					self.getValuesFromSelenium(driver, url, visible)
					exit = True
				except PageNotLoadedException:
					pass
			driver.close()

	def getValuesFromRequests(self, repo_name, url):
		result = requests.get(url)
		if(result.status_code == 200):
			self.getValues(url, result.content)
		elif result.status_code == 404:
			raise CannotScrapRepoException("Could not scrap the repo '" + repo_name + "'. Repo does not exists.")
		else:
			raise CannotScrapRepoException("Could not scrap the repo '" + repo_name + "'. Unknow error code : " + str(result.status_code))

	def getValuesFromSelenium(self, driver, url, visible):
		driver.get(url)
		driver.execute_script("return document.readyState")
		self.getValues(url, driver.page_source)

	def getValues(self, url, html):
		html = BeautifulSoup(html, 'html.parser')
		self.scrapCommits(html)				#0
		self.scrapBranches(html)			#1
		self.scrapReleases(html)			#2
		self.scrapContributors(html)		#3
		self.scrapIssues(html)				#4
		self.scrapPullRequests(html)		#5
		self.scrapWatchs(html)				#6
		self.scrapStars(html)				#7
		self.scrapForks(html)				#8
		self.scrapAge(url)

	#SCRAPPING FUNCTIONS

	def scrapCommits(self, html):
		try:
			li = html.find_all("li", class_="commits")
			self.commits = li[0].a.span.string.strip().replace(',','').encode('utf8')
		except IndexError:
			self.commits =  "0"
		except AttributeError:
			raise PageNotLoadedException("Could not scrap commits. Is the page fully loaded ?")

	def scrapBranches(self, html):
		try:
			svg = html.find_all("svg", class_="octicon-git-branch")
			self.branches = svg[0].parent.span.string.strip().replace(',','').encode('utf8')
		except IndexError:
			self.branches =  "0"
		except AttributeError:
			raise PageNotLoadedException("Could not scrap branches. Is the page fully loaded ?")

	def scrapReleases(self, html):
		try:
			svg = html.find_all("svg", class_="octicon-tag")
			self.releases = svg[0].parent.span.string.strip().replace(',','').encode('utf8')
		except IndexError:
			self.releases =  "0"
		except AttributeError:
			raise PageNotLoadedException("Could not scrap releases. Is the page fully loaded ?")

	def scrapContributors(self, html):
		try:
			svg = html.find_all("svg", class_="octicon-organization")
			self.contributors = svg[0].parent.span.string.strip().replace(',','').encode('utf8')
		except IndexError:
			self.contributors =  "0"
		except AttributeError:
			raise PageNotLoadedException("Could not scrap contributors. Is the page fully loaded ?")

	def scrapIssues(self, html):
		try:
			svg = html.find_all("svg", class_="octicon-issue-opened")
			span = svg[0].parent.find_all("span", class_="Counter")
			self.issues = span[0].string.strip().replace(',','').encode('utf8')
		except IndexError:
			self.issues =  "0"
		except AttributeError:
			raise PageNotLoadedException("Could not scrap issues. Is the page fully loaded ?")

	def scrapPullRequests(self, html):
		try:
			svg = html.find_all("svg", class_="octicon-git-pull-request")
			span = svg[0].parent.find_all("span", class_="Counter")
			self.pull_requests = span[0].string.strip().replace(',','').encode('utf8')
		except IndexError:
			self.pull_requests =  "0"
		except AttributeError:
			raise PageNotLoadedException("Could not scrap pull requests. Is the page fully loaded ?")

	def scrapWatchs(self, html):
		try:
			ul = html.find_all("ul", class_="pagehead-actions")
			a = ul[0].find_all("a", class_="social-count")
			self.watchs = a[0].string.strip().replace(',','').encode('utf8')
		except IndexError:
			self.watchs =  "0"
		except AttributeError:
			raise PageNotLoadedException("Could not scrap watchs. Is the page fully loaded ?")

	def scrapStars(self, html):
		try:
			ul = html.find_all("ul", class_="pagehead-actions")
			a = ul[0].find_all("a", class_="social-count")
			self.stars = a[1].string.strip().replace(',','').encode('utf8')
		except IndexError:
			self.stars =  "0"
		except AttributeError:
			raise PageNotLoadedException("Could not scrap stars. Is the page fully loaded ?")
		
	def scrapForks(self, html):
		try:
			ul = html.find_all("ul", class_="pagehead-actions")
			a = ul[0].find_all("a", class_="social-count")
			self.forks = a[2].string.strip().replace(',','').encode('utf8')
		except IndexError:
			self.forks =  "0"
		except AttributeError:
			raise PageNotLoadedException("Could not scrap forks. Is the page fully loaded ?")

	def scrapAge(self, url):
		try:
			self.age = "0".encode('utf8')
		except IndexError:
			self.age =  "0"
		except AttributeError:
			raise PageNotLoadedException("Could not scrap age. Is the page fully loaded ?")

	def __str__(self):
		return self.repo_name + ": commits(" + str(self.commits) + ") branches(" + str(self.branches) + ") releases(" + str(self.releases) + ") contributors(" + str(self.contributors) + ") issues(" + str(self.issues) + ") pull_requests(" + str(self.pull_requests) + ") watchs(" + str(self.watchs) + ") stars(" + str(self.stars) + ") forks(" + str(self.forks) + ") age(" + str(self.age) + ")"