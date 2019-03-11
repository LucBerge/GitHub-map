#!/usr/bin/python

from mrjob.job import *
from bs4 import BeautifulSoup
import requests, traceback, csv
import os
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait

class MRCountSum(MRJob):

	driver = None

	def mapper_init(self):
		self.driver = webdriver.Firefox()

	def mapper(self, key, value):
		url = 'https://github.com/' + value
		dict = self.getDictFromRequests(url)

		if(dict != None):
			while('-1' in dict.values()):
				dict = self.getDictFromSelenium(url)

			yield value, dict

	def mapper_final(self):
		self.driver.close()

	def getDictFromRequests(self, url):
		result = requests.get(url)
		if(result.status_code == 200):
			return self.getDict(result.content)
		else:
			return None

	def getDictFromSelenium(self, url):
		self.driver.get(url)
		self.driver.execute_script("return document.readyState")
		html = self.driver.page_source
		return self.getDict(html)

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

if __name__ == '__main__':
	try:
		MRCountSum.run()
	except KeyboardInterrupt:
		pass
