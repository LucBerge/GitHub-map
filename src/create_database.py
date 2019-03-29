#!/usr/bin/python

from __future__ import division
import sys
from mrjob.job import *
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from google.api_core.exceptions import Forbidden

from utils.githubdatabase import *
from utils.githubscrapper import *

class PageRank(MRJob):

	# CONSTANTS

	QUERY_LIMIT = 100000
	DATABASE_NAME = '/home/ubuntu/GitHub-map/src/GitHubMap.db'

	# VARIABLES

	kaggle = None
	db = None

	def steps(self):
		return [
			MRStep(mapper_init=self.query_kaggle_init,
					mapper=self.query_kaggle,
					mapper_final=self.query_kaggle_final,
					combiner=self.query_kaggle_combiner,
					reducer=self.query_kaggle_reducer),
			MRStep(mapper=self.scrapping),
			MRStep(mapper_init=self.save_init,
					mapper=self.save,
					mapper_final=self.save_final)
		]

	def connect_to_kaggle(self):
		self.kaggle = bigquery.Client()

	def disconnect_to_kaggle(self):
		pass	#Do nothing

	def open_database(self):
		self.db = GitHubDatabase(self.DATABASE_NAME)

	def close_database(self):
		self.db.close()

	#########
	# STEPS #
	#########

	#STEP 1
	def query_kaggle_init(self):
		self.connect_to_kaggle()
		self.open_database()

	def query_kaggle(self, key, value):

		nb_rows = self.QUERY_LIMIT
		offset = 500000

		try:
			while(offset%self.QUERY_LIMIT == 0):
				query = """
							SELECT repo_name, committer 
							FROM `bigquery-public-data.github_repos.commits`
							LIMIT """ + str(self.QUERY_LIMIT) + """
							OFFSET """ + str(offset)
						
				results = self.kaggle.query(query)
				print(str(self.QUERY_LIMIT) + " commits have been queried")
				saved = 0
				

				for row in results:

					saved+=1
					email = row['committer']['email']
					name = row['committer']['name']

					yield {'key' : email, 'name' : name}, None

					for repo_name in row['repo_name']:
						repo_name = repo_name
						self.db.insert_link(repo_name, email)

						yield {'key' : repo_name}, None

				offset += saved
				print(str(saved) + " commits have been saved.")
				print("Total : " + str(offset) + " commits saved")

		except Forbidden:
			print("Maximum quota reached. Do not forget to update the offset variable to " + str(offset))

	def query_kaggle_final(self):
		self.disconnect_to_kaggle()
		self.close_database()

	def query_kaggle_combiner(self, key, values):
		yield key, None

	def query_kaggle_reducer(self, key, values):
		yield key, None

	#STEP 2
	def scrapping(self, key, value):
		if '@' in key['key']:
			#us = UserScrapper(key)
			#us.scrap()
			yield key, value
		else:
			try:
				rs = RepoScrapper(key['key'])
				#rs.scrap(True)
				key['commits'] = rs.getCommits()
				key['branches'] = rs.getBranches()
				key['releases'] = rs.getReleases()
				key['contributors'] = rs.getContributors()
				key['issues'] = rs.getIssues()
				key['pull_requests'] = rs.getPullRequests()
				key['watchs'] = rs.getWatchs()
				key['stars'] = rs.getStars()
				key['forks'] = rs.getForks()
				yield key, value
			except CannotScrapRepoException as e:
				print(str(e))

	#STEP 39
	def save_init(self):
		self.open_database()

	def save(self, key, value):
		if '@' in (key['key']):
			self.db.insert_user(key['key'], key['name'], None)
		else:
			self.db.insert_repo(key['key'], key['commits'], key['branches'], key['releases'], key['contributors'], key['issues'], key['pull_requests'], key['watchs'], key['stars'], key['forks'], None)

	def save_final(self):
		self.close_database()

if __name__ == '__main__':
	try:
		PageRank.run()

	except DefaultCredentialsError:
		print('You first have to set GOOGLE_APPLICATION_CREDENTIALS environnement variable.')
		print('Open a console and type "export GOOGLE_APPLICATION_CREDENTIALS=path/to/google/credentials.json".')

	except KeyboardInterrupt:
		pass
