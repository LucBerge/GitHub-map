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

	QUERY_LIMIT = 500000
	OFFSET = 0
	CURRENT_FOLDER = '/media/lucas/DATA/Lucas/Etudes/ESISAR 2017-2020/Semestre 4 (Norway)/DAT500 - Data intensive systems/Project/src/'
	DATABASE_NAME = 'test.db'
	LOG_NAME = 'logs.txt'

	# VARIABLES

	kaggle = None
	db = None

	def steps(self):
		return [
			MRStep(mapper_init=self.connect_to_kaggle,
					mapper=self.get_kaggle_mapper,
					mapper_final=self.disconnect_to_kaggle,
					combiner=self.get_kaggle_combiner,
					reducer=self.get_kaggle_reducer),
			MRStep(mapper=self.scrapping),
			MRStep(mapper_init=self.connect_to_database,
					mapper=self.save,
					mapper_final=self.disconnect_to_database)
		]

	def connect_to_kaggle(self):
		self.kaggle = bigquery.Client()

	def disconnect_to_kaggle(self):
		pass	#Do nothing

	def connect_to_database(self):
		self.db = GitHubDatabase(self.CURRENT_FOLDER + self.DATABASE_NAME)

	def disconnect_to_database(self):
		self.db.close()

	def connect_to_kaggle_and_database(self):
		self.connect_to_kaggle()
		self.connect_to_database()

	def disconnect_to_kaggle_and_database(self):
		self.disconnect_to_kaggle()
		self.disconnect_to_database()

	#########
	# STEPS #
	#########

	#STEP 1 = Get sample repos
	def get_kaggle_mapper(self, key, value):

		try:
			while(self.OFFSET%self.QUERY_LIMIT == 0):

				query = """
					SELECT C.committer.email, C.committer.name, R.repo_name
					FROM `bigquery-public-data.github_repos.commits` C, `bigquery-public-data.github_repos.sample_repos` R
					WHERE R.repo_name IN UNNEST(C.repo_name)
					LIMIT """ + str(self.QUERY_LIMIT) + """
					OFFSET """ + str(self.OFFSET)
				
				rows = self.kaggle.query(query)

				for row in rows:
					yield {'repo_name' : row['repo_name']}, 1

					email = row['email']
					name = row['name']
					if '@' in email and name:
						yield {'email' : email, 'name' : name}, 1
						yield {'repo_name' : row['repo_name'], 'email' : email}, 1

				self.OFFSET+=self.QUERY_LIMIT
				log(str(self.OFFSET) + " commits have been saved.")

		except Forbidden:
			log("Maximum quota reached. Do not forget to update the offset variable to " + str(self.OFFSET))

	def get_kaggle_combiner(self, key, values):
		yield key, sum(values)

	def get_kaggle_reducer(self, key, values):
		yield key, sum(values)

	#STEP 2 = Scrap the repos and users (Optional)
	def scrapping(self, key, value):
		if 'repo_name' in key and 'email' in key:
			yield key, value
		elif 'email' in key:
			#us = UserScrapper(key['email'])
			#us.scrap()
			yield key, value
		elif 'repo_name' in key:
			try:
				rs = RepoScrapper(key['repo_name'])
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
				log(str(e))

	#STEP 3 = Save repos and users
	def save(self, key, value):
		if 'repo_name' in key and 'email' in key:
			self.db.insert_link(key['repo_name'], key['email'], value)
		elif 'email' in key:
			self.db.insert_user(key['email'], key['name'], None)
		elif 'repo_name' in key:
			self.db.insert_repo(key['repo_name'], key['commits'], key['branches'], key['releases'], key['contributors'], key['issues'], key['pull_requests'], key['watchs'], key['stars'], key['forks'], None)

########
# MAIN #
########

def log(message):
	print(message)
	file = open(PageRank.CURRENT_FOLDER + PageRank.LOG_NAME, "a")
	file.write(message + '\n')
	file.close()

if __name__ == '__main__':
	try:
		PageRank.run()

	except DefaultCredentialsError:
		print('You first have to set GOOGLE_APPLICATION_CREDENTIALS environnement variable.')
		print('Open a console and type "export GOOGLE_APPLICATION_CREDENTIALS=path/to/google/credentials.json".')

	except KeyboardInterrupt:
		pass

	except:
		log(traceback.print_stack())
