#!/usr/bin/python

from __future__ import division
import sys, os
from mrjob.job import *
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from google.api_core.exceptions import Forbidden

from utils.githubdatabase import *
from utils.githubscrapper import *


class CreateDatabase(MRJob):

	# VARIABLES

	kaggle = None
	db = None

	def configure_options(self):
		super(CreateDatabase, self).configure_options()

		self.add_passthrough_option(
			'--current-folder', dest='current_folder', type='str',
			help='The current folder')
		self.add_passthrough_option(
			'--database', dest='database', type='str',
			help='The database to use')


		self.add_passthrough_option(
			'--query-limit', dest='query_limit', default=200000, type='int',
			help='The maximum number of rows in the query')

	def steps(self):
		return [
			MRStep(mapper_init=self.connect_to_kaggle,
					mapper=self.get_kaggle_mapper,
					reducer=self.get_kaggle_reducer),
			MRStep(mapper=self.scrapping),
			MRStep(mapper_init=self.connect_to_database,
					mapper=self.save,
					mapper_final=self.disconnect_to_database)
		]

	def connect_to_kaggle(self):
		self.kaggle = bigquery.Client()

	def connect_to_database(self):
		self.db = GitHubDatabase(self.options.current_folder + self.options.database)

	def disconnect_to_database(self):
		self.db.close()

	#########
	# STEPS #
	#########

	#STEP 1 = Get sample repos
	def get_kaggle_mapper(self, key, value):

		try:
			query = """
				SELECT committer.email, committer.name, repo_name
				FROM `bigquery-public-data.github_repos.commits`
				WHERE RAND() < """ + str(self.options.query_limit) + """/(SELECT COUNT(*) FROM `bigquery-public-data.github_repos.commits`)
				"""
				
			rows = self.kaggle.query(query)
			self.log(str(self.options.query_limit) + " commits have been queried.")

			for row in rows:
				email = row['email']
				name = row['name']

				if '@' in email and name:
					yield {'email' : email, 'name' : name}, 1

					for repo in row['repo_name']:
						yield {'repo_name' : repo}, 1
						yield {'repo_name' : repo, 'email' : email}, 1

			self.log(str(self.options.query_limit) + " commits have been saved.")

		except Forbidden:
			self.log("Maximum quota reached.")

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
				self.log(str(e))

	#STEP 3 = Save repos and users
	def save(self, key, value):
		if 'repo_name' in key and 'email' in key:
			self.db.insert_link(key['repo_name'], key['email'], value)
		elif 'email' in key:
			self.db.insert_user(key['email'], key['name'], None)
		elif 'repo_name' in key:
			self.db.insert_repo(key['repo_name'], key['commits'], key['branches'], key['releases'], key['contributors'], key['issues'], key['pull_requests'], key['watchs'], key['stars'], key['forks'], None)

	#######
	# LOG #
	#######

	def log(self, message):
		print(message)
		file = open(self.options.current_folder + "log.txt", "a")
		file.write(str(message) + '\n')
		file.close()

########
# MAIN #
########

if __name__ == '__main__':
	try:
		valid = False
		for arg in sys.argv:
			if('--database=' in arg):
				valid = True
				sys.argv.append('--current-folder=' + os.getcwd() + '/')
				CreateDatabase.run()

		if(not valid):
			print("You forgot the --database argument.")

	except DefaultCredentialsError:
		print('You first have to set GOOGLE_APPLICATION_CREDENTIALS environnement variable.')
		print('Open a console and type "export GOOGLE_APPLICATION_CREDENTIALS=path/to/google/credentials.json".')

	except KeyboardInterrupt:
		pass

	except:
		print(traceback.print_exc())
