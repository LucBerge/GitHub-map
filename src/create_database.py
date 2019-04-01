#!/usr/bin/python

# USAGE

# python create_database.py file_with_one_line_to_simulate_one_input.txt --database=test --query-limit=200000

from __future__ import division
import sys, os
from mrjob.job import *
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from google.api_core.exceptions import Forbidden

from utils.githubdatabase import *
from utils.githubscrapper import *


class CreateDatabase(MRJob):

	# ATRIBUTS

	kaggle = None	#Big Query client for kaggle database

	def configure_options(self):	#Set options for command line call
		super(CreateDatabase, self).configure_options()	#Call the super constructor

		self.add_passthrough_option(	#Add option --current-folder wich represent the current full path to the file
			'--current-folder', dest='current_folder', type='str',
			help='The current folder')
		self.add_passthrough_option(	#Add option --database wich is the name of the dataset we want to create
			'--database', dest='database', type='str',
			help='The name of the futur database')

		self.add_passthrough_option(	#Add option --query-limit wich is the limit for one query (default 200000)
			'--query-limit', dest='query_limit', default=200000, type='int',
			help='The maximum number of rows in the query')

	def steps(self):	#Set steps
		return [
			MRStep(mapper_init=self.connect_to_kaggle,											#1 - Query the kaggle database
					mapper=self.get_kaggle_mapper,
					reducer=self.get_kaggle_reducer),
			MRStep(mapper=self.scrapping),														#2 - Scrap repos informations (currently desabled because commented)
			MRStep(mapper_init=self.save_init, mapper=self.save, mapper_final=self.save_final)	#3 - Save the result
		]

	def connect_to_kaggle(self):	# Connect to kaggle
		self.kaggle = bigquery.Client()

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
				"""	#Create the query which is a random selection from the commits table
				
			rows = self.kaggle.query(query)	#Execute the query
			self.log(str(self.options.query_limit) + " commits have been queried.")

			for row in rows:	#For each row in the result
				email = row['email']	#Get the email
				name = row['name']		#Get the name

				if '@' in email and name:	#If the format is ok
					yield {'email' : email, 'name' : name}, 1	#yield user as dict

					for repo in row['repo_name']:				#For each repo
						yield {'repo_name' : repo}, 1					#yield the repo as dict
						yield {'repo_name' : repo, 'email' : email}, 1	#yield the link as dict

			self.log(str(self.options.query_limit) + " commits have been saved.")

		except Forbidden:
			self.log("Maximum quota reached.")

	def get_kaggle_reducer(self, key, values):
		yield key, sum(values)	# Sum the number of commits from one user on one repo

	#STEP 2 = Scrap the repos and users (Optional)
	def scrapping(self, key, value):
		if 'repo_name' in key and 'email' in key:	#If it is a link
			yield key, value 							#do nothing and yield
		elif 'email' in key:						#If it is a user
			#us = UserScrapper(key['email'])			#Create the scrap object
			#us.scrap()									#Scrap
														#Get results
			yield key, value 							#yield the user
		elif 'repo_name' in key:					#If it is a repo
			try:										#Try to scrap
				rs = RepoScrapper(key['repo_name'])			#Create the scrap object
				#rs.scrap(True)								#Scrap
				key['commits'] = rs.getCommits()			#Store the numbers of commits in the key
				key['branches'] = rs.getBranches()			#Store the numbers of branches in the key
				key['releases'] = rs.getReleases()			#Store the numbers of releases in the key
				key['contributors'] = rs.getContributors()	#Store the numbers of contributors in the key
				key['issues'] = rs.getIssues()				#Store the numbers of issues in the key
				key['pull_requests'] = rs.getPullRequests()	#Store the numbers of pull_requests in the key
				key['watchs'] = rs.getWatchs()				#Store the numbers of watchs in the key
				key['stars'] = rs.getStars()				#Store the numbers of stars in the key
				key['forks'] = rs.getForks()				#Store the numbers of forks in the key
				yield key, value  							#yield the repo
			except CannotScrapRepoException as e:		#If impossible to scrap the repo
				self.log(str(e))							#Display why and do not yield it

	#STEP 3 = Save repos and users
	def save_init(self):											#Called once before the save mapper
		self.repos = open(self.options.database + "_repos.txt", "w")	#Open or create the repos file
		self.users = open(self.options.database + "_users.txt", "w")	#Open or create the users file
		self.links = open(self.options.database + "_links.txt", "w")	#Open or create the links file

	def save(self, key, value):
		if 'repo_name' in key and 'email' in key:									#If it is a link
			self.links.write(key['repo_name'] + '\t' + key['email'] + '\t' + str(value) + '\n')		#yield it as string
		elif 'email' in key:														#If it is a user
			self.users.write(key['email'] + '\t' + key['name']) 						#Do nothing for repos (for the moment)
		elif 'repo_name' in key:													#If it is a repo
			self.repos.write(key['repo_name'] + '\t' + str(key['commits']) + '\t' + str(key['branches']) + '\t' + str(key['releases']) + '\t' + str(key['contributors']) + '\t' + str(key['issues']) + '\t' + str(key['pull_requests']) + '\t' + str(key['watchs']) + '\t' + str(key['stars']) + '\t' + str(key['forks'])) #Do nothing for the users (for the moment)
			
	def save_final(self):	#Called once after the save mapper
		self.repos.close()		#Close the repos file
		self.users.close()		#Close the users file
		self.links.close()		#Close the links file

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

if __name__ == '__main__':	#Main function
	try:
		valid = False
		for arg in sys.argv:			#For each argument
			if('--database=' in arg):		#If --database option is set
				valid = True 					#Command line valid
				sys.argv.append('--current-folder=' + os.getcwd() + '/')	#Add the current path
				CreateDatabase.run()										#Run mrJob

		if(not valid):	#If no valid
			print("You forgot the --database argument. DO NOT RUN WITH HADOOP !")	#Explain why

	except DefaultCredentialsError:	#If trouble with GOOGLE_APPLICATION_CREDENTIALS variable
		print('You first have to set GOOGLE_APPLICATION_CREDENTIALS environnement variable.')
		print('Open a console and type "export GOOGLE_APPLICATION_CREDENTIALS=/full/path/to/google/credentials.json".')

	except KeyboardInterrupt:		#If Ctrl+C
		pass 							#Do nothing

	except:							#For any other exception
		print(traceback.print_exc())	#print the stack trace
