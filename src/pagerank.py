#!/usr/bin/python

from __future__ import division
import sys, os
from mrjob.job import *
from utils import githubdatabase
import traceback

class PageRank(MRJob):

	# VARIABLES

	db = None

	def configure_options(self):
		super(PageRank, self).configure_options()

		self.add_passthrough_option(
			'--current-folder', dest='current_folder', type='str',
			help='The current folder')
		self.add_passthrough_option(
			'--database', dest='database', type='str',
			help='The database to use')
		self.add_passthrough_option(
			'--iterations', dest='iterations', default=10, type='int',
			help='The number of iterations')
		self.add_passthrough_option(
			'--damping-factor', dest='damping_factor', default=0.85, type='float',
			help='Pourcentage of weigth the node keep for himself')

	def steps(self):
		steps = [MRStep(mapper_init=self.open_database, mapper=self.set_weight, mapper_final=self.close_database)]

		for i in range(self.options.iterations):
			steps.append(MRStep(mapper=self.mapper_pagerank,
								reducer=self.reducer_pagerank))

		steps.append(MRStep(mapper_init=self.open_database, mapper=self.save_weight, mapper_final=self.close_database))
		return steps

	def open_database(self):
		self.db = githubdatabase.GitHubDatabase(self.options.current_folder + self.options.database)
		self.counter = 0

	def close_database(self):
		self.db.close()

	#########
	# STEPS #
	#########

	# STEP 1 = Set the default weigth for every nodes
	def set_weight(self, key, value):

		repos = [repo[0] for repo in self.db.get_repos()] #Get all repo names
		users = [user[0] for user in self.db.get_users()] #Get all user emails

		initial_weight = 1/(len(repos) + len(users))

		for repo in repos:

			links = {}
			neighbour_users = self.db.get_neighbour_users(repo)
			total_commits = sum([commits for user, commits in neighbour_users])
			for user, commit in neighbour_users:
				links[user] = commit/total_commits
			yield {'key':repo, 'links':links}, initial_weight

		for user in users:

			links = {}
			neighbour_repos = self.db.get_neighbour_repos(user)
			total_commits = sum([commits for repo, commits in neighbour_repos])
			for repo, commit in neighbour_repos:
				links[repo] = commit/total_commits
			yield {'key':user, 'links':links}, initial_weight

	#STEP 2 = Page rank
	def mapper_pagerank(self, node, weight):
		yield {'key' : node['key']}, node['links']												#Give links to himself
		yield {'key' : node['key']}, weight*self.options.damping_factor							#Keep a part of his weight

		neighbours = node['links'] 																#Get neighbours
		for neighbour in neighbours.keys():														#For every neighbour
			yield {'key' : neighbour}, weight*neighbours[neighbour]*(1-self.options.damping_factor)	#Give a part of his weight

	def reducer_pagerank(self, node, values):
		weight = 0						#Set the new weight to 0
		for value in values:			#For all values
			if(type(value) == dict):		#If its the links gived by himself
				node['links'] = value			#Store it
			else:							#If it is a weight
				weight+=value			#Add it

		yield node, weight				#Set the new weight

	# STEP 3 = Save
	def save_weight(self, key, weight):
		if '@' in key['key']:
			self.db.update_user_weight(key['key'], weight)
		else:
			self.db.update_repo_weight(key['key'], weight)

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
				print(sys.argv)
				PageRank.run()

		if(not valid):
			print("You forgot the --database argument.")

	except KeyboardInterrupt:
		pass
	except:
		print(traceback.print_exc())

# python pagerank.py -r local --database=GitHubMap.db --iterations=10
# python pagerank.py -r hadoop --database=GitHubMap.db  --iterations=10