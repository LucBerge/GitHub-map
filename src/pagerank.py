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
		steps = [MRStep(mapper_init=self.open_database, mapper=self.get_links_mapper, reducer=self.get_links_reducer, mapper_final=self.close_database)]

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
	def get_links_mapper(self, key, value):
		repos = self.db.get_repos() #Get all repos
		users = self.db.get_users() #Get all repos
		links = self.db.get_links() #Get all links
		N = len(repos) + len(users)

		for repo_name, email, commits in links:
			yield {'key' : repo_name}, (email, commits, N)
			yield {'key' : email}, (repo_name, commits, N)

	def get_links_reducer(self, node, values):
		links = {}
		tab_values = [value for value in values]
		total_commits = sum([value[1] for value in tab_values])

		for value in tab_values:
			links[value[0]] = value[1]/total_commits

		node['links'] = links
		yield node, 1/tab_values[0][2]

	#STEP 2 = Page rank
	def mapper_pagerank(self, node, weight):
		yield {'key' : node['key']}, node['links']												#Give links to himself
		yield {'key' : node['key']}, weight*self.options.damping_factor							#Keep a part of his weight
		neighbours = node['links'] 		
																								#Get neighbours
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
				PageRank.run()

		if(not valid):
			print("You forgot the --database argument.")

	except KeyboardInterrupt:
		pass
	except:
		print(traceback.print_exc())

# python pagerank.py -r local --database=GitHubMap.db --iterations=10
# python pagerank.py -r hadoop --database=GitHubMap.db  --iterations=10