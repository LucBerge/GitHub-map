#!/usr/bin/python

from __future__ import division
import sys
from mrjob.job import *
from utils import githubdatabase

class PageRank(MRJob):

	# CONSTANTS

	EPSILON = 0.15
	MAX_STEP = 2000
	DATABASE_NAME = '/media/lucas/DATA/Lucas/Etudes/ESISAR 2017-2020/Semestre 4 (Norway)/DAT500 - Data intensive systems/Project/src/test.db'

	# VARIABLES

	db = None

	def steps(self):
		steps = [MRStep(mapper_init=self.open_database, mapper=self.set_weight, mapper_final=self.close_database)]

		for i in range(self.MAX_STEP):
			steps.append(MRStep(mapper_init=self.mapper_pagerank_init,
								mapper=self.mapper_pagerank,
								mapper_final=self.close_database,
								combiner=self.combiner_pagerank,
								reducer=self.reducer_pagerank))

		steps.append(MRStep(mapper_init=self.open_database, mapper=self.save_weight, mapper_final=self.close_database))
		return steps

	def open_database(self):
		self.db = githubdatabase.GitHubDatabase(self.DATABASE_NAME)

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
			yield get_node_as_dictionnary(repo, 'repo'), initial_weight

		for user in users:
			yield get_node_as_dictionnary(user, 'user'), initial_weight,

	#STEP 2 = Page rank
	def mapper_pagerank_init(self):
		self.open_database()
		self.repos = [repo[0] for repo in self.db.get_repos()] #Get all repo names
		self.users = [user[0] for user in self.db.get_users()] #Get all user emails
		self.min_factor = self.EPSILON/(len(self.repos) + len(self.users))

	def mapper_pagerank(self, node, weight):
		if node['type'] == "user":
			neighbour_repos = self.db.get_neighbour_repos(node['name'])

			#Give his weight*factor*(1-Epsilon) to neighbour repos
			total_commits = sum([commits for repo, commits in neighbour_repos])
			for repo, commits in neighbour_repos:
				yield get_node_as_dictionnary(repo, 'repo'), weight*(commits/total_commits*(1-self.EPSILON) + self.min_factor)

			# Get non neighbour repos
			neighbour_repos = [repo[0] for repo in neighbour_repos]
			not_neighbour_repos = [repo for repo in self.repos if repo not in neighbour_repos]

			#Give the minimum factor to non neighbour repos
			for repo in not_neighbour_repos:
				yield get_node_as_dictionnary(repo, 'repo'), weight*self.min_factor

			#Give the minimum factor to all users
			for user in self.users:
				yield get_node_as_dictionnary(user, 'user'), weight*self.min_factor

		else:
			#Give his weight*factor*(1-Epsilon) to all neighbour users
			neighbour_users = self.db.get_neighbour_users(node['name'])

			total_commits = sum([commits for user, commits in neighbour_users])
			for user, commits in neighbour_users:
				yield get_node_as_dictionnary(user, 'user'), weight*(commits/total_commits*(1-self.EPSILON) + self.min_factor)

			# Get non neighbour users
			neighbour_users = [user[0] for user in neighbour_users]
			not_neighbour_users = [user for user in self.users if user not in neighbour_users]

			#Give the minimum factor to non neighbour users
			for user in not_neighbour_users:
				yield get_node_as_dictionnary(user, 'user'), weight*self.min_factor

			#Give the minimum factor to all repos
			for repo in self.repos:
				yield get_node_as_dictionnary(repo, 'repo'), weight*self.min_factor

	def combiner_pagerank(self, node, weights):
		yield node, sum(weights)

	def reducer_pagerank(self, node, weights):
		yield node, sum(weights)

	# STEP 3 = Save
	def save_weight(self, node, weight):
		if node['type'] == "user":
			self.db.update_user_weight(node['name'], weight)
		else:
			self.db.update_repo_weight(node['name'], weight)

########
# MAIN #
########

def get_node_as_dictionnary(name, type):
	return {'name' : name, 'type' : type}

if __name__ == '__main__':
	try:
		PageRank.run()
	except KeyboardInterrupt:
		pass

# python pagerank.py -r local --database=test.db
# python pagerank.py -r hadoop --database=test.db