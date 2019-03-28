#!/usr/bin/python

from __future__ import division
import sys
from mrjob.job import *
from utils import githubdatabase

class PageRank(MRJob):

	EPSILON = 0.15
	MAX_STEP = 1
	INITIALIZED = False

	db_name = '/media/lucas/DATA/Lucas/Etudes/ESISAR 2017-2020/Semestre 4 (Norway)/DAT500 - Data intensive systems/Project/src/test.db'
	db = None

	def steps(self):
		steps = [MRStep(mapper_init=self.open_database, mapper=self.get_weight, mapper_final=self.close_database)]

		for i in range(self.MAX_STEP):
			steps.append(MRStep(mapper_init=self.open_database,
								mapper=self.mapper_pagerank,
								mapper_final=self.close_database,
								combiner=self.combiner_pagerank,
								reducer=self.reducer_pagerank))

		#steps.append(MRStep(mapper_init=self.open_database, mapper=self.save_weight, mapper_final=self.close_database))
		return steps

	def open_database(self):
		self.db = githubdatabase.GitHubDatabase(self.db_name)

	def close_database(self):
		self.db.close()

	def get_weight(self, key, value):

		if not self.INITIALIZED:
			self.INITIALIZED = True

			repos = self.db.get_repos()
			users = self.db.get_users()

			initial_weight = 1/(len(repos) + len(users))

			for repo in repos:
				yield get_node_as_dictionnary(repo[0], 'repo'), initial_weight

			for user in users:
				yield get_node_as_dictionnary(user[0], 'user'), initial_weight,

	# self.M = A*(1-self.EPSILON) + numpy.full((self.N, 1),self.EPSILON/self.N)

	def mapper_pagerank(self, node, weight):
		if node['type'] == "user":
			repos = self.db.get_repos_where_user_is(node['name'])
			total_commits = sum([commits for repo, commits in repos])
			for repo, commits in repos:
				yield get_node_as_dictionnary(repo, 'repo'), weight*commits/total_commits

		else:
			users = self.db.get_users_where_repo_is(node['name'])
			total_commits = sum([commits for user, commits in users])
			for user, commits in users:
				yield get_node_as_dictionnary(user, 'user'), weight*commits/total_commits

	def combiner_pagerank(self, node, weights):
		yield node, sum(weights)

	def reducer_pagerank(self, node, weights):
		yield node, sum(weights)

	def save_weight(self, node, weight):
		if node['type'] == "user":
			self.db.update_user_weight(node['name'], weight)
		else:
			self.db.update_repo_weight(node['name'], weight)

def get_node_as_dictionnary(name, type):
	return {'name' : name, 'type' : type}

if __name__ == '__main__':
	try:
		PageRank.run()
	except KeyboardInterrupt:
		pass

# python pagerank.py -r local --database=test.db
# python pagerank.py -r hadoop --database=test.db