#!/usr/bin/python

from __future__ import division
import sys, os, traceback, ast
from mrjob.job import *
from utils import githubdatabase

class PageRank(MRJob):

	# VARIABLES

	db = None
	counter = 0

	def configure_options(self):
		super(PageRank, self).configure_options()
		
		self.add_passthrough_option(
			'--iterations', dest='iterations', default=10, type='int',
			help='The number of iterations')
		self.add_passthrough_option(
			'--damping-factor', dest='damping_factor', default=0.85, type='float',
			help='Pourcentage of weigth the node keep for himself')

	def steps(self):
		steps = [MRStep(mapper=self.get_commits_mapper, reducer=self.get_commits_reducer)]
		steps.append(MRStep(reducer=self.get_weight_reducer))

		for i in range(self.options.iterations):
			steps.append(MRStep(mapper=self.pagerank_mapper,
								reducer=self.pagerank_reducer))

		steps.append(MRStep(mapper=self.output_mapper, reducer=self.output_reducer))

		return steps

	#########
	# STEPS #
	#########

	# STEP 1 = Get the number of commits

	# Get all links
	def get_commits_mapper(self, key, value):

		values = value.strip().split('\t')

		if len(values) == 3:
			repo = values[0]
			email = values[1]
			commits = values[2]

			yield {'key' : repo}, (email, int(commits))
			yield {'key' : email}, (repo, int(commits))

	# Get all nodes with there links

	def get_commits_reducer(self, node, values):
		links = {}
		tab_values = [value for value in values]
		total_commits = sum([value[1] for value in tab_values])

		for value in tab_values:
			links[value[0]] = value[1]/total_commits

		node['links'] = links
		yield None, node

	# STEP 3 = Get the default weight for each node

	def get_weight_reducer(self, _, nodes):
		tab_nodes = [node for node in nodes]
		N = len(tab_nodes)
		for node in tab_nodes:
			yield node, 1/N

	#STEP 4 to 4+N = Page rank
	def pagerank_mapper(self, node, weight):
		yield {'key' : node['key']}, node['links']												#Give links to himself
		yield {'key' : node['key']}, weight*self.options.damping_factor							#Keep a part of his weight
		neighbours = node['links'] 		
																								#Get neighbours
		for neighbour in neighbours.keys():														#For every neighbour
			yield {'key' : neighbour}, weight*neighbours[neighbour]*(1-self.options.damping_factor)	#Give a part of his weight

	def pagerank_reducer(self, node, values):
		weight = 0						#Set the new weight to 0
		for value in values:			#For all values
			if(type(value) == dict):		#If its the links gived by himself
				node['links'] = value			#Store it
			else:							#If it is a weight
				weight+=value			#Add it

		yield node, weight				#Set the new weight

	#STEp 5+N = Sort output
	def output_mapper(self, node, weight):
		yield None, (weight, node)

	def output_reducer(self, _, nodes):
		for weight, node in sorted(nodes, reverse=True):
			yield (weight, node['key'])

########
# MAIN #
########

if __name__ == '__main__':
	try:
		PageRank.run()
	except KeyboardInterrupt:
		pass
	except:
		print(traceback.print_exc())

# python pagerank.py -r local /folders/input.txt --output-dir hdfs:///folders/output --no-output --iterations=10 --damping-factor=0.85
# python pagerank.py -r hadoop hdfs:///folders/input.txt --output-dir hdfs:///folders/output --no-output --iterations=10 --damping-factor=0.85