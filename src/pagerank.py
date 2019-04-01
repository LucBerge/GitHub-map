#!/usr/bin/python

# USAGE

# python pagerank.py input.txt --iterations=10 --damping-factor=0.85
# python pagerank.py -r hadoop hdfs:///folders/input.txt --output-dir hdfs:///folders/output --no-output --iterations=10 --damping-factor=0.85

from __future__ import division
import sys, os, traceback, ast
from mrjob.job import *
from utils import githubdatabase

class PageRank(MRJob):

	def configure_options(self):	#Set options for command line call
		super(PageRank, self).configure_options()	#Call the super constructor
		
		self.add_passthrough_option(	#Add option --iterations wich represent the number of iterations for the pagerank (default 10)
			'--iterations', dest='iterations', default=10, type='int',
			help='The number of iterations')
		self.add_passthrough_option(
			'--damping-factor', dest='damping_factor', default=0.85, type='float',	#Add option --damping-factor wich represent the pourcentage of weigth the node keep for himself (default 0.85)
			help='Pourcentage of weigth the node keep for himself')

	def steps(self):	#Set steps
		steps = [MRStep(mapper=self.get_commits_mapper, reducer=self.get_commits_reducer)]	#1 - Get commits
		steps.append(MRStep(reducer=self.get_weight_reducer))								# 2 - Get the default waight

		for i in range(self.options.iterations):
			steps.append(MRStep(mapper=self.pagerank_mapper,								# 3 to N - Page rank step
								reducer=self.pagerank_reducer))

		steps.append(MRStep(mapper=self.output_mapper, reducer=self.output_reducer))		# 4 - Sort the output

		return steps

	#########
	# STEPS #
	#########

	# STEP 1 = Get the number of commits

	# Get all links
	def get_commits_mapper(self, key, value):

		values = value.strip().split('\t')	 #Strop and split each line

		if len(values) == 3:				#If the line have 3 values
			repo = values[0]					# Get the repo name
			email = values[1]					# Get the email
			commits = values[2]					#Get the number of commits

			yield {'key' : repo}, (email, int(commits))		#Yield the repo node as dict
			yield {'key' : email}, (repo, int(commits))		#Yield the user node as dict

	# Get all nodes with there links

	def get_commits_reducer(self, node, values):	#Calcul the pourcentage for each link
		links = {}
		tab_values = [value for value in values]	#Get the values as a regular list
		total_commits = sum([value[1] for value in tab_values])	#Get the total number of commits for the node

		for value in tab_values:						#For each connected node
			links[value[0]] = value[1]/total_commits		#Add the pourcentage for the node in the link dict

		node['links'] = links	#Add the links dict to the key
		yield None, node 		#Yield all nodes with the None key to be able to acces all nodes from one key

	# STEP 3 = Get the default weight for each node
	def get_weight_reducer(self, _, nodes):	
		tab_nodes = [node for node in nodes] 	#Get the values as a regular list
		N = len(tab_nodes)						#Get the number of nodes
		for node in tab_nodes:					#For each node
			yield node, 1/N 						#Yield the node as key with the default weight as value

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
		yield None, (weight, node)			#Yield all nodes with the None key to be able to acces all nodes from one key

	def output_reducer(self, _, nodes):
		for weight, node in sorted(nodes, reverse=True):	#For all nodes sorted by weight
			yield (weight, node['key'])							#Yield the final weight

########
# MAIN #
########

if __name__ == '__main__':		#If main function
	try:							#Try
		PageRank.run()					#Run mrJob
	except KeyboardInterrupt:		#If Ctrl+C
		pass 							#Do nothing
	except:							#If any other exception
		print(traceback.print_exc())	#print it