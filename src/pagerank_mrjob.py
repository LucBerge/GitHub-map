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
		steps = [MRStep(mapper=self.get_commits_mapper, reducer=self.get_commits_reducer)]	#1 - Get commits								# 2 - Get the default waight
		for i in range(self.options.iterations):
			steps.append(MRStep(mapper=self.pagerank_mapper,								# 3 to N - Page rank step
								reducer=self.pagerank_reducer))
		steps.append(MRStep(mapper=self.output_mapper, reducer=self.output_reducer))		# 4 - Sort the output
		return steps

	#########
	# STEPS #
	#########

	# STEP 1 = Get the number of commits
	def get_commits_mapper(self, _, line):
		values = line.strip().split('\t')				#Strip and split each line
		if len(values) == 3:							#If the line have 3 values
			yield values[0], (values[1], int(values[2]))	#Yield the repo node as str
			yield values[1], (values[0], int(values[2]))	#Yield the user node as str

	def get_commits_reducer(self, name, values):	#Calcul the pourcentage for each link
		node = {'name' : name}
		links = {}
		tab_values = [value for value in values]	#Get the values as a regular list
		total_commits = sum([value[1] for value in tab_values])	#Get the total number of commits for the node
		for value in tab_values:						#For each connected node
			links[value[0]] = value[1]/total_commits		#Add the pourcentage for the node in the link dict
		node['links'] = links	#Add the links dict to the key
		yield node, 1 			#Yield all nodes with their default weight

	#STEP 2 to 2+N = Page rank
	def pagerank_mapper(self, node, weight):
		yield node['name'], node['links']					#Give links to himself																
		for neighbour in node['links'].keys():				#For every neighbour
			yield neighbour, weight*node['links'][neighbour]	#Give a part of his weight

	def pagerank_reducer(self, name, values):
		node = {'name' : name}
		weight = 0					#Set the new weight to 0
		for value in values:		#For all values
			if(type(value) == dict):	#If its the links gived by himself
				node['links'] = value		#Store it
			else:						#If it is a weight
				weight+=value				#Add it
		yield node, weight*self.options.damping_factor+(1-self.options.damping_factor)				#Set the new weight

	#STEp 3+N = Sort output
	def output_mapper(self, node, weight):
		yield None, (weight, node['name'])			#Yield all nodes with the None key to be able to acces all nodes from one key

	def output_reducer(self, _, nodes):
		for weight, node in sorted(nodes, reverse=True):	#For all nodes sorted by weight
			yield node, weight 									#Yield the final weight

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