#!/usr/bin/python

from __future__ import division
import os, traceback, sys
import pyspark, datetime

DAMPING_FACTOR = 0.85

def log(x):
	print(x)

# STEP 1
def getNodes(line):
	email = line.split('\t')[0]
	repo = line.split('\t')[1]
	commits = int(line.split('\t')[2])
	yield repo, (email, commits)		#Yield the repo node as str
	yield email, (repo, commits)		#Yield the user node as str

def getLinks(node):
	name = node[0]
	neighbours = node[1]
	links = {}
	total_commits = 0

	for i in range(1, len(neighbours), 2):
		total_commits += neighbours[i]

	for i in range(0, len(neighbours), 2):
		links[neighbours[i]] = neighbours[i+1]/total_commits

	return name, links

#STEP 2
def setWeight(node):

	for neighbour in node[1][0].keys():											#For every neighbour
		yield (neighbour, node[1][1]*node[1][0][neighbour])

def main(input, output, iterations):

	#OPEN
	print("Opening file...")
	sc = pyspark.SparkContext(appName="pagerank")
	links = sc.textFile("file://" + input)

	#STEP 1 = Get nodes with their links
	print("STEP 1 - Getting the nodes...")
	nodes = links \
				.filter(lambda line: len(line.split('\t')) == 3) \
				.flatMap(lambda line: getNodes(line)) \
				.reduceByKey(lambda a, b: a+b) \
				.map(lambda node: getLinks(node))

	#STEP 2 = Get default weight
	print("STEP 2 - Getting default weight...")
	ranks = nodes.map(lambda node: (node[0], 1))

	#STEP 3 to 3+N = Page ranking
	print("STEP 3 - Page ranking in progress...")
	print('0/' + str(iterations))

	for i in range(iterations):
		ranks = nodes \
					.join(ranks) \
					.flatMap(lambda node: setWeight(node)) \
					.reduceByKey(lambda a, b: a + b) \
					.mapValues(lambda rank: rank*DAMPING_FACTOR + (1-DAMPING_FACTOR))
		print('\033[F' + str(i+1) + '/' + str(iterations))

	#STEP 4 = Order by weight
	print("STEP 4 - Ordering ranks...")
	orderedRanks = ranks.sortBy(lambda node: node[1])

	#SAVE
	print("Saving file...")
	orderedRanks.saveAsTextFile(output)
	sc.stop()

	print("Done !")

########
# MAIN #
########

if __name__ == '__main__':		#If main function
	try:							#Try
		if(len(sys.argv) != 3):
			print("Usage : python <input> <iterations>")
			sys.exit(-1)

		exists = os.path.isfile(sys.argv[1])
		if(not input):
			print("<input> have to be a file.")
			sys.exit(-1)

		if(sys.argv[1][0] == '/'): 	#If absolute
			path = sys.argv[1][0:sys.argv[1].rfind('/')+1]
			input = path + sys.argv[1][sys.argv[1].rfind('/')+1:]
		else:						#If relative
			path = os.getcwd() + "/"
			input = path + sys.argv[1]
		output =  path + str(datetime.datetime.now()).replace('.','_').replace(':','-')

		try:
			iterations = int(sys.argv[2])
		except:
			print("<iteration> have to be a number.")
			sys.exit(-1)

		print("====")
		print(input)
		print(output)
		print("====")
		main(input, output, iterations)	#Run spark
	except KeyboardInterrupt:		#If Ctrl+C
		pass 							#Do nothing
	except:							#If any other exception
		print(traceback.print_exc())	#print it