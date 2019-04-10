#!/usr/bin/python

from __future__ import division
import os, traceback, sys
import pyspark, datetime

DAMPING_FACTOR = 0

def log(x):
	print x

# STEP 1
def getNodes(line):
	email = line.split('\t')[0]
	repo = line.split('\t')[1]
	commits = int(line.split('\t')[2])
	yield repo, (email, commits)		#Yield the repo node as str
	yield email, (repo, commits)		#Yield the user node as str

def getDefaultWeight(node):
	name = node[0]
	neighbours = node[1]
	links = {}
	total_commits = 0

	for i in range(1, len(neighbours), 2):
		total_commits += neighbours[i]

	for i in range(0, len(neighbours), 2):
		links[neighbours[i]] = neighbours[i+1]/total_commits

	return name, links#({'name':name, 'links':links}, 1)

#STEP 2
def setWeight(node):

	yield (node[0], node[1][1]*DAMPING_FACTOR) 									#Keep a part of his weight
	for neighbour in node[1][0].keys():											#For every neighbour
		yield (neighbour, node[1][1]*node[1][0][neighbour]*(1-DAMPING_FACTOR))	#Give a part of his weight

def main(input, iterations):

	#OPEN
	print("Opening file...")
	sc = pyspark.SparkContext(appName="pagerank")
	links = sc.textFile("file://" + input, 1)

	#STEP 1 = Get nodes with their links
	print("STEP 1 - Getting the nodes...")
	nodes = links \
				.flatMap(lambda line : getNodes(line)) \
				.reduceByKey(lambda a, b: a+b) \
				.map(lambda node: getDefaultWeight(node))

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
					.reduceByKey(lambda a, b: a + b)
		print('\033[F' + str(i+1) + '/' + str(iterations))

	#STEP 4 = Order by weight
	print("STEP 4 - Ordering ranks...")
	orderedRanks = ranks.sortByKey(ascending= False)
	time = str(datetime.datetime.now()).replace('.','_').replace(':','-')

	#SAVE
	print("Saving file...")
	orderedRanks.saveAsTextFile(time)
	sc.stop()

	print("Done !")
	
########
# MAIN #
########

if __name__ == '__main__':		#If main function
	try:							#Try
		print(sys.argv)
		if(len(sys.argv) != 3):
			print("Usage : python <input> <iterations>")
			sys.exit(-1)

		exists = os.path.isfile(sys.argv[1])
		if(not input):
			print("<input> have to be a file.")
			sys.exit(-1)

		input = os.getcwd() + "/" + sys.argv[1]

		try:
			iterations = int(sys.argv[2])
		except:
			print("<iteration> have to be a number.")
			sys.exit(-1)

		main(input, iterations)	#Run spark
	except KeyboardInterrupt:		#If Ctrl+C
		pass 							#Do nothing
	except:							#If any other exception
		print(traceback.print_exc())	#print it