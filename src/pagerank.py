#!/usr/bin/python

from __future__ import division
import os, traceback, sys
import pyspark, datetime

DAMPING_FACTOR = 0.85

def log(x):
	print(x)

# STEP 1
def getNodes(line):
	values = line.strip().split('\t')				#Strip and split each line
	if len(values) == 3:							#If the line have 3 values
		yield values[0], (values[1], int(values[2]))	#Yield the repo node as str
		yield values[1], (values[0], int(values[2]))	#Yield the user node as str

def getLinks(node):
	links = {}
	total_commits = 0
	for i in range(1, len(node[1]), 2):
		total_commits += node[1][i]
	for i in range(0, len(node[1]), 2):
		links[node[1][i]] = node[1][i+1]/total_commits
	return node[0], links

#STEP 2
def setWeight(node):
	for neighbour in node[1][0].keys():											#For every neighbour
		yield (neighbour, node[1][1]*node[1][0][neighbour])

def main(input, output, iterations):

	#OPEN
	print("Opening file...")
	sc = pyspark.SparkContext(appName="pagerank")
	sc.setLogLevel("ERROR")
	commits = sc.textFile("file://" + input)

	#STEP 1 = Get the links for each node
	print("STEP 1 - Getting the links...")
	links = commits \
				.filter(lambda line: len(line.split('\t')) == 3) \
				.flatMap(lambda line: getNodes(line)) \
				.reduceByKey(lambda a, b: a+b) \
				.map(lambda node: getLinks(node))

	#STEP 2 = Get default weight
	print("STEP 2 - Setting default weight...")
	ranks = links.map(lambda node: (node[0], 1))

	#STEP 3 to 3+N = Page ranking
	print("STEP 3 - Page ranking in progress...")
	print('0/' + str(iterations))

	for i in range(iterations):
		#ranks.saveAsTextFile(output + "/" + str(i))
		ranks = links \
					.join(ranks) \
					.flatMap(lambda node: setWeight(node)) \
					.reduceByKey(lambda a, b: a + b) \
					.mapValues(lambda rank: rank*DAMPING_FACTOR + (1-DAMPING_FACTOR))
		print('\033[F' + str(i+1) + '/' + str(iterations))

	#STEP 4 = Order by weight
	print("STEP 4 - Ordering ranks...")
	orderedRanks = ranks.sortBy(lambda node: node[1], False)

	#SAVE
	print("Saving file...")
	#orderedRanks.saveAsTextFile(output + "/" + str(iterations))
	sqlContext = pyspark.sql.SQLContext(sc)
	dataFrame = sqlContext.createDataFrame(orderedRanks)
	dataFrame.repartition(1).write.format("com.databricks.spark.csv").save(output)
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