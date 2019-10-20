#!/usr/bin/python

from __future__ import division
import os, traceback, sys, pyspark, datetime, argparse

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

class PageRank:

	DAMPING_FACTOR = 0.85

	def __init__(self, input, output, max_iterations, trace):
		self.input = input
		self.output = output
		self.max_iterations = max_iterations
		self.trace = trace

	def start(self):

		#OPEN
		log("Opening file...")
		sc = pyspark.SparkContext(appName="pagerank")
		sc.setLogLevel("ERROR")
		commits = sc.textFile("file://" + self.input)

		#STEP 1 = Get the links for each node
		log("STEP 1 - Getting the links...")
		links = commits \
					.filter(lambda line: len(line.split('\t')) == 3) \
					.flatMap(lambda line: getNodes(line)) \
					.reduceByKey(lambda a, b: a+b) \
					.map(lambda node: getLinks(node))

		#STEP 2 = Get default weight
		log("STEP 2 - Setting default weight...")
		ranks = links.map(lambda node: (node[0], 1))

		#STEP 3 to 3+N = Page ranking
		log("STEP 3 - Page ranking in progress...")
		log('0/' + str(self.max_iterations))

		for i in range(self.max_iterations):
			if(self.trace):
				self.save_as_csv(sc, ranks, self.output + "/" + str(i))

			ranks = links \
						.join(ranks) \
						.flatMap(lambda node: setWeight(node)) \
						.reduceByKey(lambda a, b: a + b) \
						.mapValues(lambda rank: rank*self.DAMPING_FACTOR + (1-self.DAMPING_FACTOR))
			log('\033[F' + str(i+1) + '/' + str(self.max_iterations))

		#STEP 4 = Order by weight
		log("STEP 4 - Ordering ranks...")
		orderedRanks = ranks.sortBy(lambda node: node[1], False)

		#SAVE
		log("Saving file...")
		if(self.trace):
			self.save_as_csv(sc, ranks, self.output + "/" + str(self.max_iterations))
		else:
			self.save_as_csv(sc, orderedRanks, self.output)
		sc.stop()

	def save_as_csv(self, sc, data, output):
		sqlContext = pyspark.sql.SQLContext(sc)
		dataFrame = sqlContext.createDataFrame(data)
		dataFrame.repartition(1).write.format("com.databricks.spark.csv").save(output)

########
# MAIN #
########

def main():
	parser = argparse.ArgumentParser(description='Run pagerank algorithm on a link text file.')
	parser.add_argument('input', help='The input links text file.')
	parser.add_argument('-max-iterations', type=int, default=0, help='Max number of iterations.')
	parser.add_argument('--trace', default=False, action='store_true', help='True to output a file for each iteration.')
	args = parser.parse_args()

	if(not os.path.isfile(args.input)):
		exit("The input file does not exists.")

	if(args.max_iterations < 0):
		exit("The -max-iterations option have to be more or equal to 0.")

	args.input = os.path.abspath(args.input)
	output = os.path.dirname(args.input) + '/' + str(datetime.datetime.now()).replace('.','_').replace(':','-')

	pr = PageRank(args.input, output, args.max_iterations, args.trace)
	pr.start()

if __name__ == '__main__':	#If main function
	try:						#Try
		main()						#Run spark
	except KeyboardInterrupt:	#If Ctrl+C
		pass 						#Do nothing