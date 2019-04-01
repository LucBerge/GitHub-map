#!/usr/bin/python

from __future__ import division
import sys, os, traceback, sys
from mrjob.job import *
from utils.githubdatabase import * 

def main(database, output):
	file = open(output, "a")
	db = GitHubDatabase(database)

	links = db.get_links()

	for repo, email, commits in links:
		file.write(str(repo) + '\t' + str(email) + '\t' + str(commits) + '\n')

	db.close()
	file.close()

########
# MAIN #
########

if __name__ == '__main__':
	try:
		if(len(sys.argv) == 3):
			if('.db' in sys.argv[1] and '.txt' in sys.argv[2]):
				main(sys.argv[1], sys.argv[2])
			else:
				print('Usage : python ' + str(__file__) + ' input.db output.txt')
		else:
			print('Usage : python ' + str(__file__) + ' input.db output.txt')
	except KeyboardInterrupt:
		pass
	except:
		traceback.print_exc()