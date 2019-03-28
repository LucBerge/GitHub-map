#!/usr/bin/python

from githubdatabase import *
import ast

from google.cloud import bigquery
import traceback, os, sqlite3

GOOGLE_KEY = "../kaggle-service.json"
out = os.popen('echo $GOOGLE_APPLICATION_CREDENTIALS').read()
assert (GOOGLE_KEY in out), 'Please type [export GOOGLE_APPLICATION_CREDENTIALS=' + GOOGLE_KEY + '] in the console.'

def addReposFromScrapping(db):
	file = open("data/repos_scrapping.txt","r")
	counter = 1

	for line in file.readlines():
		repo_name, values = line.split("\t")
		dictionnary = ast.literal_eval(values)

		print(counter)

		db.insert_repo(
			repo_name.replace("\"",""),
			dictionnary['commits'],
			dictionnary['branches'],
			dictionnary['releases'],
			dictionnary['contributors'],
			dictionnary['issues'],
			dictionnary['pull_requests'],
			dictionnary['watchs'],
			dictionnary['starts'],
			dictionnary['forks'],
			None)
		counter+=1

	file.close()

def addUsersFromCommits(db, offset=0):
	MAX = 10

	while(True):
		query = """
			SELECT repo_name, committer 
			FROM `bigquery-public-data.github_repos.commits`
			LIMIT """ + str(MAX) + """
			OFFSET """ + str(offset)
		
		service = bigquery.Client()
		results = service.query(query)

		nb_rows = 0

		#For each couple, add the new value in the table or add 1 to the number of commits
		for row in results:

			nb_rows+=1

			email = row['committer']['email']
			name = row['committer']['name']

			db.insert_user(email, name, None)

			for repo_name in row['repo_name']:
				db.insert_link(repo_name, email)

		if nb_rows != MAX:
			break

		offset += nb_rows
		print(offset)

if __name__ == '__main__':
	db = GitHubDatabase("GitHubMap.db")
	try:
		addUsersFromCommits(db, 300000)
	except KeyboardInterrupt:
		pass
	except :
		traceback.print_exc()

	db.close()