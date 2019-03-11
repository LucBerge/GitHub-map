#!/usr/bin/python

import os, traceback
from google.cloud import bigquery

MAX_QUERY_SIZE = 100 #Go
GOOGLE_KEY = "../kaggle-service.json"
out = os.popen('echo $GOOGLE_APPLICATION_CREDENTIALS').read()
assert (GOOGLE_KEY in out), 'Please type [export GOOGLE_APPLICATION_CREDENTIALS=' + GOOGLE_KEY + '] in the console.'

def getRepos():
	service = bigquery.Client()

	QUERY = """
	SELECT repo_name
	FROM `bigquery-public-data.github_repos.commits`
	LIMIT 10
	"""

	results = service.query(QUERY)

	for row in results:
		print(row[0])

def main():
	getRepos()

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		None
	except:
		traceback.print_exc()