#!/usr/bin/python

import os, traceback
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError

def main():
	service = bigquery.Client()

	QUERY = """
	SELECT C.committer.email, C.committer.name, R.repo_name
	FROM `bigquery-public-data.github_repos.commits` C, `bigquery-public-data.github_repos.sample_repos` R
	WHERE R.repo_name IN UNNEST(C.repo_name)
	LIMIT 10
	OFFSET 0
	"""

	QUERY = """
	SELECT committer.email, committer.name, repo_name
	FROM `bigquery-public-data.github_repos.commits`
	WHERE RAND() < 1000000/(SELECT COUNT(*) FROM `bigquery-public-data.github_repos.commits`)
	"""

	results = service.query(QUERY)

	for row in results:
		print(row)

if __name__ == "__main__":
	try:
		main()
	# except DefaultCredentialsError:
	# 	print('You first have to set GOOGLE_APPLICATION_CREDENTIALS environnement variable.')
	# 	print('Open a console and type "export GOOGLE_APPLICATION_CREDENTIALS=path/to/google/credentials.json".')
	except KeyboardInterrupt:
		pass
	except:
		traceback.print_exc()