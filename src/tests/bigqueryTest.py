#!/usr/bin/python

import os, traceback
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError

def estimation():
	LIMIT = 1
	OFFSET = 0
	QUERY = """
			SELECT committer.email, committer.name, repo_name
			FROM `bigquery-public-data.github_repos.commits`
			LIMIT """ + str(LIMIT) + """
			OFFSET """ + str(OFFSET)

	client = bigquery.Client()
	job_config = bigquery.QueryJobConfig()
	job_config.dry_run = True
	job_config.use_query_cache = False
	query_job = client.query((QUERY), job_config=job_config)

	assert query_job.state == 'DONE'
	assert query_job.dry_run

	print("This query will process {} bytes.".format(query_job.total_bytes_processed))

def query():
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
		estimation()
	except KeyboardInterrupt:
		pass
	except:
		traceback.print_exc()