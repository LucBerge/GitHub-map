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
	WITH test AS
		(SELECT committer.email, repo_name
		FROM `bigquery-public-data.github_repos.commits`
		WHERE committer.email = '08055bfebc16ce75e33e85a3c3bec15bda3b6300@nslu2-linux.org'
		LIMIT 10)
	SELECT email, repo, COUNT(email) AS commits
	FROM test
	CROSS JOIN UNNEST(repo_name) AS repo
	GROUP BY email, repo
	"""

#https://cloud.google.com/bigquery/docs/reference/standard-sql/arrays
	
	rows = service.query(QUERY)
	output = open("input.txt","w")
	for row in rows:
		output.write(row['email'] + '\t' + row['repo'] + '\t' + str(row['commits']) + '\n')
	output.close()

if __name__ == "__main__":
	try:
		query()
	except KeyboardInterrupt:
		pass
	except:
		traceback.print_exc()