#!/usr/bin/python

import traceback
from google.cloud import bigquery

def estimation(QUERY):
	client = bigquery.Client()
	job_config = bigquery.QueryJobConfig()
	job_config.dry_run = True
	job_config.use_query_cache = False
	query_job = client.query((QUERY), job_config=job_config)
	assert query_job.state == 'DONE'
	assert query_job.dry_run
	print("This query will process {} bytes.".format(query_job.total_bytes_processed))

def query(QUERY):
	service = bigquery.Client()
	rows = service.query(QUERY)

	counter = 0
	for row in rows:
		print(row)
		counter += 1
		if (counter == 100):
			break

def main():
	QUERY = """
		SELECT committer.email, committer.name, repo_name
		FROM `bigquery-public-data.github_repos.commits`
		WHERE RAND() < 1000000/(SELECT COUNT(*) FROM `bigquery-public-data.github_repos.commits`)
		"""

	QUERY = """
		WITH links AS
			(SELECT committer.email, repo_name
			FROM `bigquery-public-data.github_repos.commits`)
		SELECT email, repo, COUNT(email) AS commits
		FROM links
		CROSS JOIN UNNEST(repo_name) AS repo
		GROUP BY email, repo
		#HAVING COUNT(email) > 100
		"""

	QUERY = """
		WITH test AS
			(WITH links AS
				(SELECT committer.email, repo_name
				FROM `bigquery-public-data.github_repos.commits`)
			SELECT COUNT(*)
			FROM links
			CROSS JOIN UNNEST(repo_name) AS repo
			GROUP BY email, repo)
		SELECT COUNT(*)
		FROM test
		"""

	#query(QUERY)
	estimation(QUERY)


if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		pass
	except:
		traceback.print_exc()