#!/usr/bin/python

from google.cloud import bigquery

service = bigquery.Client()

QUERY = """
	WITH test AS
		(SELECT committer.email, repo_name
		FROM `bigquery-public-data.github_repos.commits`)
	SELECT email, repo, COUNT(email) AS commits
	FROM test
	CROSS JOIN UNNEST(repo_name) AS repo
	GROUP BY email, repo
	#HAVING COUNT(email) > 100
	"""

#https://cloud.google.com/bigquery/docs/reference/standard-sql/arrays
	
rows = service.query(QUERY)
output = open("links.txt","w")
counter = 0
for row in rows:
	if(counter%10000 == 0):
		print(counter)
	output.write(row['email'].encode('utf8') + '\t' + row['repo'].encode('utf8') + '\t' + str(row['commits']) + '\n')
	counter += 1
output.close()
