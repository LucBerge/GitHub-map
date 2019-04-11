#!/usr/bin/python

from google.cloud import bigquery

service = bigquery.Client()

QUERY = """
	SELECT committer.email, repo_name, COUNT(committer.email) AS commits
	FROM `bigquery-public-data.github_repos.sample_commits`
	GROUP BY committer.email, repo_name
	#HAVING COUNT(email) > 100
	"""

#https://cloud.google.com/bigquery/docs/reference/standard-sql/arrays
	
rows = service.query(QUERY)
output = open("sample_links.txt","w")
counter = 0
for row in rows:
	if(counter%10000 == 0):
		print(counter)
	output.write(row['email'].encode('utf8') + '\t' + row['repo_name'].encode('utf8') + '\t' + str(row['commits']) + '\n')
	counter += 1
output.close()
