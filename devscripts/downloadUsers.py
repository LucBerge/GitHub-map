#!/usr/bin/python

from google.cloud import bigquery

service = bigquery.Client()

QUERY = """
	SELECT DISTINCT committer.email, committer.name
	FROM `bigquery-public-data.github_repos.commits`
	"""

rows = service.query(QUERY)
output = open("users.txt","w")
counter = 0
for row in rows:
	if(counter%10000 == 0):
		print(counter)
	output.write(row['email'].encode('utf8') + '\t' + row['name'].encode('utf8') + '\n')
	counter += 1
output.close()
