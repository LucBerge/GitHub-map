#!/usr/bin/python

from google.cloud import bigquery
import traceback, os, sqlite3

GOOGLE_KEY = "../kaggle-service.json"
out = os.popen('echo $GOOGLE_APPLICATION_CREDENTIALS').read()
assert (GOOGLE_KEY in out), 'Please type [export GOOGLE_APPLICATION_CREDENTIALS=' + GOOGLE_KEY + '] in the console.'

def createDb():
	db = sqlite3.connect('gitHubMap.db')
	c = db.cursor()
	c.execute('''CREATE TABLE IF NOT EXISTS repos
             (repo_name text NOT NULL,
             commits text,
             branches text,
             releases text,
             contributors text,
             issues text,
             pull_requests text,
             watchs text,
             starts text,
             forks text,
             age text,
             PRIMARY KEY(repo_name))''')

	c.execute('''CREATE TABLE IF NOT EXISTS users
             (email text NOT NULL,
             name text,
             age text,
             PRIMARY KEY(email))''')

	c.execute('''CREATE TABLE IF NOT EXISTS repos_to_users
             (repo_name text NOT NULL,
             email text NOT NULL,
             commits int,
             PRIMARY KEY(repo_name, email),
             FOREIGN KEY(repo_name) REFERENCES repos(repo_name),
             FOREIGN KEY(email) REFERENCES users(email))''')

	db.commit()
	return db

def main():

	#Create the output files
	repos_file = open('repos.txt', 'w')
	users_file = open('users.txt', 'w')
	db = createDb()
	c = db.cursor()

	#Get repo_name and committer from all commits
	query = """
		SELECT repo_name, committer 
		FROM `bigquery-public-data.github_repos.commits`
		LIMIT 10
		"""
	service = bigquery.Client()
	results = service.query(query)

	#For each couple, add the new value in the table or add 1 to the number of commits
	for row in results:
		for repo_name in row['repo_name']:

			#Add the repo to the output file
			repos_file.write(repo_name.encode('ascii')+'\n')

			#Add the user to the output file
			users_file.write(row['committer']['email'].encode('ascii') + ';' + row['committer']['name'].encode('utf-8') + '\n')

			#Add a repo to user link
			c.execute('''SELECT commits
				FROM repos_to_users
				WHERE repo_name=? AND email=?''', (repo_name, row['committer']['email']))
			rows = c.fetchall()
			if(len(rows) != 0):	#If couple already exist
				c.execute('''UPDATE repos_to_users
					SET commits = ?
					WHERE repo_name=? AND email=?''', (rows[0][0]+1, repo_name, row['committer']['email']))
			else:
				c.execute('''INSERT INTO repos_to_users (repo_name, email, commits)
					VALUES (?, ?, ?)''', (repo_name, row['committer']['email'], 1))

	repos_file.close()
	users_file.close()
	db.commit()
	db.close()

if __name__ == '__main__':
	try:
		main()
	except KeyboardInterrupt:
		pass
	except :
		traceback.print_exc()