#!/usr/bin/python

import sqlite3

class GitHubDatabase:

	# VARIABLES

	database = None
	cursor = None

	###########
	# BUILDER #
	###########

	def __init__(self, name):
		self.database = sqlite3.connect(name)
		self.cursor = self.database.cursor()

		self.cursor.execute('''CREATE TABLE IF NOT EXISTS repos
             (repo_name text NOT NULL,
             commits text,
             branches text,
             releases text,
             contributors text,
             issues text,
             pull_requests text,
             watchs text,
             stars text,
             forks text,
             age text,
             PRIMARY KEY(repo_name))''')

		self.cursor.execute('''CREATE TABLE IF NOT EXISTS users
             (email text NOT NULL,
             name text,
             age text,
             PRIMARY KEY(email))''')

		self.cursor.execute('''CREATE TABLE IF NOT EXISTS links
             (repo_name text NOT NULL,
             email text NOT NULL,
             commits int,
             PRIMARY KEY(repo_name, email),
             FOREIGN KEY(repo_name) REFERENCES repos(repo_name),
             FOREIGN KEY(email) REFERENCES users(email))''')

	def close(self):
		self.database.commit()
		self.database.close()

	###############
	# REPOS TABLE #
	###############

	def find_repo(self, repo_name):
		self.cursor.execute('''SELECT * FROM repos WHERE repo_name=?''', (repo_name,))
		result = self.cursor.fetchall()
		return result[0] if len(result) != 0 else None

	def insert_repo(self, repo_name, commits, branches, releases, contributors, issues, pull_requests, watchs, stars, forks, age):
		self.cursor.execute('''INSERT OR IGNORE INTO repos (repo_name, commits, branches, releases, contributors, issues, pull_requests, watchs, stars, forks, age) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (repo_name, commits, branches, releases, contributors, issues, pull_requests, watchs, stars, forks, age,))

	def update_repo(self, repo_name, commits, branches, releases, contributors, issues, pull_requests, watchs, stars, forks, age):
		self.cursor.execute('''UPDATE repos SET commits=?, branches=?, releases=?, contributors=?, issues=?, pull_requests=?, watchs=?, stars=?, forks=?, age=? WHERE repo_name=?''', (commits, branches, releases, contributors, issues, pull_requests, watchs, stars, forks, age, repo_name,))

	def remove_repo(self, repo_name):
		self.cursor.execute('''DELETE FROM repos WHERE repo_name=?''', (repo_name,))

	def get_repos(self):
		self.cursor.execute('''SELECT * FROM repos''')
		return self.cursor.fetchall()

	###############
	# LINKS TABLE #
	###############

	def find_link(self, repo_name, email):
		self.cursor.execute('''SELECT * FROM links WHERE repo_name=? AND email=?''', (repo_name, email,))
		result = self.cursor.fetchall()
		return result[0] if len(result) != 0 else None

	def insert_link(self, repo_name, email, commits=1):
		result = self.find_link(repo_name, email)
		if(not result):
			self.cursor.execute('''INSERT OR IGNORE INTO links (repo_name, email, commits) VALUES (?, ?, ?)''', (repo_name, email, commits,))
		else:
			self.update_link(repo_name, email, result[2]+commits)

	def update_link(self, repo_name, email, commits):
		self.cursor.execute('''UPDATE links SET commits=? WHERE repo_name=? AND email=?''', (commits, repo_name, email,))

	def remove_link(self, repo_name, email):
		self.cursor.execute('''DELETE FROM links WHERE repo_name=? AND email=?''', (repo_name, email))

	def get_links(self):
		self.cursor.execute('''SELECT * FROM links''')
		return self.cursor.fetchall()

	###############
	# USERS TABLE #
	###############

	def find_user(self, email):
		self.cursor.execute('''SELECT * FROM users WHERE email=?''', (email,))
		result = self.cursor.fetchall()
		return result[0] if len(result) != 0 else None

	def insert_user(self, email, name, age):
		self.cursor.execute('''INSERT OR IGNORE INTO users (email, name, age) VALUES (?, ?, ?)''', (email, name, age,))

	def update_user(self, email, name, age):
		self.cursor.execute('''UPDATE users SET name=?, age=? WHERE email=?''', (name, age, email,))

	def remove_user(self, email):
		self.cursor.execute('''DELETE FROM users WHERE email=?''', (email,))

	def get_user(self):
		self.cursor.execute('''SELECT * FROM users''')
		return self.cursor.fetchall()

	###############
	# USERS TABLE #
	###############

