#!/usr/bin/python

from __future__ import division
from matplotlib import pyplot
import sqlite3, numpy, copy, os, math, random

class GitHubMap:

	# CONSTANTS
	MAX_ITERATIONS = 50
	WEIGHT_COMMIT = 1
	WEIGHT_ISSUE = 1
	SPEED = 10

	MIN_SIZE = 5
	MAX_SIZE = 50

	X_MIN = -MAX_SIZE
	X_MAX = MAX_SIZE
	Y_MIN = -MAX_SIZE
	Y_MAX = MAX_SIZE

	SAVE_FOLDER = 'plot/'
	SAVE_EXTENTION = '.png'
	DPI = 100

	# VARIABLE

	file = ""
	database = None
	scrapping = False

	def __init__(self, name, scrapping=False):

		self.file = str(name)+'.db'
		self.database = sqlite3.connect(self.file)
		c = self.database.cursor()
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
	             (user_name text NOT NULL,
	             name text,
	             age text,
	             PRIMARY KEY(user_name))''')

		c.execute('''CREATE TABLE IF NOT EXISTS contributions
	             (repo_name text NOT NULL,
	             user_name text NOT NULL,
	             commits int DEFAULT 0,
	             issues int DEFAULT 0,
	             PRIMARY KEY(repo_name, user_name),
	             FOREIGN KEY(repo_name) REFERENCES repos(repo_name),
	             FOREIGN KEY(user_name) REFERENCES users(user_name))''')
		self.database.commit()
		self.scrapping = scrapping

	######################
	# DATABASE FUNCTIONS #
	######################

	def repo_exists(self, repo_name):
		c = self.database.cursor()
		c.execute('''SELECT repo_name FROM repos WHERE repo_name=?''', (repo_name))
		return len(c.fetchall()) > 0

	def user_exists(self, user_name):
		c = self.database.cursor()
		c.execute('''SELECT user_name FROM users WHERE user_name=?''', (user_name))
		return len(c.fetchall()) > 0

	def contribution_exists(self, repo_name, user_name):
		c = self.database.cursor()
		c.execute('''SELECT commits, issues FROM contributions WHERE repo_name=? AND user_name=?''', (repo_name, user_name))
		return len(c.fetchall()) > 0

	def add_repo(self, repo_name):
		dict = {'commits':0, 'branches':0, 'releases':0, 'contributors':0, 'issues':0, 'pull_requests':0, 'watchs':0, 'starts':0, 'forks':0, 'age':-1}
		c = self.database.cursor()
		c.execute('''INSERT INTO repos (repo_name, commits, branches, releases, contributors, issues, pull_requests, watchs, starts, forks, age)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (repo_name, dict['commits'], dict['branches'], dict['releases'], dict['contributors'], dict['issues'], dict['pull_requests'], dict['watchs'], dict['starts'], dict['forks'], dict['age']))
		self.database.commit()

	def add_user(self, user_name):
		dict = {'name':0, 'age':-1}
		c = self.database.cursor()
		c.execute('''INSERT INTO users (user_name, name, age)
					VALUES (?, ?, ?)''', (user_name, dict['name'], dict['age']))
		self.database.commit()

	def add_commit(self, repo_name, user_name, commit=1):
		self.add_contribution(repo_name, user_name, commit=commit)

	def add_issue(self, repo_name, user_name, issue=1):
		self.add_contribution(repo_name, user_name, issue=issue)

	def add_contribution(self, repo_name, user_name, commit=0, issue=0):
		if not self.repo_exists(repo_name):
			self.add_repo(repo_name)

		if not self.user_exists(user_name):
			self.add_user(user_name)

		if(self.contribution_exists(repo_name, user_name)):
			c = self.database.cursor()
			c.execute('''UPDATE contributions SET commits = commits + ?, issues = issues + ? WHERE repo_name=? AND user_name=?''', (commit, issue, repo_name, user_name))
			self.database.commit()
		else:
			c = self.database.cursor()
			c.execute('''INSERT INTO contributions (repo_name, user_name, commits, issues) VALUES (?, ?, ?, ?)''', (repo_name, user_name, commit, issue))
			self.database.commit()

	def get_repo(self, repo_name):
		c = self.database.cursor()
		c.execute('''SELECT * FROM repos WHERE repo_name=?''', (repo_name))
		return c.fetchall()

	def get_repos(self):
		c = self.database.cursor()
		c.execute('''SELECT repo_name FROM repos''')
		return [res[0] for res in c.fetchall()]

	def get_user(self, user_name):
		c = self.database.cursor()
		c.execute('''SELECT * FROM users WHERE user_name=?''', (user_name))
		return c.fetchall()

	def get_users(self):
		c = self.database.cursor()
		c.execute('''SELECT user_name FROM users''')
		return [res[0] for res in c.fetchall()]

	def get_contribution(self, repo_name, user_name):
		c = self.database.cursor()
		c.execute('''SELECT * FROM contributions WHERE repo_name=? AND user_name=?''', (repo_name, user_name))
		return c.fetchall()

	def get_contributions(self):
		c = self.database.cursor()
		c.execute('''SELECT * FROM contributions''')
		return [res for res in c.fetchall()]

	###################
	# GRAPH FUNCTIONS #
	###################

	def init_graph(self):
		self.repos = self.get_repos()
		self.users = self.get_users()
		self.N = len(self.repos) + len(self.users)

		#Connections between nodes
		B = numpy.zeros((self.N, self.N))

		contributions = self.get_contributions()
		for repo_name, user_name, commits, issues in contributions:
			w = self.WEIGHT_COMMIT*commits + self.WEIGHT_ISSUE*issues
			index_repo = self.repos.index(repo_name)
			index_user = len(self.repos) + self.users.index(user_name)
			B[index_repo][index_user] = w
			B[index_user][index_repo] = w

		self.A = copy.deepcopy(B)
		for i in range(self.N):
			t = numpy.sum(B[i])
			for j in range(self.N):
				self.A[j][i] = B[i][j]/t

		#Weight of every nodes
		self.W = numpy.full((self.N, 1), 1/len(self.repos))

		#Position of every nodes
		self.P = (numpy.random.rand(self.N,2)-0.5)*self.MAX_SIZE*5

		return self.A, self.W, self.P

	def stabilize(self, error_size, error_move, plot=False):
		if not os.path.exists(self.SAVE_FOLDER):
			os.makedirs(self.SAVE_FOLDER)
		self.init_graph()
		iter_size = self.stabilize_size(error_size, plot)[2]
		iter_move = self.stabilize_move(error_move, plot, start_iter=iter_size)[1]
		return self.A, self.W, self.P, iter_size, iter_move

	def stabilize_size(self, error, plot=False, start_iter=0):
		for i in range(self.MAX_ITERATIONS):
			if(plot):
				self.plot(str(i+start_iter) + self.SAVE_EXTENTION)

			Wn = self.step_size()
			stop = self.is_size_stabilized(self.W, Wn, error)
			self.W = Wn

			if(stop):
				break

		return self.W, self.A, i+1

	def step_size(self):
		return numpy.matmul(self.A, self.W)

	def is_size_stabilized(self, W, Wn, error):
		return False not in (numpy.absolute(numpy.subtract(W, Wn)) < error)

	def stabilize_move(self, error, plot=False, start_iter=0):

		# Get the target
		T = self.get_target()
		
		for i in range(self.MAX_ITERATIONS):
			if(plot):
				self.plot(str(i+start_iter) + self.SAVE_EXTENTION)

			Pn = self.step_move(T)
			stop = self.is_move_stabilized(self.P, Pn, error)
			self.P = Pn

			if(stop):
				break

		return self.P, i+1

	def get_target(self):
		# Calculer S
		S = self.get_size(self.W)

		# Calculer T
		T = numpy.zeros((self.N, self.N))
		for i in range(self.N):
			for j in range(self.N):
				if(self.A[j][i] != 0):
					T[i][j] = S[i] + S[j]
		return T

	def is_move_stabilized(self, P, Pn, error):
		return False not in (numpy.absolute(numpy.subtract(P, Pn)) < error)

	def step_move(self, T):
		Pn = copy.deepcopy(self.P)

		#Get the real distance
		V = numpy.zeros((self.N, 2), dtype=numpy.ndarray)
		for i in range(self.N):
			n = 0
			for j in range(self.N):
				if(T[i][j] != 0):
					distance = self.get_distance(self.P[i][0], self.P[i][1], self.P[j][0], self.P[j][1])
					ratio = (distance - T[i][j])/max(T[i][j], distance)
					V[i][0] += (self.P[j][0]-self.P[i][0])*ratio
					V[i][1] += (self.P[j][1]-self.P[i][1])*ratio
					n+=1

			V[i][0] = V[i][0]/n
			V[i][1] = V[i][1]/n

		return numpy.add(self.P, V)

	def get_distance(self, x1, y1, x2, y2):
		res = math.sqrt((x1-x2)**2 + (y1-y2)**2)
		return res

	def plot(self, save_name=None):
		S = self.get_size(self.W)
		for i in range(self.N):

			if(i < len(self.repos)): #If it is a repo
				pyplot.plot(self.P[i][0],self.P[i][1], 'ro', markersize=S[i])
				pyplot.text(self.P[i][0],self.P[i][1], self.repos[i][0], fontsize=S[i]/2, horizontalalignment='center', verticalalignment='center')
			else:
				pyplot.plot(self.P[i][0],self.P[i][1], 'bo', markersize=S[i])
				pyplot.text(self.P[i][0],self.P[i][1], self.users[i - len(self.repos)][0], fontsize=S[i]/2, horizontalalignment='center', verticalalignment='center')

		pyplot.axis((-self.MAX_SIZE*5, self.MAX_SIZE*5, -self.MAX_SIZE*5, self.MAX_SIZE*5))
		
		if(save_name):
			pyplot.savefig(self.SAVE_FOLDER + save_name)
		else:
			pyplot.show()
		pyplot.clf()

	def get_size(self, W):
		min = numpy.amin(W)
		max = numpy.amax(W)

		S = numpy.zeros((self.N, 1))
		if(min != max):
			for i in range(self.N):
				S[i] = (W[i]-min)*(self.MAX_SIZE-self.MIN_SIZE)/(max-min) + self.MIN_SIZE
		else:
			for i in range(self.N):
				S[i] = W[i]*(self.MAX_SIZE-self.MIN_SIZE) + self.MIN_SIZE

		return S

########
# MAIN #
########

if __name__ == '__main__':
	ghm = GitHubMap('MonTest')
	ghm.add_commit('A','1', 1)
	ghm.add_commit('A','2', 1)
	ghm.add_commit('B','2', 2)
	ghm.add_commit('C','2', 3)
	ghm.add_commit('C','3', 1)
	ghm.add_commit('B','1', 2)

	print(ghm.stabilize(0.001, 0.1, True))
	ghm.plot()

