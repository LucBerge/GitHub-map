#!/usr/bin/python

from __future__ import division
from matplotlib import pyplot, collections, textpath, patches
import sqlite3, numpy, copy, os, math, random

class PageRankGraph:

	# CONSTANTS

	MAX_ITERATIONS = 2000

	DAMPING_FACTOR = 0.85
	SPEED = 0.5

	OUTPUT_FOLDER = None
	OUTPUT_EXTENTION = None

	# VARIABLES

	database = None
	cursor = None
	initialized = False
	iteration = 0

	###########
	# BUILDER #
	###########

	def __init__(self, name):
		self.database = sqlite3.connect(name)
		self.cursor = self.database.cursor()

		self.cursor.execute('''CREATE TABLE IF NOT EXISTS settings
				(id TEXT NOT NULL,
	             damping_factor FLOAT DEFAULT 0.85,
	             speed FLOAT DEFAULT 0.5,
	             output_folder TEXT DEFAULT "plot/",
	             output_extension TEXT DEFAULT ".svg",
	             iteration INT DEFAULT 0,
	             stabilized BOOLEAN DEFAULT 0,
	             PRIMARY KEY(id))''')

		self.cursor.execute('''INSERT OR IGNORE INTO settings(id) VALUES(0)''')

		self.cursor.execute('''CREATE TABLE IF NOT EXISTS nodes
	             (id TEXT NOT NULL,
	             x FLOAT,
	             y FLOAT,
	             w FLOAT,
	             PRIMARY KEY(id))''')

		self.cursor.execute('''CREATE TABLE IF NOT EXISTS links
	             (id1 TEXT NOT NULL,
	             id2 TEXT NOT NULL,
	             w INT DEFAULT 0,
	             m FLOAT DEFAULT 0,
	             PRIMARY KEY(id1, id2),
	             FOREIGN KEY(id1) REFERENCES nodes(id),
	             FOREIGN KEY(id2) REFERENCES nodes(id))''')

		#Add triggers to set the database to not stabilized after a new insert
		self.cursor.execute('''CREATE TRIGGER IF NOT EXISTS trigger_insert_node AFTER INSERT ON nodes
						BEGIN
							UPDATE settings SET stabilized=0 WHERE id=0;
						END''')
		self.cursor.execute('''CREATE TRIGGER IF NOT EXISTS trigger_insert_link AFTER INSERT ON links
						BEGIN
							UPDATE settings SET stabilized=0 WHERE id=0;
						END''')
		self.cursor.execute('''CREATE TRIGGER IF NOT EXISTS trigger_delete_node AFTER DELETE ON nodes
						BEGIN
							UPDATE settings SET stabilized=0 WHERE id=0;
						END''')
		self.cursor.execute('''CREATE TRIGGER IF NOT EXISTS trigger_delete_link AFTER DELETE ON links
						BEGIN
							UPDATE settings SET stabilized=0 WHERE id=0;
						END''')

		self.DAMPING_FACTOR = self.get_damping_factor()
		self.SPEED = self.get_speed()
		self.OUTPUT_FOLDER = self.get_output_folder()
		self.OUTPUT_EXTENTION = self.get_output_extention()

		self.iteration = self.get_iteration()

	def close(self):
		self.database.commit()
		self.database.close()

	##################
	# SETTINGS TABLE #
	##################

	def get_damping_factor(self):
		self.cursor.execute('''SELECT damping_factor FROM settings WHERE id=0''')
		return	self.cursor.fetchall()[0][0]

	def get_speed(self):
		self.cursor.execute('''SELECT speed FROM settings WHERE id=0''')
		return	self.cursor.fetchall()[0][0]

	def get_output_folder(self):
		self.cursor.execute('''SELECT output_folder FROM settings WHERE id=0''')
		return	self.cursor.fetchall()[0][0]

	def get_output_extention(self):
		self.cursor.execute('''SELECT output_extension FROM settings WHERE id=0''')
		return	self.cursor.fetchall()[0][0]

	def get_iteration(self):
		self.cursor.execute('''SELECT iteration FROM settings WHERE id=0''')
		return	self.cursor.fetchall()[0][0]

	def is_stabilized(self):
		self.cursor.execute('''SELECT stabilized FROM settings WHERE id=0''')
		return	self.cursor.fetchall()[0][0]

	def set_damping_factor(self, damping_factor):
		self.cursor.execute('''SELECT damping_factor FROM settings WHERE id=0''', (damping_factor,))

	def set_speed(self, speed):
		self.cursor.execute('''UPDATE settings SET speed=? WHERE id=0''', (speed,))

	def set_output_folder(self, output_folder):
		self.cursor.execute('''UPDATE settings SET output_folder=? WHERE id=0''', (output_folder,))

	def set_output_extension(self, output_extension):
		self.cursor.execute('''UPDATE settings SET output_extension=? WHERE id=0''', (output_extension,))

	def set_iteration(self, iteration):
		self.cursor.execute('''UPDATE settings SET iteration=? WHERE id=0''', (iteration,))

	def set_stabilized(self, stabilized):
		self.cursor.execute('''UPDATE settings SET stabilized=? WHERE id=0''', (stabilized,))

	###############
	# NODES TABLE #
	###############

	def find_node(self, id):
		self.cursor.execute('''SELECT x, y, w FROM nodes WHERE id=?''', (id,))
		result = self.cursor.fetchall()
		return result[0] if len(result) != 0 else [None, None, None]

	def insert_node(self, id, x, y, w):
		self.cursor.execute('''INSERT OR IGNORE INTO nodes (id, x, y, w) VALUES (?, ?, ?, ?)''', (id, x, y, w,))

	def update_node(self, id, x, y, w):
		self.cursor.execute('''UPDATE nodes SET x=?, y=?, w=? WHERE id=?''', (x, y, w, id,))

	def remove_node(self, id):
		self.cursor.execute('''DELETE FROM nodes WHERE id=?''', (id,))

	def get_nodes(self):
		self.cursor.execute('''SELECT id FROM nodes''')
		return [res[0] for res in self.cursor.fetchall()]

	###############
	# LINKS TABLE #
	###############

	def find_link(self, id1, id2):
		self.cursor.execute('''SELECT w, m FROM links WHERE id1=? AND id2=?''', (id1, id2,))
		result = self.cursor.fetchall()
		return result[0] if len(result) != 0 else [0, 0]

	def insert_link(self, id1, id2, w):
		self.cursor.execute('''INSERT OR IGNORE INTO links (id1, id2, w) VALUES (?, ?, ?)''', (id1, id2, w,))

	def update_link(self, id1, id2, m):
		self.cursor.execute('''UPDATE links SET m=? WHERE id1=? AND id2=?''', (m, id1, id2,))

	def remove_link(self, id1, id2):
		self.cursor.execute('''DELETE FROM links WHERE id1=? AND id2=?''', (id1, id2))

	def get_links(self):
		self.cursor.execute('''SELECT * FROM links''')
		return [res for res in	self.cursor.fetchall()]

	#######################
	# INTERFACE FUNCTIONS #
	#######################

	def add_biderectional_link(self, id1, id2, w=1):
		self.add_directional_link(id1, id2, w)
		self.add_directional_link(id2, id1, w)

	def add_directional_link(self, id1, id2, w=1):
		self.insert_node(id1, None, None, None)
		self.insert_node(id2, None, None, None)

		old_w = self.find_link(id1, id2)[0]

		if(old_w):
			self.update_link(id1, id2, old_w+w)
		else:
			self.insert_link(id1, id2, w)

	###################
	# GRAPH FUNCTIONS #
	###################

	def init_graph(self):
		print("Creating the graph...")

		self.nodes = self.get_nodes()
		self.N = len(self.nodes)

		#Weight of every nodes
		self.W = numpy.full((self.N, 1), 1)

		#Position of every nodes
		self.P = (numpy.random.rand(self.N,2)-0.5)*self.N

		#Connections between nodes
		A = numpy.zeros((self.N, self.N))

		links = self.get_links()
		for id1, id2, w, m in links:
			index_id1 = self.nodes.index(id1)
			index_id2 = self.nodes.index(id2)
			A[index_id1][index_id2] = w

		self.M = A/numpy.sum(A, axis=1)
		return self.M, self.W, self.P

	def load_graph(self):
		print("Loading the graph...")

		self.nodes = self.get_nodes()
		self.N = len(self.nodes)

		#Weight of every nodes
		self.W = numpy.zeros((self.N,1))

		#Position of every nodes
		self.P = numpy.zeros((self.N,2))

		#Links between every nodes
		self.M = numpy.zeros((self.N, self.N))

		for i in range(self.N):
			self.P[i][0], self.P[i][1], self.W[i][0] = self.find_node(self.nodes[i])
			for j in range(self.N):
				self.M[i][j] = self.find_link(self.nodes[i], self.nodes[j])[1]

		return self.M, self.W, self.P

	def save_graph(self):
		print("Saving the graph...")
		self.set_iteration(self.iteration)

		for i in range(self.N):
			self.update_node(self.nodes[i], self.P[i][0], self.P[i][1], self.W[i][0])
			for j in range(self.N):
				self.update_link(self.nodes[i], self.nodes[j], self.M[i][j])

	###########################
	# STABILIZATION FUNCTIONS #
	###########################

	def stabilize(self, pagerank_error=1, spring_layout_error=100, pagerank_enabled=True, spring_layout_enabled=True, plot=False):
		
		if (not os.path.exists(self.OUTPUT_FOLDER)) and plot:
			print("Creating output folder...")
			os.makedirs(self.OUTPUT_FOLDER)

		if not self.is_stabilized():
			self.init_graph()
		else:
			self.load_graph()

		print("Stabilization in progress...")
		print('0/' + str(self.MAX_ITERATIONS))

		pagerank_stabilized = spring_layout_stabilized = False

		for i in range(self.MAX_ITERATIONS):
			if(plot):
				self.plot(str(i) + self.OUTPUT_EXTENTION)

			if(pagerank_enabled and not pagerank_stabilized):
				self.W, pagerank_stabilized = self.step_size(pagerank_error)

			if(spring_layout_enabled and not spring_layout_stabilized):
				self.P, spring_layout_stabilized = self.step_move(spring_layout_error)

			self.iteration+=1
			print('\033[F' + str(i+1) + '/' + str(self.MAX_ITERATIONS))

			if(pagerank_stabilized and spring_layout_stabilized):
				break

		self.save_graph()
		self.set_stabilized(True)

		return self.M, self.W, self.P, self.iteration

	################
	# PAGE RANKING #
	################

	def step_size(self, pagerank_error):
		Wn = numpy.matmul(self.M, self.W)*self.DAMPING_FACTOR + numpy.full((self.N, 1),1-self.DAMPING_FACTOR)
		stabilized = self.is_pagerank_stabilized(self.W, Wn, pagerank_error)
		return Wn, stabilized

	def is_pagerank_stabilized(self, W, Wn, error):
		return (numpy.sum(numpy.absolute(numpy.subtract(W, Wn)))) < error

	######################
	# GRAPH POSITIONNING #
	######################

	def step_move(self, spring_layout_error):

		V = Va = Vr = numpy.zeros((self.N, 2))
		for i in range(self.N):

			Va = Vr = numpy.zeros((self.N, 2))
			number_of_attractions = 0
			number_of_repulsions = 0

			n = 0
			for j in range(self.N):
				if(i != j):
					R = max(self.W[i][0], self.W[j][0])
					r = min(self.W[i][0], self.W[j][0])
					target = R + 2*r
					x = self.get_distance(self.P[i], self.P[j])

					if(target < x and self.M[i][j] != 0):	# If attraction
						Va[i] += self.get_unit_vector(self.P[i], self.P[j])*self.getForce(r, R, x)
						number_of_attractions+=1
					elif x < target:
						Vr[i] -= self.get_unit_vector(self.P[i], self.P[j])*self.getForce(r, R, x)
						number_of_repulsions += 1

			if number_of_repulsions != 0:
				V[i] +=  Vr[i]/number_of_repulsions*self.SPEED

			if number_of_attractions != 0 :
				V[i] += Va[i]/number_of_attractions*self.SPEED


		Pn = numpy.add(self.P, V)
		stabilized = self.is_move_stabilized(self.P, Pn, spring_layout_error)
		return Pn, stabilized

	def getForce(self, r, R, x):
		if x <= R or R+4*r <= x:
			return r
		else:
			# return -math.sqrt((2*r)**2 - (2*r+R-x)**2)/2 + r # Hyperbolic function
			return abs(x/2 - (r+ R/2))	#Linear function

	def get_unit_vector(self, P1, P2):
		return (P2-P1)/self.get_distance(P1, P2)

	def get_distance(self, P1, P2):
		return math.sqrt((P1[0]-P2[0])**2 + (P1[1]-P2[1])**2)

	def is_move_stabilized(self, P, Pn, error):
		return False not in (numpy.absolute(numpy.subtract(P, Pn)) < self.W/error)
	
	###########
	# PLOTING #
	###########

	def plot(self, save_name=None):

		MIN_X, MIN_Y = numpy.amin(self.P, axis=0)
		MAX_X, MAX_Y = numpy.amax(self.P, axis=0)

		MIN_W = numpy.amin(self.W)
		MAX_W = numpy.amax(self.W)
		
		fig, ax = pyplot.subplots()

		circles = [pyplot.Circle((self.P[i][0],self.P[i][1]), radius=self.W[i], linewidth=0) for i in range(self.N)]
		c = collections.PatchCollection(circles)
		ax.add_collection(c)

		for i in range(self.N):
			ax.annotate(self.nodes[i], xy=(self.P[i][0], self.P[i][1]), fontsize=self.W[i]*150/self.N, va="center", ha="center")

		ax.set_xlim(MIN_X - MAX_W, MAX_X + MAX_W)
		ax.set_ylim(MIN_Y - MAX_W, MAX_Y + MAX_W)
		
		ax.set_aspect(1)
		if(save_name):
			pyplot.savefig(self.OUTPUT_FOLDER + save_name)
		else:
			pyplot.show()

		pyplot.clf()
		pyplot.close()
