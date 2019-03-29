#!/usr/bin/python

import sys
sys.path.append("../")
from utils.pagerankgraph import *
from string import ascii_uppercase

def createStarsGraph(G):
	for i in range(0,50):
		G.add_biderectional_link(str(i),'A')

	for i in range(50,100):
		G.add_biderectional_link(str(i),'B')

def createRingGraph(G):
	G.add_biderectional_link('A','B')
	G.add_biderectional_link('B','C')
	G.add_biderectional_link('C','D')
	G.add_biderectional_link('D','E')
	G.add_biderectional_link('E','F')
	G.add_biderectional_link('F','G')
	G.add_biderectional_link('G','H')
	G.add_biderectional_link('H','I')
	G.add_biderectional_link('I','J')
	G.add_biderectional_link('J','K')
	G.add_biderectional_link('K','L')
	G.add_biderectional_link('L','M')
	G.add_biderectional_link('M','N')
	G.add_biderectional_link('N','O')
	G.add_biderectional_link('O','A')

def createRandomGraph(G):
	NB_CONNECTIONS = 1000
	MAX_COMMITS = 100
	REPOS = []
	USERS = []

	for letter in ascii_uppercase:
		REPOS.append(letter)

	for user in range(500):
		USERS.append(str(user))

	for i in range(NB_CONNECTIONS):
		user = random.choice(USERS)
		repo = random.choice(REPOS)
		commits = random.randint(0, MAX_COMMITS)
		G.add_biderectional_link(user,repo, w=commits)

G = PageRankGraph('example.db')
createRandomGraph(G)
G.stabilize(1/100, 1/1000, plot=True)
G.close()
G.plot()