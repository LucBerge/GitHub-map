#!/usr/bin/python

import networkx as nx
from matplotlib import pyplot, collections
import numpy
from string import ascii_uppercase
import random

def plot(names, positions, weights, save_name=None):

	MIN_X, MIN_Y = numpy.amin(positions, axis=0)
	MAX_X, MAX_Y = numpy.amax(positions, axis=0)

	MIN_W = numpy.amin(weights)
	MAX_W = numpy.amax(weights)
		
	fig, ax = pyplot.subplots()

	circles = [pyplot.Circle((positions[i][0],positions[i][1]), radius=weights[i], linewidth=0) for i in range(len(names))]
	c = collections.PatchCollection(circles)
	ax.add_collection(c)

	for i in range(len(names)):
		ax.annotate(names[i], xy=(positions[i][0], positions[i][1]), fontsize=weights[i]*150, va="center", ha="center")

	ax.set_xlim(MIN_X - MAX_W, MAX_X + MAX_W)
	ax.set_ylim(MIN_Y - MAX_W, MAX_Y + MAX_W)
		
	ax.set_aspect(1)
	if(save_name):
		pyplot.savefig(save_name)
	else:
		pyplot.show()

	pyplot.clf()
	pyplot.close()



G=nx.Graph()

# G.add_edge('B','A')

# for i in range(0,50):
# 	G.add_edge(str(i),'A')

# for i in range(50,100):
# 	G.add_edge(str(i),'B')

# G.add_edge('A','B')
# G.add_edge('B','C')
# G.add_edge('C','D')
# G.add_edge('D','E')
# G.add_edge('E','F')
# G.add_edge('F','G')
# G.add_edge('G','H')
# G.add_edge('H','I')
# G.add_edge('I','J')
# G.add_edge('J','K')
# G.add_edge('K','L')
# G.add_edge('L','M')
# G.add_edge('M','N')
# G.add_edge('N','O')
# G.add_edge('O','A')

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
	G.add_edge(user,repo, w=commits)


### WITH PAGE RANK ###

nx.draw(G)

weight = nx.pagerank(G)
nodes = nx.spring_layout(G,k=0.15,iterations=1000, weight=weight.values())
plot(list(nodes.keys()), list(nodes.values()), list(weight.values()))


### WITHOUT PAGERANK ###

# nodes = nx.spring_layout(G,k=0.15,iterations=1000)
# weight = [1/len(nodes)] * len(nodes)
# plot(list(nodes.keys()), list(nodes.values()), weight, 'networkxPlot.png')

