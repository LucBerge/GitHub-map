#!/usr/bin/python

from utils import pagerankgraph

ghm = pagerankgraph.PageRankGraph('example')

### START ###

for i in range(0,50):
	ghm.add_biderectional_link(str(i),'A')

#for i in range(50,100):
	#ghm.add_biderectional_link(str(i),'B')

### CIRCLE ###

# ghm.add_biderectional_link('A','B')
# ghm.add_biderectional_link('B','C')
# ghm.add_biderectional_link('C','D')
# ghm.add_biderectional_link('D','E')
# ghm.add_biderectional_link('E','F')
# ghm.add_biderectional_link('F','G')
# ghm.add_biderectional_link('G','H')
# ghm.add_biderectional_link('H','I')
# ghm.add_biderectional_link('I','J')
# ghm.add_biderectional_link('J','K')
# ghm.add_biderectional_link('K','L')
# ghm.add_biderectional_link('L','M')
# ghm.add_biderectional_link('M','N')
# ghm.add_biderectional_link('N','O')
# ghm.add_biderectional_link('O','A')

### RANDOM ###

# NB_CONNECTIONS = 1000
# MAX_COMMITS = 100
# REPOS = []
# USERS = []

# for letter in ascii_uppercase:
# 	REPOS.append(letter)

# for user in range(500):
# 	USERS.append(str(user))

# for i in range(NB_CONNECTIONS):
# 	user = random.choice(USERS)
# 	repo = random.choice(REPOS)
# 	commits = random.randint(0, MAX_COMMITS)
# 	ghm.add_biderectional_link(user,repo, w=commits)

### MAIN ###

#ghm.stabilize(1/100, 1/1000, move=False, plot=True)
ghm.stabilize(1/100, 1/1000, pageRank=False, plot=True)
ghm.plot()
ghm.close()