#!/usr/bin/python

from pagerankgraph.PageRankGraph import PageRankGraph

ghm = PageRankGraph('example')

for i in range(0,50):
	ghm.add_biderectional_link('A',str(i))

for i in range(50,100):
	ghm.add_biderectional_link('B',str(i))

ghm.stabilize(1/100, 1/1000, plot=True)
ghm.plot()
ghm.close()
