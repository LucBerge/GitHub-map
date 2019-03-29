#!/usr/bin/python

import sys, traceback
sys.path.append("../")
from utils import githubscrapper

try:
	torvalds = githubscrapper.ReposScrapper('torvalds/linux')
	torvalds.scrap(True)
	print(torvalds)
except :
	traceback.print_exc()

try:
	fake = githubscrapper.ReposScrapper('fake/repo')
	fake.scrap(True)
	print(fake)
except :
	traceback.print_exc()
