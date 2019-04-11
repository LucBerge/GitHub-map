#!/usr/bin/python

import sys, os

exists1 = os.path.isfile(sys.argv[1])
if(not exists1):
	print("Usage <input> <output>")
	sys.exit(-1)
	
input = open(sys.argv[1],"rU")
output = open(sys.argv[2],"w")

for line in input:
	line_split = line.split('\t')
	if(len(line_split) == 3):
		if(int(line_split[2]) > 50):
			output.write(line)

input.close()
output.close()
