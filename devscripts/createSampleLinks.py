#!/usr/bin/python

import sys

try:		
	input = open(sys.argv[1],"rU")
	output = open(sys.argv[2],"w")
	limit = int(sys.argv[3])
	for line in input:
		line_split = line.split('\t')
		if(len(line_split) == 3):
			if(int(line_split[2]) >= limit):
				output.write(line)
	input.close()
	output.close()
except:
	print("Usage <input> <output> <limit>")
