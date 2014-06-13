#!/usr/local/bin/python

import collections, sys, string

country_id = 15

flag = 'RANGES'

g = open(sys.argv[2], "w")
with open(sys.argv[1]) as f:
	first_line = f.readline()
	for m_line in f:
		line = m_line.replace("\n","")
		values = line.split(',')
		myString = format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (flag, country_id, country_id, values[2], values[3],
											values[4], values[5], values[0], values[6], values[7]))
		g.write(myString)
f.close()
g.close()
