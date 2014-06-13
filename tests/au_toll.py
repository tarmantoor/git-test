# This script is used for processing AU toll data data
# argv[1] - CSV file
# argv[2] - shipping service id
# argv[3] - from country id
# argv[4] - to country id
# argv[7] - output file name

# how to run
# python au_toll.py Au_toll.csv  1552 15 15 au_toll

import collections, sys, string
from collections import OrderedDict
service_id = sys.argv[2]
from_ctry = sys.argv[3]
to_ctry = sys.argv[4]
flag = 'DEFAULT'

g = open(sys.argv[5], "w")
with open(sys.argv[1]) as f:
	first_row = f.readline()
	to_zip_row_1 = f.readline()
	to_zip_row = to_zip_row_1.replace("\n","")
	to_zip_array_1 = to_zip_row.split(',')
	to_zip_array_1.pop(0);
	to_zip_array = []
	print len(to_zip_array_1)
	
	for x in to_zip_array_1:
	    if x not in to_zip_array:
	         to_zip_array.append(x)
	
	idx = len(to_zip_array) - 1
	to_zip_array.pop(idx) 
	for m in to_zip_array:
		print m

	print len(to_zip_array)

	for m_line in f:
		i = 1
		line = m_line.replace("\n","")
		values = line.split(',')
		from_zip = values[0].strip()
		for val in to_zip_array:
			to_zip = val.strip()
			min = int(values[i].strip()) * 24
			max = int(values[i+1].strip()) * 24 
			myString = format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (flag,from_ctry,to_ctry,from_zip,to_zip,service_id,min,max))
			g.write(myString)
			i += 2
f.close()
g.close()
