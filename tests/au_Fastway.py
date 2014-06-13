# read the file Transit Time Zipcodes - and put all the values in teh dictionary city_dict [key - city | value - list of zipcodes]
# argv[1] - Transit Times Zipcodes
# argv[2] - Transit times list
# service id for fastway - 1549
# au country id - 15

import collections, sys, string

city_dict = dict()

service_id = sys.argv[3]
from_ctry = sys.argv[4]
to_ctry = sys.argv[5]

with open(sys.argv[1]) as f:
	first_line = f.readline()
	count = 0
	for m_line in f:
		line = m_line.replace("\n","")
		values = line.split(',')
		if len(values) < 3:
			print "Invalid data"
		zip = values[0].strip()
		city = values[2].strip()
		if city in city_dict:
			city_dict[city].append(zip)
		else:
			count +=1
			city_dict[city] = [zip]
f.close()

g = open(sys.argv[6], "w")
with open(sys.argv[2]) as f1:
	first_line = f1.readline()
	for m_line in f1:
		line = m_line.replace("\n","")
		values = line.split(',')
		
		if len(values) != 4:
			print "invalid data"

		flag = 'DEFAULT'

		from_city = values[0].strip()
		to_city =  values[1].strip()
		min = values[2].strip()
		max = values[3].strip()
		
		if min == 'N/A' or max == 'N/A':
			continue

		if min == '' or max == '':
			continue

		if min == '0.5':
		   min = '1'
		
		if max == '0.5':
			max = '1'

		min_hours = int(min) * 24
		max_hours = int(max) * 24

		print max_hours
		if from_city in city_dict:
			from_zip_values = city_dict[from_city]
			for from_zip in from_zip_values:
				if(to_city in city_dict):
					to_zip_values = city_dict[to_city]
					for to_zip in to_zip_values:
						myString = format("%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\n" % (flag,from_ctry,to_ctry,from_zip,to_zip,service_id,min_hours,max_hours))
						g.write(myString)
				else:
					"to city not found"
		else:
			print "From city not found "
f1.close()		










