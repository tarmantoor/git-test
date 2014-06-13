# read the file Transit Time Zipcodes - and put all the values in teh dictionary city_dict [key - city | value - list of zipcodes]
# argv[1] - Transit Times Zipcodes
# argv[2] - Transit times list
# service id for fastway - 1549
# au country id - 15

#python au_Fastway_ranges.py Transit\ Times\ Zipcodes.csv Transit\ Times\ List.csv 1549 15 15

import collections, sys, string

service_id = sys.argv[3]
from_ctry = sys.argv[4]
to_ctry = sys.argv[5]

g = open("au_fastway_ranges","w")
with open(sys.argv[1]) as f:
	first_line = f.readline()
	second_line = f.readline()
	s_line = second_line.replace("\n","")
	array = s_line.split(',')
	zip_range_end = int(array[0].strip())
	zip_range_start = int(array[0].strip())
	for m_line in f:
		line = m_line.replace("\n","")
		values = line.split(',')

		if len(values) < 3:
			print "Invalid data"
		
		city = values[2].strip()
		current_zip = int(values[0].strip())
		
		if(current_zip == zip_range_end+1):
			zip_range_end = current_zip
		else:
			myStr = format("%s,%s,%s\n" % (zip_range_start,zip_range_end,city))
			g.write(myStr)
			zip_range_start = current_zip
			zip_range_end = current_zip
f.close()
g.close()

s = open("au_fastway_ranges_data", "w")
t = open("au_fastway_ranges_sbe", "w")
def convert(from_city, to_city, min, max):
	with open("au_ranges") as f1:
		for m_line in f1:
			if len(m_line) < 3:
				print(m_line)	
			line = m_line.replace("\n","")
			values = line.split(',')
			flag = 'RANGES'
			if(values[2].strip() == from_city):
				min_range_src = int(values[0])
				max_range_src = int(values[1])
				with open("au_ranges") as f2:
					for t_line in f2:
						if len(t_line) < 3:
							print(t_line)
						line_t = t_line.replace("\n","")
						values_t = line_t.split(',')
						if(values_t[2].strip() == to_city):
							min_range_dest = int(values_t[0])
							max_range_dest = int(values_t[1])
							myString = format("%s\t%s\t%s\t%d\t%d\t%d\t%d\t%s\t%d\t%d\n" % (flag,from_ctry,to_ctry,min_range_src,max_range_src,min_range_dest,max_range_dest,service_id,min,max))
							myStr = format("%s\t%s\t%s\t%d\t%d\t%s\t%d\t%d\n" % (flag,from_ctry,to_ctry,min_range_src,min_range_dest,service_id,min,max))
							s.write(myString)
							t.write(myStr)
				f2.close()				
	f1.close()


with open(sys.argv[2]) as f3:
	first_line = f3.readline()
	for m_line in f3:
		line = m_line.replace("\n","")
		values = line.split(',')
		
		if len(values) != 4:
			print "invalid data"

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
		convert(from_city, to_city, min_hours, max_hours)
f3.close()














