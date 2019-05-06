# Author: Amauri
# Date: 06/05/2019
#
# Project to learn and develop data pipeline with Apache Beam using dataset from MegaSena.

#Import modules.
from __future__ import print_function
import apache_beam as beam

#Create a list to store the results after sort.
sortedlist = []

#Function to split each line of .csv file.
def splitline(row):
	parse = row.split(";")
	dezena1 = parse[2]
	dezena2 = parse[3]
	dezena3 = parse[4]
	dezena4 = parse[5]
	dezena5 = parse[6]
	dezena6 = parse[7]
	return (dezena1+','+dezena2+','+dezena3+','+dezena4+','+dezena5+','+dezena6)

#Function to store and sort results in a list.
def sortresults(row):
	sortedlist.append(row) 
	sortedlist.sort(key=lambda x : x[1], reverse = True)


# Create data pipeline.
pipeline = beam.Pipeline()

# Read each line from .csv file.
lines = pipeline | "read" >> beam.io.ReadFromText('./data/mega.csv')

#Extract only the 6 draft numbers.
dezenas_linha = lines  | "splitline" >> beam.Map(splitline)

#Split 'dezenas_linha' in single numbers.
dezenas = dezenas_linha | "split" >> beam.FlatMap(lambda x: x.split(','))

#Map each number in a tuple.
pares = dezenas | "pair" >> beam.Map(lambda x: (x,1))

#Count the number of ocurrency of each drafted number.
somas = pares | "sum" >> beam.CombinePerKey(sum)

#Sort results.
somas | "sort" >> beam.ParDo(sortresults)

#Run pipeline.
pipeline.run()

#Print results.
for i in sortedlist:
	print('%s: %s' %(i[0], i[1]))