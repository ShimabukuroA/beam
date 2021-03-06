# Author: Amauri
# Date: 06/05/2019
#
# Project to count the number of ocurrencies of couples of drafted numbers.

#Import modules.
from __future__ import print_function
import apache_beam as beam
from itertools import combinations

# Create a list to store the results after sort.
sortedlist = []

# Function to split each line of .csv file and return a list of all possibles combinations of 2 drafted numbers.
def splitline(row):
	parse = row.split(";")
	dezena1 = int(parse[2])
	dezena2 = int(parse[3])
	dezena3 = int(parse[4])
	dezena4 = int(parse[5])
	dezena5 = int(parse[6])
	dezena6 = int(parse[7])
	sorteio = [dezena1, dezena2, dezena3, dezena4, dezena5, dezena6]
	sorteio.sort()
	combinacoes = list(combinations(sorteio, 2))
	return combinacoes

# Function to store and sort results in a list.
def sortresults(row):
	sortedlist.append(row) 
	sortedlist.sort(key=lambda x : x[1], reverse = True)

# Function to split list of tuples.
def splitagain(row):
	for i in row:
		yield i

# Create data pipeline.
pipeline = beam.Pipeline()

# Read each line from .csv file.
lines = pipeline | "read" >> beam.io.ReadFromText('./data/mega.csv')

# Split each line from .csv file.
dezenas_linha = lines  | "splitline" >> beam.Map(splitline)

# Split all possibles combinations of 2 drafted numbers.
pares = dezenas_linha | "splitagain" >> beam.FlatMap(splitagain)

#Map each couple in a tuple.
mapping = pares | "pair" >> beam.Map(lambda x: (x,1))

#Count the number of ocurrency of each couple of draft numbers.
somas = mapping | "sum" >> beam.CombinePerKey(sum)

#Sort results.
somas | "sort" >> beam.ParDo(sortresults)

#Run pipeline.
pipeline.run()

#Print results.
for i in sortedlist:
	print('%s: %s' %(i[0], i[1]))