# Author: Amauri
# Date: 29/04/2019
#
# Project to learn and develop data pipeline with Apache Beam and Python with IES dataset.

from __future__ import print_function
import apache_beam as beam


sortedlist = []

# This function processes each row of .csv file and return a list with UF initials.
def splitline(row):
	parse = row.split(";")
	uf = parse[9] #Sigla de UF
	return (uf)

# This functions appends each row from PCollection "Counts" in a list and sort by number of IES.
def sortresults(row):
	sortedlist.append(row) # Each row follows the format: (UF, number of IES)
	sortedlist.sort(key=lambda x : x[1], reverse = True) # Sort list in decrescent order

# Create data pipeline.
pipeline = beam.Pipeline()

# Read each line from .csv file.
lines = pipeline | beam.io.ReadFromText('./data/cadastro_ies.csv')

# Get UF initials from each line .
words = lines  | "split" >> beam.Map(splitline)

# Mapping each UF initials in a tuple.
maps = words	| "map" >> beam.Map(lambda row: (row,1))

# Group by UF initials and count total number of educational institutions per UF.
counts = maps    | "group" >> beam.CombinePerKey(sum)

# Sorting results by number of IES. 
counts	| "sort" >> beam.ParDo(sortresults)

# Print result.
#counts |  "print" >> beam.Map(lambda row: print("%s : %s" %(row[0], row[1])))

# Run the pipeline.
pipeline.run()

#Print some results
print("UF com mais unidades de ensino superior:\n%s:%s unidades" % (sortedlist[0][0], sortedlist[0][1]))
