# Author: Amauri
# Date: 29/04/2019
#
# Project to learn and develop data pipeline with Apache Beam and Python.

from __future__ import print_function
import apache_beam as beam


# This function processes each row of .csv file and return a list with UF initials.
def splitline(row):
	parse = row.split(";")
	uf = parse[9] #Sigla de UF
	return (uf)


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

result = counts	| "max" >> beam.FlatMap(lambda a,b: a if a[1] > b[1] else b)

# Print result.
result |  "print" >> beam.Map(lambda row: print("%s : %d" %(row[0], row[1])))

# Run the pipeline.
pipeline.run()

