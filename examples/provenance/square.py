import sys

filename = sys.argv[1]

## read in an existing data file
with open(filename, 'r') as f:
  data = [float(item) for item in f.read().split()]

## square all the elements
squares = [x**2 for x in data]

print " ".join((str(x) for x in squares))
