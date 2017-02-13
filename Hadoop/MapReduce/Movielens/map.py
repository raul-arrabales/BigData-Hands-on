#!/usr/bin/env python

import sys

for line in sys.stdin:
  (userId, movieId, rating, timeStamp) = line.split('::')
  print '%s\t%d' % (userId, 1)
  
