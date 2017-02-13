#!/usr/bin/env python

import sys

for line in sys.stdin:
  values = line.split('::')
  if len(values) == 3:
    print '%s\t%s\t%s' % (values[0], 'Title', values[1])
  elif len(values) == 4:
    print '%s\t%s\t%s' % (values[1], 'Rating', values[2])
    
