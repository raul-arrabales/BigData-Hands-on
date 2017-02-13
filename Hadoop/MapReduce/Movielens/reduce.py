#!/usr/bin/env python

import sys

(lastUserId, total) = (None, 0)

for line in sys.stdin:
  (userId, count) = line.split('\t')
  if lastUserId and userId != lastUserId:
    print '%s\t%d' % (lastUserId, total)
    (lastUserId, total) = (userId, int(count))
  else:
    (lastUserId, total) = (userId, total + int(count))

  if lastUserId:
    print '%s\t%d' % (lastUserId, total)
