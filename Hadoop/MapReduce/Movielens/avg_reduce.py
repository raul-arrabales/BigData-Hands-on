#!/usr/bin/env python

import sys

(lastMovieId, movieTitle, rating, count) = (None, None, 0, 0)

for line in sys.stdin:
  (movieId, key, value) = line.strip().split('\t')
  if lastMovieId and movieId != lastMovieId:
    if count > 0:
      print '%s\t%s\t%f' % (lastMovieId, title, rating/count)
    lastMovieId = movieId
    if key == 'Title':
      (movieTitle, rating, count) = (value, 0, 0)
    elif key == 'Rating':
      (rating, count) = (float(value), 1)
  else:
    lastMovieId = movieId
    if key == 'Title':
      movieTitle = value
    elif key == 'Rating':
      (rating, count) = (rating + float(value), count + 1)

  if lastMovieId:
    if count > 0:
      print '%s\t%s\t%f' % (lastMovieId, title, rating/count)
      
   
