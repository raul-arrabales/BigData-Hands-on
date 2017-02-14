Ratings = LOAD '/movielens/ratings_tab.dat'
  USING PigStorage('\t')
  AS (UserId:int, MovieId:int, Rating:float, Ts:int);

ByUserId = GROUP Ratings BY UserId;

Karma = FOREACH ByUserId
  GENERATE group AS UserId, COUNT(Ratings);

DUMP Karma;
