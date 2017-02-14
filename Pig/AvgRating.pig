Movies = LOAD '/movielens/movies_tab.dat'
  USING PigStorage('\t')
  AS (MovieId:int, Title:chararray, Genres:chararray);

Ratings = LOAD '/movielens/ratings_tab.dat'
  USING PigStorage('\t')
  AS (UserId:int, MovieId:int, Rating:float, Ts:int);
  
MovieRatings = JOIN Movies BY MovieId, Ratings BY MovieId;

ByMovieId = GROUP MovieRatings BY (Ratings::MovieId, Movies::Title);

AvgRating = FOREACH ByMovieId GENERATE FLATTEN(group), 
  AVG(MovieRatings.Ratings::Rating);

DUMP AvgRating;
