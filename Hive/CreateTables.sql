-- Table structure for movies data set (tab separated)
CREATE TABLE Movies
  (MovieId INT, Title STRING, Genres ARRAY<STRING>)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  COLLECTION ITEMS TERMINATED BY '|'
  LINES TERMINATED BY '\n';

-- Table structure for movie ratings data set (tab separated)
CREATE TABLE Ratings
  (UserId INT, MovieId INT, Rating FLOAT, Time INT)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n';
 
  
-- Load movies dataset into the table structure
LOAD DATA INPATH '/movielens/movies_tab.dat' INTO TABLE Movies;

-- Load ratings dataset into the table structure
LOAD DATA INPATH '/movielens/ratings_tab.dat' INTO TABLE Ratings;

-- Check data load
SELECT * FROM Movies LIMIT 4;
SELECT UserId, MovieId, Rating, Time FROM Ratings LIMIT 5;

-- Number of ratings per user
SELECT UserID, COUNT(*) FROM Ratings GROUP BY UserId;

-- Average rating per movie
INSERT OVERWRITE LOCAL DIRECTORY '/root/raul/result'
SELECT Movies.MovieId, Title, AVG(Rating) FROM Movies
  JOIN Ratings ON (Movies.MovieId = Ratings.MovieId)
  GROUP BY Movies.MovieId, Title;

