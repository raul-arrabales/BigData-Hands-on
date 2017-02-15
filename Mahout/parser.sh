# Turn each movie into a different file
while IFS=' ' read id title genres;
  do echo "$genres" >> "$2/$id";
done < $1

# Then generate files with this script:
# mkdir files
# chmod 755 parser.sh
# ./parser.sh movies.dat files

