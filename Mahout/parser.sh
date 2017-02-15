while IFS=' ' read id title genres;
do echo "$genres" >> $2/$id;
done < $1
