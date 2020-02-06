# Spark-Recommender-System

## User Based CF
1. load data from Hive;
2. compute the recommendation list for each user;
3. write the result into Hive table;

## Item Based CF
1. load data from Hive
2. compute the similarity coefficient between each pair of item using adjusted cosine similarity
3. according to the item rated by user, predict the rating of items that hasn't been rate by user
4. sort the rating predictions for each user reversely