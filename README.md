# MovieRecommenderSystem

This is movie recommender system which can recommend movies for a user based on his movie history. It is implemented by Hadoop mapreduce. 

Input dataset(USERID, MOVIEID, RATING):
148884,1,3
822109,1,5
885013,1,4
205962,2,4
166634,2,3
175945,2,4
  .
  .
  .
  
Recommending List dataset(USERID MOVIEID:PROBABLITY OF RATING), The higher rating, the more likely to be recommended for this user.
101484	1:0.22
101484	10:0.16
101484	100:0.09
101484	11:0.2
101484	12:0.21
101484	13:0.28
101484	14:0.15
101484	15:0.25
101484	16:0.26
101484	17:0.52
101484	18:0.42
101484	19:0.29
101484	2:0.12
101484	20:0.16
  .
  .
  .
