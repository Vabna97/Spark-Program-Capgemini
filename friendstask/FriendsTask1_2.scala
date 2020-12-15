// Databricks notebook source
//  /FileStore/tables/FriendsData.csv
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/7997667546876632/4476577809800074/2675886973836102/latest.html
val friendrdd=sc.textFile("/FileStore/tables/FriendsData.csv")
friendrdd.take(5)

// COMMAND ----------

// Remove header
val friendRmHead=friendrdd.filter( line => !line.contains("Age"))

// COMMAND ----------

friendRmHead.take(5)

// COMMAND ----------

// splitting data to form key value within another key value pair (age,(1,friends))
val splitfriend=friendRmHead.map(x => (x.split(",")(2).toInt,(1,x.split(",")(3).toInt)))

// COMMAND ----------

// calculate the  occurence of an age and number of friends at that age
val reduceFriend = splitfriend.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
reduceFriend.take(5)

// COMMAND ----------

// finding the average number of friends for each age group
val finalfriend=reduceFriend.mapValues(data=> data._2/data._1)

// COMMAND ----------

// DBTITLE 1,Average no. of friends for each age group
println("Age : Friends")
for((x,y)<-finalfriend.collect()) println(x+"  : "+y)

// COMMAND ----------

// forming key value pair (age,friends)
val splitfriend2=splitfriend.map{case (x,y) => (x,y._2)}

// COMMAND ----------

splitfriend2.take(5)

// COMMAND ----------

// finding the maximum number of friends for each given age
val maxFriend=splitfriend2.reduceByKey(math.max(_, _))

// COMMAND ----------

val eachagemax=maxFriend.sortByKey()

// COMMAND ----------

// DBTITLE 1,Display maximum number of friends for each given age
println("Age : Friends")
for((x,y)<-eachagemax.collect()) println(x+" : "+y)
