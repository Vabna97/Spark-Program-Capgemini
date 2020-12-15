// Databricks notebook source
// Task to fetch the records whose latitude>40 or country is Iceland
//  /FileStore/tables/airports.text
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/7997667546876632/4476577809800072/2675886973836102/latest.html
val data= sc.textFile("/FileStore/tables/airports.text")
data.take(1)

// COMMAND ----------

// data is filtered to fetch the records whose latitude>40 or country is Iceland
val rddAirport = data.filter( line => {(line.split(",")(3) == "\"Iceland\"") || (line.split(",")(6)>"\"40\"")})
rddAirport.take(5)

// COMMAND ----------

// The records of the rdd containing the required records are stored as a textfile with name  CountryIsland.csv
rddAirport.saveAsTextFile("countryIsland.csv")
