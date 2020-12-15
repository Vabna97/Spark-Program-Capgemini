// Databricks notebook source
// Find out the records with time stamp Pacific/Port_Moresby and altitude 10
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/7997667546876632/4476577809800087/2675886973836102/latest.html
//  /FileStore/tables/airports.text
val data= sc.textFile("/FileStore/tables/airports.text")
data.take(1)

// COMMAND ----------

val rddAirport = data.filter( line => {(line.split(",")(11) == "\"Pacific/Port_Moresby\"") && (line.split(",")(9).toInt %2==0)})
rddAirport.collect()
