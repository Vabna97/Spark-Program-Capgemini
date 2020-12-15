// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/7997667546876632/4237932506858455/2675886973836102/latest.html
// /FileStore/tables/numberData.csv
val numrdd=sc.textFile("/FileStore/tables/numberData.csv")

// COMMAND ----------

numrdd.take(5)

// COMMAND ----------

val head1=numrdd.first

// COMMAND ----------

// Removing header from the data
val numfinal=numrdd.filter(x=>x!=head1)

// COMMAND ----------

numfinal.take(5)

// COMMAND ----------

val numagain=numfinal.map(x => x.toInt)

// COMMAND ----------

//finding which number is prime
def isPrime(n: Int): Boolean = n >= 2 && (2 to math.sqrt(n).toInt).forall(n %_ != 0)

// COMMAND ----------

// all prime numbers are stored in primerdd
val primerdd=numagain.filter(isPrime)

// COMMAND ----------

// computing the sum of all the prime numbers and storing in primesum and printing it
val primesum=primerdd.reduce(_+_)
System.out.println("Sum: "+primesum)
