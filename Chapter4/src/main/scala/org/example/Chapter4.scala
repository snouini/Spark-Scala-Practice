package org.example

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.example.schemas
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Chapter4 {
  case class Controls (table: String)

  def main(args: Array[String]): Unit = {

   val spark = SparkSession.builder().appName("Chapter4").master("local[*]").getOrCreate()

   val depDelaysDF = spark.read.option("header","true")
     .option("delimiter",",")
     .format("csv").load("Chapter4/src/main/resources/departuredelays.csv")

   depDelaysDF.createOrReplaceTempView("us_delay_flights_table")

    // Queries using spark sql
   spark.sql(
     """SELECT distance, origin, destination from us_delay_flights_table
       where distance > 1000 Order by distance DESC""").show(10)

   spark.sql(
     """SELECT delay, origin , destination from us_delay_flights_table
       where origin = 'SFO' AND destination = 'ORD' AND delay > 120 ORDER BY delay DESC  """)
     .show(10)


   spark.sql(
     """SELECT delay , origin , destination,
     CASE
       WHEN delay > 360 THEN 'Very long delay'
       WHEN delay > 120 and delay < 360 THEN 'long delay'
       WHEN delay > 60 and delay < 120 THEN 'Short delay'
       WHEN delay > 0 and delay < 60 THEN 'Tolerable delay'
       WHEN delay = 0 THEN 'No delay'
     ELSE 'Early'
     END AS Flight_Delays
     FROM us_delay_flights_table
     ORDER BY origin, delay DESC"""
   ).show(10)

   // Same above queries with DataFrame API

    depDelaysDF.select("distance", "origin" , "destination")
      .where("distance > 1000").show(10)
    depDelaysDF.select("delay" ,"origin" , "destination")
      .where("origin = 'SFO' and destination = 'ORD' and delay > 120 ")
      .orderBy(desc("delay"))
      .show(10)
    depDelaysDF.select("delay", "origin", "destination")
      .withColumn("Flight_delays" ,
        when(col("delay") > 360 , "Very long delay")
          .otherwise(when(col("delay") > 120 &&
            col("delay") < 360 , "Long delay")
            .otherwise(when(col("delay") > 60
              && col("delay") < 120 , "Short delay").
              otherwise((when(col("delay") > 0
                              && col("delay") < 60 , "Tolerable delay")
                .otherwise("No delay"))))))
      .orderBy(col("origin") , col("delay").desc)
      .show(10)

    spark.sql("""CREATE DATABASE learn_spark_db""")
    




   }











    //  val d1 = LocalDate.parse("2022-04-06")
    //  val d2 = LocalDate.parse("2022-05-05")


      //println(ChronoUnit.MONTHS.between(d2,d1))

    //val listMap2: ListMap[String, String] =
      //ListMap("VD"-> "Vanilla Donut", "GD" -> "Glazed Donut") += ("KD" -> "Krispy Kreme Donut")







}
