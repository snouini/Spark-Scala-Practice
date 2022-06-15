package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql._



object Quijote {
  


    val spark = SparkSession.builder().appName("spark-etl").master("local[*]").getOrCreate()

    val file = spark.read.text("src/main/resources/quijote.txt")

    val total_lines = file.count()

    println(s"Total lines =  ${total_lines}")      /* total of lines of elquijote.txt */
    file.show(10 , truncate = false)    /* Showing 10 first lines of text file without truncation*/
    file.show()              /* implementing show() without arguments returns a dataframe with top 20 truncated rows*/







}
