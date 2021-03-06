package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql._
import org.example.schemas._
import org.example.Chapter3_Datasets._

object Chapter3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Chapter3")
      .master("local[*]").getOrCreate()

    import spark.implicits._


    /* WORKING WITH BLOGS.CSV FILE */

    val blogs = spark.read.schema(schema2)
      .json("Chapter3/src/main/resources/blogs.json")

    /* using expression to compute a value*/
    blogs.select(expr("Hits*2")).show(2)

    /* using col to compute a value */
    blogs.select(col("Hits") * 2).show(2)

    blogs.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

    blogs.withColumn("AuthorId",
      (concat(expr("First"), expr("Last"), expr("Id")))).select("AuthorId").show()

    val blogRow = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))

    val blogRowdf = blogRow.toDF("Author", "State")

    blogRowdf.show()


   /* WORKING WITH SF-FIRE-CALLS.CSV */

    /* Loading csv file */
    val fire_df = spark.read.option("header", "true").schema(schema3)
      .csv("Chapter3/src/main/resources/sf-fire-calls.csv")

    /* writing the dataframe into a parquet file*/
    val fire_parquet_file = fire_df.write.format("parquet").save("Chapter3/src/main/" +
      "saved_files/parquet/sf-fire-parquet")

    val fire_parquet_table = "fire_parquet_table"
    fire_df.write.format("parquet").saveAsTable(fire_parquet_table)

    /* writing the dataframe into a csv file*/
    val fire_csv_file = fire_df.write.options(Map("header"->"true", "delimiter"->"," , "mode" ->"overwrite"))
      .csv("Chapter3/src/main/saved_files/csv/sf-fire-csv")

    /* writing the dataframe into an avro file*/
    val fire_avro_file = fire_df.write.format("avro").save("Chapter3/src/main/" +
      "saved_files/parquet/sf-fire-avro")



    val fewfiredf = fire_df.select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(col("CallType") =!= "Medical Incident")


    fewfiredf.show(5, false)

    fire_df.select("CallType").where(col("CallType").isNotNull)
      .agg(countDistinct("CallType") as "DistinctCallTypes").show()


    /*Renaming, adding and dropping columns*/

    val newfiredf = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")

    newfiredf
      .select("ResponseDelayedinMins")
      .where($"ResponseDelayedinMins" > 5)
      .show(5, false)

    /*Changing string date columns to timestamp*/

    val fireTsDF = newfiredf
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
        "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")

    fireTsDF.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, false)

    fireTsDF.select(year($"IncidentDate")).distinct().orderBy(year($"IncidentDate")).show()

    fireTsDF
      .select("CallType").where($"CallType".isNotNull)
      .groupBy("CallType").count().orderBy(desc("count"))
      .show()

    fireTsDF.select(F.sum("NumAlarms"), round(F.avg("ResponseDelayedinMins"), 2),
      round(F.min("ResponseDelayedinMins"), 2),
      round(F.max("ResponseDelayedinMins"), 2)).show()

    /* End-to-end example*/

    /* Different types of fire calls in 2018*/

    fireTsDF.select("CallType", "IncidentDate")
      .where(year($"IncidentDate") === 2018)
      .distinct().show()

    /*What months within the year 2018 saw the highest number of fire calls?*/

    fireTsDF.select("IncidentDate").where(year($"IncidentDate") === 2018)
      .groupBy(month($"IncidentDate")).count().orderBy(desc("count")).show()

    /* Which neighborhood in San Francisco generated the most fire calls in 2018? */

    fireTsDF.select("Neighborhood")
      .where(year($"IncidentDate") === 2018)
      .groupBy("Neighborhood").count().orderBy(desc("count")).show()

    /*Which week in the year in 2018 had the most fire calls?*/

    fireTsDF.select("IncidentDate", "CallType")
      .where(year($"IncidentDate") === 2018)
      .groupBy(weekofyear($"IncidentDate")).agg(count("CallType"))
      .orderBy(desc("count(CallType)")).show()

    /*Is there a correlation between neighborhood, zip code, and number of fire calls?*/

    /* SECOND PART - DATASETS */

    val iotds = getiotdevicesds()(spark : SparkSession )

    val result2 = iotds_tempfilter(iotds : Dataset[DeviceIoTData])(spark : SparkSession)

    /* Exercices - page 74 */

    val q1 = GetFailingDevices(iotds : Dataset[DeviceIoTData])(spark : SparkSession)

    val q2 = GetMinMax(iotds : Dataset[DeviceIoTData])(spark : SparkSession)





  }
}
