package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql._





object Mnmscount {

  def main(args : Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MnMcount")
    .master("local[*]").getOrCreate()

    import spark.implicits._

    /* Complete dataframe */
    val mnmdf = spark.read.format("csv").option("header","true")
    .option("inferSchema","true").load("src/main/resources/mnm_dataset.csv")

    /* Dataframe with column produced by groupby statement */
    val countmnm = mnmdf.select("State", "Color", "Count")
   .groupBy("State", "Color").agg(count("Color").alias("Total"))

    /* groupby with dataframe filtered by Canada State*/
    val Cacountmnm = mnmdf.select("State","Color","Count")
      .where(col("State") === "CA").groupBy("State","Color")
      .agg(count("Color").alias("Total")).orderBy(desc("Total"))

    /* groupby with dataframe filtered by WA, WY and UT States */
    val countmnm_WY_UT_WA = mnmdf.select("State","Color","Count")
      .where(col("State") === "WY" || col("State") === "UT" || col("State") === "WA")
      .groupBy("State","Color")
      .agg(count("Color").alias("Total")).orderBy(asc("Total"))

    /* groupby with multiple aggregation functions (CA dataframe)*/
    val Ca_multipleagg = mnmdf.select("State","Color","Count")
      .where(col("State") === "CA").groupBy("State" , "Color")
      .agg(min($"Count").alias("min"), max($"Count")
        .alias("Max") , round(avg($"Count"),2).alias("average"))

    Cacountmnm.show()
    countmnm_WY_UT_WA.show()
    Ca_multipleagg.show()



  }
}
