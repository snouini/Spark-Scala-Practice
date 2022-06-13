package org.example
import org.example.schemas._
import org.apache.spark.sql.{SparkSession, types, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types.IntegerType



object Chapter3 {

  def main(args : Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Chapter3")
      .master("local[*]").getOrCreate()

    import spark.implicits._

    val blogs = spark.read.schema(schema2)
      .json("src/main/resources/blogs.json")

    /* using expression to compute a value*/
    blogs.select(expr("Hits*2")).show(2)

    /* using col to compute a value */
    blogs.select(col("Hits")*2).show(2)

    blogs.withColumn("Big Hitters" , (expr("Hits > 10000"))).show()

    blogs.withColumn("AuthorId" ,
      (concat(expr("First"), expr("Last"), expr("Id")))).select("AuthorId").show()

    val blogRow = Seq(("Matei Zaharia","CA"),("Reynold Xin" , "CA"))

    val blogRowdf = blogRow.toDF("Author", "State")

    blogRowdf.show()


    val fs_df = spark.read.option("header","true").schema(schema3)
      .csv("src/main/resources/financial-survey.csv")

    fs_df.show(truncate = false)

    val new_fs_df = fs_df.withColumn("new_value" , $"value".cast(IntegerType)).drop(col("value"))

    new_fs_df.select("new_value").where($"new_value" > 5).show()

    val fs_df_ = new_fs_df.withColumn("Date" , lit("05-06-2022"))

    val fs_df_ts = fs_df_.withColumn("new_date" , to_timestamp(col("Date") , "MM/dd/yy"))
      .drop("Date")

    fs_df_ts.show()



  }
}