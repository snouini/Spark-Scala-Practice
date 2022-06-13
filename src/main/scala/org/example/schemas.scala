package org.example

import org.apache.spark.sql.{SparkSession, types, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._

object schemas {

  val schema1 = StructType(Array(StructField("author", StringType , false),
    StructField("title" , StringType , false), StructField("pages", IntegerType, false)))

  val schema2 = (StructType(Array(StructField("Id", IntegerType , true),
    StructField("First" , StringType , false), StructField("Last", StringType, false),
    StructField("Url", StringType, false), StructField("Published", StringType, false),
    StructField("Hits", IntegerType, false),StructField("Campaigns", ArrayType(StringType), false))))

  val schema3 = StructType(Array(StructField("year", StringType, true),
    StructField("industry_code_ANZSIC", StringType , true), StructField("industry_name_ANZSIC", StringType , true),
    StructField("rme_size_grp", StringType , true) , StructField("variable", StringType , true),
    StructField("value", StringType , true), StructField("Unit", StringType , true)))

}

