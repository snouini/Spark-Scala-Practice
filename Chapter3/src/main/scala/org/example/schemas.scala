package org.example

import org.apache.spark.sql.types._

object schemas {

  val schema1 = StructType(Array(StructField("author", StringType, false),
    StructField("title", StringType, false), StructField("pages", IntegerType, false)))

  val schema2 = (StructType(Array(StructField("Id", IntegerType, true),
    StructField("First", StringType, false), StructField("Last", StringType, false),
    StructField("Url", StringType, false), StructField("Published", StringType, false),
    StructField("Hits", IntegerType, false), StructField("Campaigns", ArrayType(StringType), false))))

  val schema3 = StructType(Array(StructField("CallNumber", IntegerType, true),
    StructField("UnitID", StringType, true), StructField("IncidentNumber", IntegerType, true),
    StructField("CallType", StringType, true), StructField("CallDate", StringType, true),
    StructField("WatchDate", StringType, true), StructField("CallFinalDisposition", StringType, true),
    StructField("AvailableDtTm", StringType, true), StructField("Address", StringType, true),
    StructField("City", StringType, true), StructField("Zipcode", IntegerType, true),
    StructField("Battalion", StringType, true), StructField("StationArea", StringType, true),
    StructField("Box", StringType, true), StructField("OriginalPriority", StringType, true),
    StructField("Priority", StringType, true), StructField("FinalPriority", IntegerType, true),
    StructField("ALSUnit", BooleanType, true), StructField("CallTypeGroup", StringType, true),
    StructField("NumAlarms", IntegerType, true), StructField("UnitType", StringType, true),
    StructField("UnitSequenceInCallDispatch", IntegerType, true),
    StructField("FirePreventionDistrict", StringType, true), StructField("SupervisorDistrict", StringType, true),
    StructField("Neighborhood", StringType, true), StructField("Location", StringType, true),
    StructField("RowID", StringType, true), StructField("Delay", FloatType, true)))

  case class DeviceIoTData(battery_level: Long, c02_level: Long,
                           cca2: String, cca3: String, cn: String, device_id: Long,
                           device_name: String, humidity: Long, ip: String, latitude: Double,
                           lcd: String, longitude: Double, scale: String, temp: Long,
                           timestamp: Long)

  case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long,
                                 cca3: String)

  case class DeviceCo2ByCountry (c02_level : Long, cn : String)
}
