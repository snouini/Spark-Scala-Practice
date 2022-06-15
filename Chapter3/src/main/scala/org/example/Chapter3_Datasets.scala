package org.example
import org.apache.spark.sql.SparkSession
import org.example.schemas._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F }



object Chapter3_Datasets {


  def getiotdevicesds()(implicit spark: SparkSession) : Dataset[DeviceIoTData] = {

    import spark.implicits._

    val iotdevices_dataset = spark.read
      .json("Chapter3/src/main/resources/iot_devices.json").as[DeviceIoTData]

    iotdevices_dataset
  }

  def iotds_tempfilter(iotdevices_dataset : Dataset[DeviceIoTData])
                      (implicit spark : SparkSession) : Unit  = {

    iotdevices_dataset.filter(d => d.temp > 30 && d.humidity > 70).show()
     }

  def GetdsTempByCountry(iotdevices_dataset: Dataset[DeviceIoTData])
                        (implicit spark: SparkSession): Dataset[DeviceTempByCountry] = {
    import spark.implicits._

    iotdevices_dataset
      .filter(d => {
        d.temp > 25
      })
      .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
      .toDF("temp", "device_name", "device_id", "cca3").as[DeviceTempByCountry]
  }

  def GetFailingDevices(iotdevices_dataset: Dataset[DeviceIoTData])
              (implicit spark : SparkSession) : Unit = {
    import spark.implicits._

    iotdevices_dataset
      .filter(d=> d.battery_level < 1)
      .map(d => (d.device_id , d.device_name , d.battery_level))
      .toDF("device_id", "device_name", "battery_level").show()
  }

  def GetMinMax(iotdevices_dataset: Dataset[DeviceIoTData])
                       (implicit spark : SparkSession) : Unit = {
    import spark.implicits._

    iotdevices_dataset
      .map(d => (d.temp , d.c02_level , d.battery_level))
      .toDF("temp", "c02_level", "battery_level")
      .select(F.min("temp") as "min_temp",
        F.max("temp")  as "max_temp" , F.min("c02_level") as "min_CO2" ,
        F.max("c02_level") as "max_CO2",
        F.min("battery_level")  as "min_battery_level" , F.max("battery_level")
        as "max_battery_level").show()
  }
  def Countries_contamination(iotdevices_dataset: Dataset[DeviceIoTData])
            (implicit spark : SparkSession) : DataFrame = {

    import spark.implicits._

    val map = iotdevices_dataset.map(d => (d.c02_level , d.cn)).toDF("co2_level", "country")

      map
        .groupBy("country")
        .agg(max("co2_level")).
        orderBy(desc("co2_level"))
    }

  }

