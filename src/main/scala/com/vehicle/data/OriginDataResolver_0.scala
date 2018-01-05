package com.vehicle.data

import java.io.{File, PrintWriter}

import org.apache.commons.lang.NumberUtils

import scala.io.Source

/**
  * 第一版清洗数据:
  *
  * 0:
  * price_level		  空值替换为0
  * price      		  空值替换为对应price_level
  * engine_torque	  空值替换为0
  * rated_passenger	乘客数量取平均值
  * TR				      带分号的取了平均值
  * fuel_type		    空值替换为0
  * power			      带区间的取了平均值
  */
object OriginDataResolver_0 {
  def main(args: Array[String]): Unit = {
    /*数据根目录*/
    val dataRootPath: String = "C:\\Users\\jerry\\IdeaProjects\\vehicle_sales_prediction\\data\\origin"
    /*原始数据文件*/
    val filePath: String = dataRootPath + "\\yancheng_train_20171226.csv"
    /*处理后的文件*/
    val targetPath = dataRootPath + "\\train_0.csv"

    val writer = new PrintWriter(new File(targetPath))
    val origin: Iterator[String] = Source.fromFile(filePath).getLines()
    origin.take(1).foreach(str => writer.write(str + "\n"))
    val data: Array[String] = origin.toArray
    for (index <- 1 until data.length) {
      val str = data(index)
      val arr: Array[String] = str.split(",")
      /*[0:sale_date,1:class_id,2:sale_quantity,3:brand_id,4:compartment,5:type_id
    ,6:level_id,7:department_id,8:TR,9:gearbox_type,10:displacement,11:if_charging
    ,12:price_level,13:price,14:driven_type_id,15:fuel_type_id,16:newenergy_type_id
    ,17:emission_standards_id,18:if_MPV_id,19:if_luxurious_id,20:power,21:cylinder_number
    ,22:engine_torque,23:car_length,24:car_width,25:car_height,26:total_quality
    ,27:equipment_quality,28:rated_passenger,29:wheelbase,30:front_track,31:rear_track]*/
      val level_id = arr(6)
      if (!NumberUtils.isNumber(level_id)) {
        arr(6) = "0"
      }

      val price_level = arr(12)
      if (price_level.endsWith("WL")) {
        val tmp = price_level.split("WL")(0)
        arr(12) = tmp
      } else if (price_level.contains("W")) {
        val b1 = price_level.split("W")(0)
        val b2 = b1.split("-")
        val from = b2(0).toInt
        val to = b2(1).toInt
        val avg = ((from + to) / 2).toString
        arr(12) = avg
      }

      val price = arr(13)
      if (!NumberUtils.isDigits(price)) {
        arr(13) = arr(12)
      }

      val engine_torque = arr(22)
      if (!NumberUtils.isDigits(engine_torque)) {
        arr(22) = "0"
      }

      val rated_passenger = arr(28)
      if (rated_passenger.contains("-")) {
        val a2 = rated_passenger.split("-")
        val from = a2(0).toInt
        val to = a2(1).toInt
        val value = ((from + to) / 2.0).toString
        arr(28) = value
      }

      val tr: String = arr(8)
      if (tr.contains(";")) {
        val a2: Array[String] = tr.split(";", -1)
        val m: Int = a2(0).toInt
        val n: Int = a2(1).toInt
        arr(8) = ((m + n) / 2.0).toString
      }

      val fuelType: String = arr(15)
      if (!NumberUtils.isNumber(fuelType)) {
        arr(15) = "0"
      }

      val power: String = arr(20)
      if (power.contains("/")) {
        val a2: Array[String] = power.split("/", -1)
        val m: Double = a2(0).toDouble
        val n: Double = a2(1).toDouble
        arr(20) = ((m + n) / 2).toString
      }

      val value: String = String.join(",", arr: _*) + '\n'
      writer.write(value)
    }
    writer.close()
  }
}
