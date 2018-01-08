package com.vehicle.data

import java.io.{File, PrintWriter}
import java.util

import com.alibaba.fastjson.JSON
import com.vehicle.feature.FeatureTransform

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * 第二版数据处理
  * 处理类型特征，并储存映射关系
  */
object OriginDataResolver_1 {
  def main(args: Array[String]): Unit = {
    /*数据根目录*/
    val dataRootPath: String = "C:\\Users\\jerry\\IdeaProjects\\vehicle_sales_prediction\\data\\origin"
    /*原始数据文件*/
    val filePath: String = dataRootPath + "\\train_0.csv"
    /*处理后的文件*/
    val targetPath = dataRootPath + "\\train_1.csv"
    /*映射关系文件*/
    val mapPath = dataRootPath + "\\map.json"

    /*[0:sale_date,1:class_id,2:sale_quantity,3:brand_id,4:compartment,5:type_id
    ,6:level_id,7:department_id,8:TR,9:gearbox_type,10:displacement,11:if_charging
    ,12:price_level,13:price,14:driven_type_id,15:fuel_type_id,16:newenergy_type_id
    ,17:emission_standards_id,18:if_MPV_id,19:if_luxurious_id,20:power,21:cylinder_number
    ,22:engine_torque,23:car_length,24:car_width,25:car_height,26:total_quality
    ,27:equipment_quality,28:rated_passenger,29:wheelbase,30:front_track,31:rear_track]*/

    /*类型特征的序号*/
    val typeIndex: Array[Int] = Array(1, 3, 9, 11)
    val mapList: util.Map[Int, util.Map[String, Int]] = new util.HashMap[Int, util.Map[String, Int]]()
    val listList: util.Map[Int, util.List[String]] = new util.HashMap[Int, util.List[String]]()
    typeIndex.foreach(index => {
      listList.put(index, new util.LinkedList[String]())
    })

    val writer: PrintWriter = new PrintWriter(new File(targetPath))
    val jsonWriter: PrintWriter = new PrintWriter(new File(mapPath))
    val origin: Iterator[String] = Source.fromFile(filePath).getLines()
    origin.take(1).foreach(str => writer.write(str + "\n"))

    val data: Array[String] = origin.toArray.drop(1)

    /*将每一种类型特征可能的取值放进list中*/
    data.foreach(str => {
      val arr: Array[String] = str.split(",", -1)
      typeIndex.foreach(index => {
        val ele: String = arr(index)
        listList.get(index).add(ele)
      })
    })

    for ((index: Int, list: util.List[String]) <- listList) {
      val ele: util.Map[String, Int] = FeatureTransform.transform(list.toArray(new Array[String](list.size())))
      mapList.put(index, ele)
    }

    val json: String = JSON.toJSON(mapList).toString
    jsonWriter.write(json)
    jsonWriter.close()

    /*已获得每一个类型特征的数值映射*/
    data.foreach(str => {
      val arr: Array[String] = str.split(",", -1)

      for (index <- typeIndex) {
        val typeKey = arr(index)
        val typeValue: Int = mapList.get(index).get(typeKey)
        arr(index) = typeValue.toString
      }

      val value = String.join(",", arr: _*) + '\n'
      writer.write(value)
    })

    writer.close()
  }
}
