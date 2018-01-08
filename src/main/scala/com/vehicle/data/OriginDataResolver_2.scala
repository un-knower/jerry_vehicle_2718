package com.vehicle.data

import java.lang.reflect.Type
import java.util

import com.alibaba.fastjson.{JSON, TypeReference}
import com.vehicle.utils.SparkUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.io.{BufferedSource, Source}

/**
  * 第3版数据处理
  * 测试集数据处理，使用DataFrame
  */
object OriginDataResolver_2 {
  def main(args: Array[String]): Unit = {
    val originFilePath: String = "hdfs://jerry:9000/vehicle/data/train_1.csv"
    val testFilePath: String = "hdfs://jerry:9000/vehicle/data/test.csv"
    val testTargetFilePath: String = "hdfs://jerry:9000/vehicle/data/test"
    val csvFormat: String = "com.databricks.spark.csv"

    val sc: SparkSession = SparkUtils.getSparkSession(this.getClass.getSimpleName)
    val originData: DataFrame = sc.read
      .format(csvFormat)
      .option("header", value = true)
      .option("inferSchema", value = true)
      .load(originFilePath).cache()

    val originTestData: DataFrame = sc.read
      .format(csvFormat)
      .option("header", value = true)
      .option("inferSchema", value = true)
      .load(testFilePath)
      .withColumnRenamed("predict_date", "sale_date")

    val indexedTestData: DataFrame = transformClassId(sc, originTestData).cache()
    /*[sale_date,class_id,sale_quantity,brand_id,compartment,type_id
    ,level_id,department_id,TR,gearbox_type,displacement,if_charging
    ,price_level,price,driven_type_id,fuel_type_id,newenergy_type_id
    ,emission_standards_id,if_MPV_id,if_luxurious_id,power,cylinder_number
    ,engine_torque,car_length,car_width,car_height,total_quality
    ,equipment_quality,rated_passenger,wheelbase,front_track,rear_track]*/

    val joinOriginData = originData.drop("sale_date", "sale_quantity")
    val testData: DataFrame = indexedTestData.join(joinOriginData, Seq("class_id"))

    val targetTestData: DataFrame = testData.coalesce(1).toDF
    targetTestData.write
      .mode("overwrite")
      .option("header", value = true)
      .csv(testTargetFilePath)

    sc.close()
  }

  /**
    * 转化class_id的映射
    *
    * @param df
    * @return
    */
  def transformClassId(sc: SparkSession, df: DataFrame): DataFrame = {
    val source: BufferedSource = Source.fromFile("C:\\Users\\jerry\\IdeaProjects\\vehicle_sales_prediction\\data\\origin\\map.json")
    val json: String = source.bufferedReader().readLine()
    val typeReference: Type = new TypeReference[util.Map[String, util.Map[String, Int]]] {}.getType
    val map: util.Map[String, util.Map[String, Int]] = JSON.parseObject(json, typeReference)
    val mapping: util.Map[String, Int] = map.get("1")

    val list = for ((classId, index) <- mapping) yield Row(classId, index)
    val list1 = new util.ArrayList[Row]()
    list.foreach(row => list1.add(row))

    val struct = StructType(
      List(
        StructField("class_id", StringType),
        StructField("class_id_index", IntegerType)
      )
    )
    val df2: DataFrame = sc.createDataFrame(list1, struct)
    val result: DataFrame = df.join(df2, Seq("class_id"))
    result.withColumn("class_id", result("class_id_index")).drop("class_id_index")
  }
}
