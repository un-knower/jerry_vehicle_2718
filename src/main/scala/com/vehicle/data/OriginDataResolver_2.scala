package com.vehicle.data

import com.vehicle.utils.SparkUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * 第三版数据处理
  * 类型转化
  */
object OriginDataResolver_2 {
  def main(args: Array[String]): Unit = {
    val filePath: String = "hdfs://jerry:9000/vehicle/data/yancheng_train_20171226_0.csv"
    val targetPath: String = "hdfs://jerry:9000/vehicle/data/yancheng_train_20171226_1.csv"
    val csvFormat: String = "com.databricks.spark.csv"

    val sc: SparkSession = SparkUtils.getSparkSession(this.getClass.getSimpleName)
    val originData: DataFrame = sc.read
      .format(csvFormat)
      .option("header", value = true)
      .option("inferSchema", value = true)
      .load(filePath)

    /*[sale_date,class_id,sale_quantity,brand_id,compartment,type_id
    ,level_id,department_id,TR,gearbox_type,displacement,if_charging
    ,price_level,price,driven_type_id,fuel_type_id,newenergy_type_id
    ,emission_standards_id,if_MPV_id,if_luxurious_id,power,cylinder_number
    ,engine_torque,car_length,car_width,car_height,total_quality
    ,equipment_quality,rated_passenger,wheelbase,front_track,rear_track]*/

    /*1.特征名称处理 原始名称 索引后名称 并集名称*/
    /*原始名称*/
    val originStr: String = "sale_date,class_id,sale_quantity,brand_id,compartment,type_id,level_id,department_id,TR,gearbox_type,displacement,if_charging,price_level,price,driven_type_id,fuel_type_id,newenergy_type_id,emission_standards_id,if_MPV_id,if_luxurious_id,power,cylinder_number,engine_torque,car_length,car_width,car_height,total_quality,equipment_quality,rated_passenger,wheelbase,front_track,rear_track"
    /*索引后名称*/
    val indexed: String = "sale_date,class_id,sale_quantity,brand_id_indexed,compartment,type_id,level_id,department_id,TR,gearbox_type_indexed,displacement,if_charging_indexed,price_level,price,driven_type_id,fuel_type_id,newenergy_type_id,emission_standards_id,if_MPV_id,if_luxurious_id,power,cylinder_number,engine_torque,car_length,car_width,car_height,total_quality,equipment_quality,rated_passenger,wheelbase,front_track,rear_track"

    val indexedNames: Array[String] = indexed.split(",", -1)
    val originNames: Array[String] = originStr.split(",", -1)
    val dropNamesSet = new mutable.HashSet[String]()

    /*并集名称-待删除列名*/
    val dropNames: Array[String] = dropNamesSet.toArray
    originNames.foreach(dropNamesSet.add)
    indexedNames.foreach(dropNamesSet.add)

    /*2.将所有值转化为double类型(除类型特征)*/
    originNames.foreach(col => {
      if (indexed.contains(col)) {
        originData(col).cast("double")
      }
    })

    /*3.将类型特征转化为数值类型*/
    val f1: StringIndexer = new StringIndexer().setInputCol("gearbox_type").setOutputCol("gearbox_type_indexed")
    val f2: StringIndexer = new StringIndexer().setInputCol("if_charging").setOutputCol("if_charging_indexed")
    val f3: StringIndexer = new StringIndexer().setInputCol("brand_id").setOutputCol("brand_id_indexed")

    /*4.合并特征*/
    val assembler: VectorAssembler = new VectorAssembler().setInputCols(indexedNames).setOutputCol("features")

    /*5.工作流水线*/
    val pipeline: Pipeline = new Pipeline().setStages(Array(f1, f2, f3))

    var df = pipeline.fit(originData).transform(originData)
    df = df.withColumn("quantity", df("sale_quantity"))
    df.show(3)

    df.printSchema()
  }
}
