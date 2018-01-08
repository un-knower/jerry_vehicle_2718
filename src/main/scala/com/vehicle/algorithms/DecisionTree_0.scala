package com.vehicle.algorithms

import com.vehicle.utils.SparkUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object DecisionTree_0 {
  def main(args: Array[String]): Unit = {
    val trainFilePath: String = "hdfs://jerry:9000/vehicle/train/train_1.csv"
    val testFilePath: String = "hdfs://jerry:9000/vehicle/train/test_1.csv"
    val modelPath: String = "hdfs://jerry:9000/vehicle/model/"
    val csvFormat: String = "com.databricks.spark.csv"

    val originFeatureNames: String = "sale_date,class_id,brand_id,compartment,type_id,level_id,department_id,TR,gearbox_type,displacement,if_charging,price_level,price,driven_type_id,fuel_type_id,newenergy_type_id,emission_standards_id,if_MPV_id,if_luxurious_id,power,cylinder_number,engine_torque,car_length,car_width,car_height,total_quality,equipment_quality,rated_passenger,wheelbase,front_track,rear_track"
    val featureNames: Array[String] = originFeatureNames.split(",", -1)
    val labelName: String = "sale_quantity"

    val sc: SparkSession = SparkUtils.getSparkSession(this.getClass.getSimpleName)
    val trainData: DataFrame = sc.read
      .format(csvFormat)
      .option("header", value = true)
      .option("inferSchema", value = true)
      .load(trainFilePath).cache()

    val testData: DataFrame = sc.read
      .format(csvFormat)
      .option("header", value = true)
      .option("inferSchema", value = true)
      .load(testFilePath).cache()

    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(featureNames)
      .setOutputCol("feature_0")

    val standard: StandardScaler = new StandardScaler()
      .setInputCol("feature_0")
      .setOutputCol("feature_1")
      .setWithStd(true)
      .setWithMean(true)

    val scaler: MinMaxScaler = new MinMaxScaler()
      .setInputCol("feature_1")
      .setOutputCol("feature_2")
      .setMax(10)
      .setMin(0)

    val normalizer: Normalizer = new Normalizer()
      .setInputCol("feature_2")
      .setOutputCol("feature_3")
      .setP(2)

    val pca: PCA = new PCA()
      .setInputCol("feature_3")
      .setOutputCol("feature")
      .setK((featureNames.length / 1.3).toInt)

    val algorithms: RandomForestRegressor = new RandomForestRegressor()
      .setFeaturesCol("feature")
      .setLabelCol(labelName)
      .setPredictionCol("predict_sale_quantity")
      .setMaxBins(featureNames.length * 2)
      .setMaxDepth(featureNames.length / 2)
      .setNumTrees((140 / 0.618).toInt)
      .setFeatureSubsetStrategy("sqrt")
      .setImpurity("variance")
      .setMinInstancesPerNode(1)

    val evaluator: RegressionEvaluator = new RegressionEvaluator()
      .setLabelCol("sale_quantity")
      .setPredictionCol("predict_sale_quantity")
      .setMetricName("rmse")

    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(assembler, standard, scaler, normalizer, pca, algorithms))

    val paramGrid: Array[ParamMap] = new ParamGridBuilder().build()

    val crossingValidator: CrossValidator = new CrossValidator()
      .setNumFolds(5)
      .setEvaluator(evaluator)
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)

    val model: CrossValidatorModel = crossingValidator.fit(trainData)
    val resultDF: DataFrame = model.transform(trainData)
    val result: Double = 1.0
    val evaluatorValue: Double = evaluator.evaluate(resultDF)
    val modelName: String = getModelName(result, evaluatorValue)

    println(model.params)
    model.save(modelPath + modelName)
  }

  /**
    * 评估结果
    *
    * @param dataFrame
    * @return
    */
  def evaluateResultDF(dataFrame: DataFrame): Double = {
    val d1: RDD[(Int, Int)] = dataFrame.rdd.map(row => {
      val classId: Int = row.getAs[Int]("class_id")
      val saleQuantity: Int = row.getAs[Int]("sale_quantity")
      val saleQuantityPredicted: Int = row.getAs[Int]("predict_sale_quantity")

      val value = Math.abs(saleQuantity - saleQuantityPredicted)
      (classId, value * value)
    }).reduceByKey((v1: Int, v2: Int) => {
      v1 + v2
    })

    val sum: Double = d1.mapPartitions(it => {
      it.map(_._2)
    }).sum()

    val result: Double = Math.sqrt(sum) / d1.count()
    result
  }

  /**
    * 获得模型名称
    *
    * @param v1
    * @param v2
    * @return
    */
  def getModelName(v1: Double, v2: Double): String = {
    val sb = new StringBuilder
    sb.append(DecisionTree_0.getClass.getSimpleName.dropRight(1)).append("_")
      .append(System.currentTimeMillis()).append("_")
      .append(v1).append("_")
      .append(v2).append("_")

    sb.toString()
  }
}
