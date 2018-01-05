package com.vehicle.utils

import java.io.File
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SparkUtils {
  val prop: Properties = new Properties()
  prop.load(SparkUtils.getClass.getClassLoader.getResourceAsStream("spark.properties"))

  def getSparkConf(name: String): SparkConf = {
    val conf = new SparkConf
    import scala.collection.JavaConversions._
    for (k: String <- prop.stringPropertyNames()) {
      conf.set(k, prop.getProperty(k))
    }
    conf.setAppName(name.substring(0, name.length - 1) + "_" + System.currentTimeMillis())
    conf.setJars(getJars)
    conf
  }

  /**
    * 获取jar包
    *
    * @return
    */
  def getJars: Array[String] = {
    val jars = new ArrayBuffer[String]()
    val classesPath = SparkUtils.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    val jarPath = new File(classesPath).getParentFile.listFiles()
    jarPath.foreach(f = file => {
      val path = file.getAbsolutePath
      if (path.endsWith("jar-with-dependencies.jar")) jars.append(path)
    })
    jars.toArray
  }

  def getSparkContext(name: String): SparkContext = new SparkContext(config = getSparkConf(name))

  def getSparkSession(name: String): SparkSession = SparkSession.builder().config(getSparkConf(name)).getOrCreate()
}
