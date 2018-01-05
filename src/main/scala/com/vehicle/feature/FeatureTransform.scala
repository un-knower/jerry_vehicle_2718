package com.vehicle.feature

import java.util
import java.util.Map

import com.vehicle.common.utils.ComparatorUtils

/**
  * 用于第二次处理数据
  * 将类型特征转化为数值特征并记录映射关系
  */
object FeatureTransform {
  /**
    * 将类型特征转化为数值特征
    *
    * @param arr
    * @return
    */
  def transform(arr: Array[String]): util.Map[String, Int] = {
    val map: util.HashMap[String, Integer] = new util.HashMap[String, Integer]()

    arr.foreach(ele => {
      if (map.containsKey(ele)) {
        map.put(ele, map.get(ele) + 1)
      } else {
        map.put(ele, 1)
      }
    })

    val featureNum: Int = map.keySet().size()
    val sortedSet: Array[util.Map.Entry[String, Int]] = getSortedEntry(map)

    val resultMap: util.HashMap[String, Int] = new util.HashMap[String, Int]()
    for (index <- 0 until featureNum) {
      resultMap.put(sortedSet(index).getKey, index)
    }

    resultMap
  }

  /**
    * 根据value值排序key
    *
    * @param map
    * @return
    */
  private def getSortedEntry(map: util.Map[String, Integer]): Array[util.Map.Entry[String, Int]] = {
    val values: util.Set[util.Map.Entry[String, Integer]] = map.entrySet()
    val list: util.List[util.Map.Entry[String, Integer]] = new util.ArrayList[util.Map.Entry[String, Integer]](values.size())
    list.addAll(values)

    val result: util.List[util.Map.Entry[String, Integer]] = ComparatorUtils.sort(list)
    result.toArray(new Array[util.Map.Entry[String, Int]](result.size()))
  }
}
