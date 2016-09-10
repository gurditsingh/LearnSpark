package com.spark.custom.partitioner

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by GURDIT_SINGH on 10-09-2016.
 */
class FindMaxScore(val fields: List[String], val keyField: String, val delimiter: String) extends Serializable {

  def maxValue(sc: SparkContext, input: String, output: String) = {
    val textFile = sc.textFile(input)
    val mappedRDD = textFile.map(line => (extractKey(line), extractValue(line)))
    val groupedRDD=mappedRDD.groupByKey(new CustomPartitioner)
    val max = groupedRDD.mapPartitions(itr => {
      val (maxFemale, maxMale) = itr.foldLeft(Double.MinValue, Double.MinValue)((acc, record) => {
        val maxFemale = record._2.filter(value => value.split(",")(1).equals("female")).foldLeft(Double.MinValue)((accf, rec) => {
          val score = rec.split(",")(2).toDouble;
          (accf max score)
        })
        val maxMale = record._2.filter(value => value.split(",")(1).equals("male")).foldLeft(Double.MinValue)((accm, rec) => {
          val score = rec.split(",")(2).toDouble;
          (accm max score)
        })
        (acc._1 max maxFemale, acc._2 max maxMale)
      })
      List((maxFemale, maxMale)).iterator
    })
    max.saveAsTextFile(output)
  }

  def extractKey(value: String) = {
    val data = value.split(delimiter)
    if (data.length != fields.length)
      throw new RuntimeException("fields and data are not same")
    val keyValue = fields.zip(data.toList).filter(x => x._1.equals(keyField)).map(x => x._2)
    keyValue.head
  }

  def extractValue(value: String) = {
    val data = value.split(delimiter)
    if (data.length != fields.length)
      throw new RuntimeException("fields and data are not same")
    val keyValue = fields.zip(data.toList).filter(x => !x._1.equals(keyField)).map(x => x._2)
    keyValue.mkString(delimiter)
  }
}

object FindMaxScore {
  def main(args: Array[String]) {
    val customPartition = new FindMaxScore(List("f1", "f2", "f3", "f4"), "f2", ",")
    val conf = new SparkConf().setMaster("local").setAppName("Partitioner")
    val sc = new SparkContext(conf)
    customPartition.maxValue(sc, args(0), args(1))
  }
}
