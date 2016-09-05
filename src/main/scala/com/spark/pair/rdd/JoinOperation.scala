package com.spark.pair.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by GURDIT_SINGH on 05-09-2016.
 */
class JoinOperation(val fields: List[String], val keyField: String, val delimiter: String) extends Serializable {


  def innerJoin(sc: SparkContext, input1: String, input2: String, output: String) = {
    val textFile1 = sc.textFile(input1)
    val textFile2 = sc.textFile(input2)
    val mapTuple1 = textFile1.map(line => (extractKey(line), extractValue(line)))
    val mapTuple2 = textFile2.map(line => (extractKey(line), extractValue(line)))
    val group1 = mapTuple1.groupByKey();
    val group2 = mapTuple2.groupByKey();
    val result = group1.join(group2).flatMapValues(x => x._1 ++ x._2)
    result.saveAsTextFile("innerJoin_output")
  }

  def rightJoin(sc: SparkContext, input1: String, input2: String, output: String) = {
    val textFile1 = sc.textFile(input1)
    val textFile2 = sc.textFile(input2)
    val mapTuple1 = textFile1.map(line => (extractKey(line), extractValue(line)))
    val mapTuple2 = textFile2.map(line => (extractKey(line), extractValue(line)))
    val group1 = mapTuple1.groupByKey();
    val group2 = mapTuple2.groupByKey();
    val result = group1.rightOuterJoin(group2).flatMapValues(x => (x._1 match {
      case Some(x) => x;
      case None => List()
    }) ++ x._2)
    result.saveAsTextFile("rightJoin_output")
  }

  def leftJoin(sc: SparkContext, input1: String, input2: String, output: String) = {
    val textFile1 = sc.textFile(input1)
    val textFile2 = sc.textFile(input2)
    val mapTuple1 = textFile1.map(line => (extractKey(line), extractValue(line)))
    val mapTuple2 = textFile2.map(line => (extractKey(line), extractValue(line)))
    val group1 = mapTuple1.groupByKey();
    val group2 = mapTuple2.groupByKey();
    val result = group1.leftOuterJoin(group2).flatMapValues(x => (x._2 match {
      case Some(x) => x;
      case None => List()
    }) ++ x._1)
    result.saveAsTextFile("leftJoin_output")
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

object JoinOperation {
  def main(args: Array[String]) {
    val jo = new JoinOperation(List("f1", "f2", "f3"),"f1",",")
    val conf = new SparkConf().setMaster("local").setAppName("Join")
    val sc = new SparkContext(conf)
    jo.innerJoin(sc, args(0), args(1), args(2))
    jo.leftJoin(sc, args(0), args(1), args(2))
    jo.rightJoin(sc, args(0), args(1), args(2))
  }
}