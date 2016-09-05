package com.spark.learn

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by GURDIT_SINGH on 25-08-2016.
 */
class RDDOperation {

  def distinct(sc: SparkContext) = {
    val list = List("hello", "world", "hello", "world")
    val textFile = sc.parallelize(list)
    val result = textFile.distinct()
    result.collect().foreach(show)
  }

  def union(sc: SparkContext) = {
    val list1 = List("hello")
    val list2 = List("world")
    val text1 = sc.parallelize(list1)
    val text2 = sc.parallelize(list2)
    val result =text1.map(x=>(x,1)).union(text2.map(x=>(x,2)))
    result.collect().map(x=>show(x._1+"="+x._2))
  }

  def intersection(sc: SparkContext) = {
    val list1 = List("hello")
    val list2 = List("hello","world")
    val text1 = sc.parallelize(list1)
    val text2 = sc.parallelize(list2)
    val result =text1.map(x=>(x,1)).intersection(text2.map(x=>(x,1)))
    result.collect().map(x=>show(x._1+"="+x._2))
  }

  def subtract(sc: SparkContext) = {
    val list1 = List("hello","world")
    val list2 = List("hello")
    val text1 = sc.parallelize(list1)
    val text2 = sc.parallelize(list2)
    val result =text1.map(x=>(x,1)).subtract(text2.map(x=>(x,1)))
    result.collect().map(x=>show(x._1+"="+x._2))
  }

  def cartesian(sc: SparkContext) = {
    val list1 = List("hello","world")
    val list2 = List("hello","world")
    val text1 = sc.parallelize(list1)
    val text2 = sc.parallelize(list2)
    val result =text1.map(x=>(x,1)).cartesian(text2.map(x=>(x,1)))
    result.collect().map(x=>show(x._1+"="+x._2))
  }

  def show(value: String) = {
    println("------" + value)
  }
}

object RDDOperation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("RDDOperation")
    val sc = new SparkContext(conf)
    val obj = new RDDOperation
//    obj.distinct(sc)
    obj.cartesian(sc)
  }
}
