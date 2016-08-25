package com.spark.learn

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by GURDIT_SINGH on 25-08-2016.
 */
object Transformation {

  def transform(sc:SparkContext)={
    val list=List("hello","world","hello")
    val textFile=sc.parallelize(list)
    val result=textFile.filter(word=>word.contains("hello"))
    result.collect().foreach(println)
  }

  def main(args: Array[String]) {
    val conf=new SparkConf().setMaster("local").setAppName("Transformation")
    val sc=new SparkContext(conf)
    transform(sc)
  }
}
