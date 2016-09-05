package com.spark.learn

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by GURDIT_SINGH on 25-08-2016.
 */
object Action {
  def transform(sc:SparkContext,output:String)={
    val list=List("hello","world","hello")
    val textFile=sc.parallelize(list)
    val result=textFile.filter(word=>word.contains("hello"))
    result.saveAsTextFile(output)
  }

  def main(args: Array[String]) {
    val conf=new SparkConf().setMaster("local").setAppName("Action")
    val sc=new SparkContext(conf)
    transform(sc,args(0))
  }
}
