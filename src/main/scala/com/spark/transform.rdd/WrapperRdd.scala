package com.spark.learn

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by GURDIT_SINGH on 03-09-2016.
 */
object WrapperRdd {

  def doubleRdd(sc:SparkContext)={
    val list=List(2.2,2.3,2.4,2.5)
    val textFile=sc.parallelize(list)
    val result=textFile.mean();
    println(result)

  }

  def main(args: Array[String]) {
    val conf=new SparkConf().setMaster("local").setAppName("wrapper classes")
    val sc=new SparkContext(conf)
    doubleRdd(sc)
  }
}
