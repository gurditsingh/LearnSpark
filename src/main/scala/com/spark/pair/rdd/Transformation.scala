package com.spark.pair.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by GURDIT_SINGH on 03-09-2016.
 */
object Transformation {

  def reduceKey(sc:SparkContext)={
    val list=List((1,2),(3,4),(3,6))
    val rddList=sc.parallelize(list)
    val result=rddList.reduceByKey((x,y)=>x+y)
    result.collect().foreach(println)
  }

  def groupKey(sc:SparkContext)={
    val list=List((1,2),(3,4),(3,6))
    val rddList=sc.parallelize(list)
    val result=rddList.groupByKey()
    result.collect().foreach(println)
  }

  def innerJoin(sc:SparkContext)={
    val list=List((1,2),(3,4),(3,6))
    val other=List((3,9))
    val rddList=sc.parallelize(list)
    val rddOther=sc.parallelize(other)
    val result=rddList.join(rddOther)
    result.collect().foreach(println)
  }

  def main(args: Array[String]) {
    val conf=new SparkConf().setMaster("local").setAppName("wrapper classes")
    val sc=new SparkContext(conf)
    innerJoin(sc)
  }
}
