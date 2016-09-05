package com.spark.pair.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by GURDIT_SINGH on 03-09-2016.
 */
object PerKeyAvg {

  def doubleRdd(sc:SparkContext)={
    val list=List(("x",2),("y",3),("x",4),("z",2),("y",4))
    val rddList=sc.parallelize(list)
    val rddAvg=rddList.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues(a=>a._1/a._2.toFloat)
    rddAvg.collect().foreach(println)
  }

  def main(args: Array[String]) {
    val conf=new SparkConf().setMaster("local").setAppName("wrapper classes")
    val sc=new SparkContext(conf)
    doubleRdd(sc)
  }
}
