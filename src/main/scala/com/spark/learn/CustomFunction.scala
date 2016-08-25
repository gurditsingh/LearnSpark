package com.spark.learn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Created by GURDIT_SINGH on 25-08-2016.
 */
class CustomFunction(val query:String) extends Serializable  {

  def getMatch(rdd:RDD[String]):RDD[String]={
    rdd.flatMap(x=>x.split(query))
  }
}
object CustomFunction{

  def main(args: Array[String]) {
    val conf=new SparkConf().setMaster("local").setAppName("CustomFunction")
    val sc=new SparkContext(conf)
    val list=List("hello","world","hello")
    val textFile=sc.parallelize(list)
    val result=new CustomFunction(" ").getMatch(textFile)
    println(result.take(5).foreach(println))
  }
}