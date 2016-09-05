package com.spark.learn

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by GURDIT_SINGH on 28-08-2016.
 */
object Aggregation {

  def agg(sc:SparkContext)={
    val list=List(2,2,2,2)
    val textFile=sc.parallelize(list)
    val result=textFile.aggregate((0,0))(
      (acc,value)=>(acc._1+value,acc._2+1),
      (acc1,acc2)=>(acc1._1+acc2._1,acc1._2+acc2._2)
    )
    println(result._1/result._2.toDouble)
  }

  def agg2(sc:SparkContext)={
    val list=List(2,2,2,2)
    val textFile=sc.parallelize(list)
    val result=textFile.mapPartitions(itr=> {
      val (acc, rec) = itr.foldLeft((0, 0))((acc, record) => {
        (acc._1 + 1, acc._2 + record)
      })
      List((acc, rec)).iterator
    }).reduce((a,b)=> (0, 0))
    println(result._2/result._1)
  }



  def main(args: Array[String]) {
    val conf=new SparkConf().setMaster("local").setAppName("Aggregation")
    val sc=new SparkContext(conf)
    agg2(sc)
  }

}
