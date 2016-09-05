package com.spark.learn

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by GURDIT_SINGH on 25-08-2016.
 */
object WordCountPP {
  def wordCount(sc:SparkContext,output:String)={
    val list=List("hello","world")
    val textFile=sc.parallelize(list)
    val result=textFile.map(word=>(word,1)).reduceByKey((x,y)=>x+y);
    result.saveAsTextFile(output)
  }

  def main(args: Array[String]) {
    val conf=new SparkConf().setMaster("local").setAppName("WordCount")
    val sc=new SparkContext(conf)
    wordCount(sc,args(0))
  }


}
