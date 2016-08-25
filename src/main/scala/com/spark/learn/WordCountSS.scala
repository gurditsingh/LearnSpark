package com.spark.learn

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by GURDIT_SINGH
 */
object WordCountSS {
  
  def wordCount(sc:SparkContext,input:String,output:String)={
    val textFile=sc.textFile(input)
    val result=textFile.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey((x,y)=>x+y);
   result.saveAsTextFile(output)
  }

  def main(args: Array[String]) {
    val conf=new SparkConf().setMaster("local").setAppName("WordCount")
    val sc=new SparkContext(conf)
    wordCount(sc,args(0),args(1))
  }

}
