package com.spark.custom.partitioner

import org.apache.spark.Partitioner

/**
 * Created by GURDIT_SINGH on 10-09-2016.
 */
class CustomPartitioner extends Partitioner{

  def numPartitions: Int= 3;

  def getPartition(key: Any): Int={
    val keyValue=key.toString.toInt;
    if(keyValue<20)
      0;
    else if(keyValue>20 && keyValue<50)
      1;
    else
      2;
  }

}
