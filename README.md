LearnSpark
------------ck3-1
    val aggdf=df.groupBy("state").agg(functions.sum("a_g").alias("count"))


    val w=expressions.Window.orderBy(functions.desc("count"))
////
    aggdf.withColumn("rank",functions.dense_rank().over(w)).where(functions.col("rank") <= 3).show

------------ck5-1

  val groupdf=df.groupByKey(r=>Row.fromSeq(Array(r.get(3))))(RowEncoder(key))


    groupdf.mapGroups((r,itr)=>{
      var male:Int=0
      var total:Int=0
      itr.foreach(row => {
        if(row.getString(7).equals("M"))
          male+=row.getInt(9)
        total += row.getInt(9)
      })
      Row.fromSeq(Array(r.get(0),(male*100)/total))
    })(RowEncoder(key2)).show()

    val w=expressions.Window.orderBy(functions.desc("aadhar_percent"))
////
    aggdf.withColumn("rank",functions.dense_rank().over(w)).where(functions.col("rank") <= 3).show
