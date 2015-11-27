package cn.edu.sjtu.omnilab.kalin.hz

import org.apache.spark.{SparkContext, SparkConf}

object FilterGeoRangeJob {

  // The whole HZ area
  // val lonRange = (120.0, 120.5)
  // val latRange = (30.0, 30.5)

  // Metro area
  val lonRange = (120.069897, 120.236048)
  val latRange = (30.180852, 30.348045)
  
  def main(args: Array[String]) = {
    
    if (args.length < 2){
      println("usage: FilterDataGeo <in> <out>")
      sys.exit(-1)
    }
    
    val input = args(0)
    val output = args(1)
    
    val conf = new SparkConf()
      .setAppName("LogGeoFilter")
    
    val spark = new SparkContext(conf)
    val inputRDD = spark.textFile(input)

    // filter out data outside given ranges
    val filtered = inputRDD.map(_.split("\t"))
      .filter(line => {
        val lon = line(DataSchema.LON).toDouble
        val lat = line(DataSchema.LAT).toDouble
        var selected = false
        if( lon >= lonRange._1 && lon <= lonRange._2 &&
            lat >= latRange._1 && lat <= latRange._2)
          selected = true
        selected
    })
    
    filtered.map(_.mkString("\t")).saveAsTextFile(output)

    // Generate statistics
    val lonRDD = filtered.map(_(DataSchema.LON).toDouble)
    val latRDD = filtered.map(_(DataSchema.LAT).toDouble)
    val userRDD = filtered.map(_(DataSchema.IMSI)).distinct
    val bsRDD = filtered.map(_(DataSchema.BS)).distinct

    val range = Array(
      ("Records:", filtered.count),
      ("LON:", lonRDD.min, lonRDD.max),
      ("LAT:", latRDD.min, latRDD.max),
      ("TotalUsers:", userRDD.count),
      ("TotalBS:", bsRDD.count))

    spark.parallelize(range).saveAsTextFile(output + ".stat")
    
    spark.stop()
  }

}
