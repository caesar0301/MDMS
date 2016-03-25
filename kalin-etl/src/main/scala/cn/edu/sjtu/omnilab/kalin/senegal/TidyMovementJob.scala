package cn.edu.sjtu.omnilab.kalin.senegal

import cn.edu.sjtu.omnilab.kalin.stlab.{STUtils, MPoint, CleanseMob}
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Wangle Senegal mobility data with the same strategy employed
  * in Hangzhou data. Work fot both data set2 and set3.
  */
object TidyMovementJob {

  def main(args: Array[String]) = {
    
    if (args.length < 2){
      println("usage: TidyMovementJob <in> <out>")
      sys.exit(-1)
    }
    
    val input = args(0)
    val output = args(1)
    
    val conf =  new SparkConf().setAppName("Tidy Senegal mobility data")
    val spark = new SparkContext(conf)
    
    val inputRDD = spark.textFile(input).map(_.split(",")).cache
    
    val formatedRDD = inputRDD.map(parts => {
      val uid = parts(0)
      val time = STUtils.ISOToUnix(parts(1)) / 1000.0 + 3600 * 8 // convert to GMT time
      val loc = parts(2)
      MPoint(uid, time, loc)
    })
    
    val tidyMove = CleanseMob.cleanse(formatedRDD, minDays=14*0.75, tzOffset=0, addNight=true)

    tidyMove
      .sortBy(tuple => (tuple.uid, tuple.time))
      .map(tuple => "%s,%.3f,%s".format(tuple.uid, tuple.time, tuple.location))
      .saveAsTextFile(output)
    
    spark.stop()
  }
}
