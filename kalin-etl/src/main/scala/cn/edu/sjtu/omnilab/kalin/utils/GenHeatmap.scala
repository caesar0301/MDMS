package cn.edu.sjtu.omnilab.kalin.utils

import cn.edu.sjtu.omnilab.kalin.stlab._
import org.apache.spark.{SparkConf, SparkContext}

object GenHeatmap {

  def main(args: Array[String]) = {
    
    if (args.length < 2){
      println("usage: GenHeatmap <in> <out> [interval=3600]")
      sys.exit(-1)
    }
    
    val input = args(0)
    val output = args(1)
    var interval = 3600

    if (args.size >= 3)
      interval = args(2).toInt
    
    val conf =  new SparkConf()
      .setAppName("Generate heatmap from mobility data")
    val spark = new SparkContext(conf)
    
    val formatedRDD =
      spark.textFile(input).map(_.split(","))
        .map(parts => {
          val uid = parts(0)
          val time = parts(1).toDouble
          val loc = parts(2)
          STPoint(uid, time, loc)
        })
    
    Heatmap.draw(formatedRDD, interval)

      .sortBy(m => (m.interval, m.loc))

      .map(m => "%d,%s,%d"
        .format(m.interval, m.loc, m.unique))

      .saveAsTextFile(output)
    
    spark.stop()
  }
}