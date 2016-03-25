package cn.edu.sjtu.omnilab.kalin.utils

import cn.edu.sjtu.omnilab.kalin.stlab._
import org.apache.spark.{SparkConf, SparkContext}

object GenFlowmap {

  def main(args: Array[String]) = {
    
    if (args.length < 2){
      println("usage: GenFlowmap <in> <out> [interval=86400] [minnum=1]")
      sys.exit(-1)
    }
    
    val input = args(0)
    val output = args(1)
    var minnum = 1
    var interval = 86400

    if (args.size >= 3)
      interval = args(2).toInt

    if (args.size >= 4)
      minnum = args(3).toInt
    
    val conf =  new SparkConf().setAppName("Generate flowmap")
    val spark = new SparkContext(conf)
    
    val formatedRDD =
      spark.textFile(input).map(_.split(","))
        .map(parts => {
          val uid = parts(0)
          val time = parts(1).toDouble
          val loc = parts(2)
          STPoint(uid, time, loc)
        })
    
    Flowmap.draw(formatedRDD, interval)

      .filter(_.NumUnique >= minnum)

      .sortBy(m => (m.interval, m.FROM, m.TO))

      .map(m => "%d,%s,%s,%d,%d"
        .format(m.interval, m.FROM, m.TO, m.NumTotal, m.NumUnique))

      .saveAsTextFile(output)
    
    spark.stop()
  }
}