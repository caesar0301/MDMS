package cn.edu.sjtu.omnilab.kalin.wifi

import cn.edu.sjtu.omnilab.kalin.stlab._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * The modified method to generate heatmap for WifiSyslog
 *
 * Input: hdfs://user/omnilab/warehouse/WifiSyslog/
 */
object GenHeatmap {

  val apToBuild = new APToBuilding()

  def main(args: Array[String]) = {
    
    if (args.length < 2){
      println("Usage: GenHeatmap <WifiSyslog> <out> [interval=3600]")
      sys.exit(-1)
    }
    
    val input = args(0)
    val output = args(1)
    var interval = 3600

    if (args.size >= 3)
      interval = args(2).toInt

    val conf =  new SparkConf()
      .setAppName("Generate heatmap for WifiSyslog")
    val spark = new SparkContext(conf)
    
    val inRDD =
      spark.textFile(input).map(_.split(","))
        .map(parts => {
          val uid = parts(0)
          val time = STUtils.ISOToUnix(parts(1)) / 1000.0
          val messageCode = parts(2).toInt

          if ( List(0,1,2,3).contains(messageCode) ) {
            val buildInfo = apToBuild.parse(parts(3))
            if (buildInfo == null) {
              null
            } else {
              val loc = buildInfo.get(0)
              STPoint(uid, time, loc)
            }
          } else {
            null
          }
        })
        .filter(_ != null)

    // Count statistics for heatmap
    Heatmap.draw(inRDD, interval)

      .sortBy(m => (m.interval, m.loc))

      .map(m => "%d,%s,%d"
        .format(m.interval, m.loc, m.unique))

      .saveAsTextFile(output)
    
    spark.stop()
  }
}